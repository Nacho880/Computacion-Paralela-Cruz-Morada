#include "sftp.h"

#include "logger.h"

#include <libssh2.h>
#include <libssh2_sftp.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <omp.h>
#include <sys/stat.h>
#include <thread>

using namespace std;

namespace {

bool matches_reporte_pattern(const string& filename) {
    return filename.size() >= 12 &&
           filename.compare(0, 8, "reporte_") == 0 &&
           filename.size() >= 4 &&
           filename.compare(filename.size() - 4, 4, ".csv") == 0;
}

bool ensure_directory(const string& path) {
    struct stat st {};
    if (stat(path.c_str(), &st) == 0) {
        return S_ISDIR(st.st_mode);
    }
    return mkdir(path.c_str(), 0755) == 0;
}

int connect_socket(const string& host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        return -1;
    }

    sockaddr_in sin {};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(static_cast<uint16_t>(port));

    if (inet_pton(AF_INET, host.c_str(), &sin.sin_addr) <= 0) {
        close(sock);
        return -1;
    }

    if (connect(sock, reinterpret_cast<sockaddr*>(&sin), sizeof(sin)) != 0) {
        close(sock);
        return -1;
    }

    return sock;
}

bool download_file_with_sftp(LIBSSH2_SFTP* sftp,
                             const SftpConfig& config,
                             const string& remote_name) {
    const string remote_path = "/" + remote_name;
    LIBSSH2_SFTP_HANDLE* handle =
        libssh2_sftp_open(sftp, remote_path.c_str(), LIBSSH2_FXF_READ, 0);
    if (!handle) {
        Logger::instance().log_sftp_error("No se pudo abrir remoto: " + remote_path);
        return false;
    }

    const string local_path = config.local_dir + "/" + remote_name;
    ofstream output(local_path, ios::binary);
    if (!output.is_open()) {
        Logger::instance().log_sftp_error("No se pudo crear archivo local: " + local_path);
        libssh2_sftp_close(handle);
        return false;
    }

    char buffer[65536];
    ssize_t nbytes;
    while ((nbytes = libssh2_sftp_read(handle, buffer, sizeof(buffer))) > 0) {
        output.write(buffer, nbytes);
    }

    if (nbytes < 0) {
        Logger::instance().log_sftp_error("Error al descargar " + remote_name + ": lectura remota fallida");
        libssh2_sftp_close(handle);
        return false;
    }

    libssh2_sftp_close(handle);
    return true;
}

vector<string> list_remote_files(const SftpConfig& config) {
    vector<string> files;

    int sock = connect_socket(config.host, config.port);
    if (sock < 0) {
        Logger::instance().log_sftp_error(
            "No se pudo conectar al servidor " + config.host + ": " + strerror(errno));
        return files;
    }

    LIBSSH2_SESSION* session = libssh2_session_init();
    if (!session) {
        close(sock);
        Logger::instance().log_sftp_error("No se pudo inicializar sesion libssh2");
        return files;
    }

    if (libssh2_session_handshake(session, sock) != 0) {
        Logger::instance().log_sftp_error("Handshake SSH fallido");
        libssh2_session_free(session);
        close(sock);
        return files;
    }

    if (libssh2_userauth_password(session, config.user.c_str(), config.password.c_str()) != 0) {
        Logger::instance().log_sftp_error("Autenticacion SFTP fallida");
        libssh2_session_disconnect(session, "auth failed");
        libssh2_session_free(session);
        close(sock);
        return files;
    }

    LIBSSH2_SFTP* sftp = libssh2_sftp_init(session);
    if (!sftp) {
        Logger::instance().log_sftp_error("No se pudo iniciar SFTP");
        libssh2_session_disconnect(session, "sftp init failed");
        libssh2_session_free(session);
        close(sock);
        return files;
    }

    LIBSSH2_SFTP_HANDLE* dir = libssh2_sftp_opendir(sftp, "/");
    if (!dir) {
        Logger::instance().log_sftp_error("No se pudo abrir directorio raiz SFTP");
        libssh2_sftp_shutdown(sftp);
        libssh2_session_disconnect(session, "opendir failed");
        libssh2_session_free(session);
        close(sock);
        return files;
    }

    char mem[512];
    char longentry[512];
    LIBSSH2_SFTP_ATTRIBUTES attrs {};

    while (true) {
        ssize_t rc = libssh2_sftp_readdir_ex(dir, mem, sizeof(mem), longentry, sizeof(longentry), &attrs);
        if (rc <= 0) {
            break;
        }

        string name(mem, static_cast<size_t>(rc));
        if (name == "." || name == "..") {
            continue;
        }

        if (matches_reporte_pattern(name)) {
            files.push_back(name);
        }
    }

    libssh2_sftp_closedir(dir);
    libssh2_sftp_shutdown(sftp);
    libssh2_session_disconnect(session, "done");
    libssh2_session_free(session);
    close(sock);

    sort(files.begin(), files.end());
    return files;
}

}  // namespace

vector<string> sftp_download_reports(const SftpConfig& config) {
    vector<string> downloaded;

    if (!ensure_directory(config.local_dir)) {
        Logger::instance().log_sftp_error("No se pudo crear directorio local: " + config.local_dir);
        return downloaded;
    }

    static bool libssh2_initialized = false;
#pragma omp critical(libssh2_init_once)
    {
        if (!libssh2_initialized) {
            libssh2_init(0);
            libssh2_initialized = true;
        }
    }

    const vector<string> remote_files = list_remote_files(config);
    if (remote_files.empty()) {
        Logger::instance().log_sftp_error("No se encontraron archivos reporte_*.csv en el servidor");
        return downloaded;
    }

    fputs("=== ETAPA 1: DESCARGA SFTP ===\n", stdout);
    fputs("Iniciando descarga SFTP...\n\n", stdout);
    fflush(stdout);

    vector<int> success_flags(remote_files.size(), 0);
    const size_t total_files = remote_files.size();
    atomic<size_t> downloaded_count{0};
    atomic<size_t> next_file_index{0};

    int thread_count = omp_get_max_threads();
    if (thread_count < 8) {
        thread_count = 8;
    } else if (thread_count > 12) {
        thread_count = 12;
    }
    if (static_cast<size_t>(thread_count) > total_files) {
        thread_count = static_cast<int>(total_files);
    }

    vector<thread> workers;
    workers.reserve(thread_count);
    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        workers.emplace_back([&, thread_id]() {
            int sock = connect_socket(config.host, config.port);
            if (sock < 0) {
                return;
            }

            LIBSSH2_SESSION* session = libssh2_session_init();
            if (!session) {
                close(sock);
                return;
            }

            if (libssh2_session_handshake(session, sock) != 0) {
                libssh2_session_free(session);
                close(sock);
                return;
            }

            if (libssh2_userauth_password(session, config.user.c_str(), config.password.c_str()) != 0) {
                libssh2_session_disconnect(session, "auth failed");
                libssh2_session_free(session);
                close(sock);
                return;
            }

            LIBSSH2_SFTP* sftp = libssh2_sftp_init(session);
            if (!sftp) {
                libssh2_session_disconnect(session, "sftp init failed");
                libssh2_session_free(session);
                close(sock);
                return;
            }

            while (true) {
                const size_t index = next_file_index.fetch_add(1, memory_order_relaxed);
                if (index >= total_files) {
                    break;
                }
                if (download_file_with_sftp(sftp, config, remote_files[index])) {
                    success_flags[index] = 1;
                    const size_t current = downloaded_count.fetch_add(1) + 1;
                    if (current % 50 == 0 || current == total_files) {
#pragma omp critical(sftp_progress)
                        {
                            printf("Archivos descargados: %zu/%zu\n", current, total_files);
                            fflush(stdout);
                        }
                    }
                }
            }

            libssh2_sftp_shutdown(sftp);
            libssh2_session_disconnect(session, "done");
            libssh2_session_free(session);
            close(sock);
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }

    for (size_t i = 0; i < remote_files.size(); ++i) {
        if (success_flags[i]) {
            downloaded.push_back(config.local_dir + "/" + remote_files[i]);
        }
    }

    return downloaded;
}
