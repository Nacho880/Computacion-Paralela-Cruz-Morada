#include "api.h"
#include "csv.h"
#include "logger.h"
#include "metrics.h"
#include "sftp.h"

#include <curl/curl.h>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <omp.h>
#include <thread>
#include <vector>

using namespace std;

namespace {

int configure_openmp_threads() {
    const unsigned hw = thread::hardware_concurrency();
    const int threads = hw == 0 ? 32 : static_cast<int>(hw);
    omp_set_num_threads(threads);
    return threads;
}

} 

int main() {
    const double start_time = omp_get_wtime();
    configure_openmp_threads();

    setvbuf(stdout, nullptr, _IONBF, 0);
    setvbuf(stderr, nullptr, _IONBF, 0);

    curl_global_init(CURL_GLOBAL_DEFAULT);

    const string api_base = "https://api.sebastian.cl/cpyd";

    string api_email;
    string api_rut;
    unique_ptr<ApiClient> api;

    while (true) {
        api_email.clear();
        api_rut.clear();

        cout << "Ingrese correo institucional (nombre@utem.cl): ";
        getline(cin, api_email);

        cout << "Ingrese RUT (12.345.678-9): ";
        getline(cin, api_rut);

        if (api_email.empty() || api_rut.empty()) {
            cerr << "Correo o RUT incorrectos. Intente nuevamente.\n";
            continue;
        }

        api = make_unique<ApiClient>(api_base, api_email, api_rut);

        if (api->authenticate()) {
            cout << "Autenticación correcta.\n";
            break;
        }

        api.reset();
        Logger::instance().flush_api_summary();
        cerr << "Correo o RUT incorrectos. Intente nuevamente.\n";
    }

    const SftpConfig sftp_config {
        "137.184.45.251",
        22,
        "utem",
        "CPyD.2026",
        "reporte_*.csv",
        "downloads"
    };

    const vector<string> csv_files = sftp_download_reports(sftp_config);
    if (csv_files.empty()) {
        cerr << "No se descargaron archivos CSV.\n";
        curl_global_cleanup();
        return 1;
    }

    printf("Archivos descargados: %zu\n", csv_files.size());
    fflush(stdout);

    api->load_disk_cache();

    printf("Cache UUID en disco: %lld entradas\n",
           static_cast<long long>(api->cache_size()));
    fflush(stdout);

    fputs("=== ETAPA 2: Recoleccion de UUID unicos ===\n", stdout);
    fflush(stdout);

    api->resolve_uuids_pipeline(csv_files);

    const MetricsResult metrics = compute_metrics_streaming(csv_files, *api);

    if (metrics.transactions_processed == 0) {
        cerr << "No se encontraron transacciones validas.\n";
        Logger::instance().flush_csv_summaries();
        Logger::instance().flush_api_summary();
        api->flush_disk_cache();
        curl_global_cleanup();
        return 1;
    }

    const double elapsed = omp_get_wtime() - start_time;

    const string output = format_results(metrics, elapsed);

    fputs(output.c_str(), stdout);
    fputc('\n', stdout);
    fflush(stdout);

    save_results(output);

    Logger::instance().flush_csv_summaries();
    Logger::instance().flush_api_summary();

    api->flush_disk_cache();

    curl_global_cleanup();
    return 0;
}
