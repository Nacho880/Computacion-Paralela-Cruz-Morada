#include "logger.h"

#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <sstream>

using namespace std;

const string Logger::LOG_FILE = "log.txt";
const size_t Logger::MAX_UUID_SAMPLES = 5;

bool CsvErrorSummary::has_errors() const {
    return invalid_columns > 0 || invalid_dates > 0 || invalid_unidades > 0 ||
           invalid_descuento > 0 || invalid_monto > 0 || invalid_uuid > 0 || missing_header;
}

Logger::Logger() = default;

Logger& Logger::instance() {
    static Logger instance;
    return instance;
}

void Logger::write(const string& message) {
    lock_guard<mutex> lock(mutex_);

    auto now = chrono::system_clock::now();
    time_t time = chrono::system_clock::to_time_t(now);
    tm tm_buf{};
#ifdef _WIN32
    localtime_s(&tm_buf, &time);
#else
    localtime_r(&time, &tm_buf);
#endif

    ostringstream timestamp;
    timestamp << put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");

    ofstream file(LOG_FILE, ios::app);
    if (!file.is_open()) {
        return;
    }

    file << timestamp.str() << '\n' << message << '\n';
}

void Logger::log_sftp_error(const string& message) {
    write("Error SFTP:\n" + message);
}

void Logger::accumulate_csv_summary(const string& filepath, const CsvErrorSummary& summary) {
    if (!summary.has_errors()) {
        return;
    }

    lock_guard<mutex> lock(mutex_);
    CsvErrorSummary& total = csv_summaries_[filepath];
    total.invalid_columns += summary.invalid_columns;
    total.invalid_dates += summary.invalid_dates;
    total.invalid_unidades += summary.invalid_unidades;
    total.invalid_descuento += summary.invalid_descuento;
    total.invalid_monto += summary.invalid_monto;
    total.invalid_uuid += summary.invalid_uuid;
    total.valid_rows += summary.valid_rows;
    total.missing_header = total.missing_header || summary.missing_header;
}

void Logger::flush_csv_summaries() {
    ostringstream body;

    {
        lock_guard<mutex> lock(mutex_);

        if (csv_summaries_.empty()) {
            return;
        }

        body << "Resumen errores CSV:\n";

        for (const auto& entry : csv_summaries_) {
            const string& file = entry.first;
            const CsvErrorSummary& s = entry.second;

            body << file << ":\n";

            if (s.missing_header)
                body << "  sin encabezado valido: 1\n";

            if (s.invalid_columns > 0)
                body << "  columnas invalidas: " << s.invalid_columns << '\n';

            if (s.invalid_dates > 0)
                body << "  fechas invalidas: " << s.invalid_dates << '\n';

            if (s.invalid_unidades > 0)
                body << "  unidades invalidas: " << s.invalid_unidades << '\n';

            if (s.invalid_descuento > 0)
                body << "  descuentos invalidos: " << s.invalid_descuento << '\n';

            if (s.invalid_monto > 0)
                body << "  montos invalidos: " << s.invalid_monto << '\n';

            if (s.invalid_uuid > 0)
                body << "  uuid invalidos: " << s.invalid_uuid << '\n';

            body << "  filas validas: " << s.valid_rows << '\n';
        }

        csv_summaries_.clear();
    }

    write(body.str());
}

void Logger::accumulate_api_error(const string& category) {
    lock_guard<mutex> lock(mutex_);
    if (category == "curl") {
        ++api_summary_.curl_errors;
    } else if (category == "http") {
        ++api_summary_.http_errors;
    } else if (category == "timeout") {
        ++api_summary_.timeouts;
    } else if (category == "empty_gender") {
        ++api_summary_.empty_gender;
    }
}

void Logger::accumulate_uuid_not_found(const string& uuid) {
    lock_guard<mutex> lock(mutex_);
    ++api_summary_.uuid_not_found;
    if (uuid_not_found_samples_.size() < MAX_UUID_SAMPLES) {
        uuid_not_found_samples_.push_back(uuid);
    }
}

void Logger::flush_api_summary() {
    ostringstream body;

    {
        lock_guard<mutex> lock(mutex_);

        if (api_summary_.curl_errors == 0 &&
            api_summary_.http_errors == 0 &&
            api_summary_.timeouts == 0 &&
            api_summary_.empty_gender == 0 &&
            api_summary_.uuid_not_found == 0) {
            return;
        }

        body << "Resumen errores API:\n";

        if (api_summary_.timeouts > 0)
            body << "  timeouts: " << api_summary_.timeouts << '\n';

        if (api_summary_.curl_errors > 0)
            body << "  errores curl: " << api_summary_.curl_errors << '\n';

        if (api_summary_.http_errors > 0)
            body << "  errores HTTP: " << api_summary_.http_errors << '\n';

        if (api_summary_.empty_gender > 0)
            body << "  respuestas sin genero: "
                 << api_summary_.empty_gender << '\n';

        if (api_summary_.uuid_not_found > 0) {
            body << "  uuid no encontrados: "
                 << api_summary_.uuid_not_found << '\n';

            for (const auto& sample : uuid_not_found_samples_) {
                body << "    ejemplo: " << sample << '\n';
            }
        }

        api_summary_ = ApiErrorSummary{};
        uuid_not_found_samples_.clear();
    }

    write(body.str());
}

void Logger::write_message(const string& message) {
    write(message);
}
