#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

struct CsvErrorSummary {
    long long invalid_columns = 0;
    long long invalid_dates = 0;
    long long invalid_unidades = 0;
    long long invalid_descuento = 0;
    long long invalid_monto = 0;
    long long invalid_uuid = 0;
    long long valid_rows = 0;
    bool missing_header = false;

    bool has_errors() const;
};

struct ApiErrorSummary {
    long long curl_errors = 0;
    long long http_errors = 0;
    long long timeouts = 0;
    long long empty_gender = 0;
    long long uuid_not_found = 0;
};

class Logger {
public:
    static Logger& instance();

    void log_sftp_error(const string& message);
    void accumulate_csv_summary(const string& filepath, const CsvErrorSummary& summary);
    void flush_csv_summaries();
    void accumulate_api_error(const string& category);
    void accumulate_uuid_not_found(const string& uuid);
    void flush_api_summary();
    void write_message(const string& message);

private:
    Logger();
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    void write(const string& message);

    mutex mutex_;
    unordered_map<string, CsvErrorSummary> csv_summaries_;
    ApiErrorSummary api_summary_;
    vector<string> uuid_not_found_samples_;
    static const string LOG_FILE;
    static const size_t MAX_UUID_SAMPLES;
};
