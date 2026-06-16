#pragma once

#include <string>
#include <vector>

#include "api.h"

using namespace std;

struct MetricsResult {
    double promedio_femenino;
    double promedio_masculino;
    long long count_femenino;
    long long count_masculino;
    long long transactions_processed;
};

struct TimingReport {
    double sftp_seconds = 0.0;
    double cache_load_seconds = 0.0;
    double csv_scan_seconds = 0.0;
    double api_seconds = 0.0;
    double metrics_seconds = 0.0;
    double total_seconds = 0.0;
};

MetricsResult compute_metrics_streaming(const vector<string>& csv_files, const ApiClient& api);
string format_results(const MetricsResult& result, double elapsed_seconds);
string format_timing_report(const TimingReport& timing);
void save_results(const string& content);
