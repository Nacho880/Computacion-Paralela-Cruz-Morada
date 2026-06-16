#include "metrics.h"

#include "csv.h"
#include "logger.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <omp.h>
#include <sstream>

using namespace std;

MetricsResult compute_metrics_streaming(const vector<string>& csv_files, const ApiClient& api) {
    MetricsResult result {};

    double sum_femenino = 0.0;
    double sum_masculino = 0.0;
    long long count_femenino = 0;
    long long count_masculino = 0;
    long long transactions = 0;

#pragma omp parallel for schedule(dynamic) reduction(+ : sum_femenino, sum_masculino, count_femenino, count_masculino, transactions)
    for (size_t i = 0; i < csv_files.size(); ++i) {
        const CsvErrorSummary summary =
            parse_csv_file_streaming(csv_files[i], [&](const Transaction& tx) {
                const string gender = api.lookup_gender(tx.codigo_cliente);
                ++transactions;

                if (gender == "FEMENINO") {
                    sum_femenino += tx.monto_aplicado;
                    ++count_femenino;
                } else if (gender == "MASCULINO") {
                    sum_masculino += tx.monto_aplicado;
                    ++count_masculino;
                }
            });

#pragma omp critical(csv_log)
        Logger::instance().accumulate_csv_summary(csv_files[i], summary);
    }

    result.count_femenino = count_femenino;
    result.count_masculino = count_masculino;
    result.transactions_processed = transactions;
    result.promedio_femenino = count_femenino > 0 ? sum_femenino / static_cast<double>(count_femenino) : 0.0;
    result.promedio_masculino =
        count_masculino > 0 ? sum_masculino / static_cast<double>(count_masculino) : 0.0;

    return result;
}

string format_results(const MetricsResult& result, double elapsed_seconds) {
    ostringstream out;
    out << fixed << setprecision(2);
    out << "FEMENINO = " << result.promedio_femenino << '\n';
    out << "MASCULINO = " << result.promedio_masculino << '\n';
    out << "TIEMPO = " << elapsed_seconds << " segundos";
    return out.str();
}

string format_timing_report(const TimingReport& timing) {
    ostringstream out;
    out << fixed << setprecision(2);
    out << "## REPORTE DE TIEMPOS\n";
    out << "Descarga SFTP: " << timing.sftp_seconds << "s\n";
    out << "Carga cache: " << timing.cache_load_seconds << "s\n";
    out << "Resolucion API: " << timing.api_seconds << "s\n";
    out << "Calculo metricas: " << timing.metrics_seconds << "s\n";
    out << "\nTiempo total: " << timing.total_seconds << "s\n";
    return out.str();
}

void save_results(const string& content) {
    ofstream file("resultados.txt", ios::trunc);
    if (file.is_open()) {
        file << content << '\n';
    }
}
