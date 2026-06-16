#pragma once

#include <functional>
#include <string>
#include <unordered_set>
#include <vector>

#include "logger.h"

using namespace std;

struct Transaction {
    string fecha;
    string canal;
    string sku;
    string producto;
    int unidades;
    double porcentaje_descuento;
    double monto_aplicado;
    string boleta;
    string local;
    string codigo_cliente;
};

using TransactionCallback = function<void(const Transaction&)>;
using UuidCallback = function<void(string)>;

CsvErrorSummary parse_csv_file_streaming(const string& filepath, const TransactionCallback& on_row);
void parse_csv_uuids_streaming(const string& filepath, const UuidCallback& on_uuid);
long long scan_csv_files_for_uuids(const vector<string>& files,
                                   const function<bool(const string&)>& should_enqueue,
                                   const function<void(string)>& enqueue_uuid);
