#include "csv.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstring>
#include <fstream>
#include <limits>
#include <omp.h>
#include <sstream>
#include <unordered_set>

using namespace std;

namespace {

constexpr int EXPECTED_COLUMNS = 10;

const vector<string> EXPECTED_HEADER = {
    "FECHA", "CANAL", "SKU", "PRODUCTO", "UNIDADES",
    "PORCENTAJE DESCUENTO", "MONTO APLICADO", "BOLETA", "LOCAL", "CODIGO CLIENTE"};



string to_upper_ascii(const string& value) {
    string out = value;
    transform(out.begin(), out.end(), out.begin(),
              [](unsigned char c) { return static_cast<char>(toupper(c)); });
    return out;
}

string_view strip_outer_quotes_view(string_view value) {
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        return string_view(value.data() + 1, value.size() - 2);
    }
    return value;
}

char detect_delimiter(const string& line) {
    const string upper = to_upper_ascii(line);
    if (upper.find("FECHA;") != string::npos || upper.find("\"FECHA\";") != string::npos) {
        return ';';
    }
    return ',';
}

bool split_csv_line_to_views(const string& line, char delimiter, array<string_view, EXPECTED_COLUMNS>& fields) {
    size_t field_idx = 0;
    size_t start = 0;
    bool in_quotes = false;

    for (size_t i = 0; i < line.size() && field_idx < EXPECTED_COLUMNS; ++i) {
        const char c = line[i];
        if (c == '"') {
            if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                ++i;
                continue;
            }
            in_quotes = !in_quotes;
        } else if (c == delimiter && !in_quotes) {
            fields[field_idx++] = strip_outer_quotes_view(string_view(line.data() + start, i - start));
            start = i + 1;
        }
    }

    if (field_idx < EXPECTED_COLUMNS) {
        fields[field_idx++] = strip_outer_quotes_view(string_view(line.data() + start, line.size() - start));
    }

    return field_idx == EXPECTED_COLUMNS;
}

bool is_digit_at(string_view value, size_t index) {
    return isdigit(static_cast<unsigned char>(value[index]));
}

bool is_valid_date(string_view fecha) {
    if (fecha.size() < 10 || fecha[4] != '-' || fecha[7] != '-') {
        return false;
    }

    for (size_t i = 0; i < 10; ++i) {
        if (i == 4 || i == 7) {
            continue;
        }
        if (!is_digit_at(string(fecha), i)) {
            return false;
        }
    }

    if (fecha.size() == 10) {
        return true;
    }

    if (fecha.size() < 16 || fecha[10] != 'T' || fecha[13] != ':') {
        return false;
    }

    for (size_t i = 11; i < 13; ++i) {
        if (!is_digit_at(string(fecha), i)) {
            return false;
        }
    }
    for (size_t i = 14; i < 16; ++i) {
        if (!is_digit_at(string(fecha), i)) {
            return false;
        }
    }

    if (fecha.size() == 16) {
        return true;
    }

    if (fecha.size() >= 19 && fecha[16] == ':') {
        for (size_t i = 17; i < 19; ++i) {
            if (!is_digit_at(string(fecha), i)) {
                return false;
            }
        }
        return true;
    }

    return false;
}

bool parse_int_field(const string& value, int& out) {
    if (value.empty()) {
        return false;
    }
    const char* ptr = value.c_str();
    const char* end = ptr + value.size();
    bool negative = false;
    if (*ptr == '+' || *ptr == '-') {
        negative = (*ptr == '-');
        ++ptr;
        if (ptr == end) {
            return false;
        }
    }
    long long result = 0;
    while (ptr < end) {
        const char c = *ptr++;
        if (c < '0' || c > '9') {
            return false;
        }
        result = result * 10 + (c - '0');
        if (result > numeric_limits<int>::max()) {
            return false;
        }
    }
    out = negative ? static_cast<int>(-result) : static_cast<int>(result);
    return true;
}

bool parse_double_field(const string& value, double& out) {
    if (value.empty()) {
        return false;
    }
    const char* ptr = value.c_str();
    const char* end = ptr + value.size();
    bool negative = false;
    if (*ptr == '+' || *ptr == '-') {
        negative = (*ptr == '-');
        ++ptr;
        if (ptr == end) {
            return false;
        }
    }

    double integer_part = 0.0;
    while (ptr < end && *ptr >= '0' && *ptr <= '9') {
        integer_part = integer_part * 10.0 + (*ptr++ - '0');
    }

    double fraction_part = 0.0;
    double fraction_scale = 1.0;
    if (ptr < end && (*ptr == '.' || *ptr == ',')) {
        ++ptr;
        while (ptr < end && *ptr >= '0' && *ptr <= '9') {
            fraction_part = fraction_part * 10.0 + (*ptr++ - '0');
            fraction_scale *= 10.0;
        }
    }

    if (ptr != end) {
        return false;
    }

    out = integer_part + (fraction_scale > 1.0 ? fraction_part / fraction_scale : 0.0);
    if (negative) {
        out = -out;
    }
    return true;
}

bool is_header_line(const array<string_view, EXPECTED_COLUMNS>& fields) {
    for (int i = 0; i < EXPECTED_COLUMNS; ++i) {
        if (to_upper_ascii(string(fields[static_cast<size_t>(i)])) != EXPECTED_HEADER[static_cast<size_t>(i)]) {
            return false;
        }
    }
    return true;
}

bool validate_uuid_format(const string& uuid) {
    if (uuid.size() != 36) {
        return false;
    }
    for (size_t i = 0; i < uuid.size(); ++i) {
        char c = uuid[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (c != '-') {
                return false;
            }
        } else if (!isxdigit(static_cast<unsigned char>(c))) {
            return false;
        }
    }
    return true;
}

}  

CsvErrorSummary parse_csv_file_streaming(const string& filepath, const TransactionCallback& on_row) {
    CsvErrorSummary summary;

    ifstream input(filepath);
    if (!input.is_open()) {
        summary.missing_header = true;
        return summary;
    }

    string line;
    int line_number = 0;
    bool header_skipped = false;
    char delimiter = ',';

    while (getline(input, line)) {
        ++line_number;

        if (line.empty()) {
            continue;
        }

        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        if (line_number == 1) {
            delimiter = detect_delimiter(line);
        }

        array<string_view, EXPECTED_COLUMNS> fields;
        if (!split_csv_line_to_views(line, delimiter, fields)) {
            ++summary.invalid_columns;
            continue;
        }

        if (!header_skipped) {
            if (is_header_line(fields)) {
                header_skipped = true;
                continue;
            }
            header_skipped = true;
        }

        Transaction tx {};
        tx.fecha.assign(fields[0]);
        tx.canal.assign(fields[1]);
        tx.sku.assign(fields[2]);
        tx.producto.assign(fields[3]);

        if (!is_valid_date(fields[0])) {
            ++summary.invalid_dates;
            continue;
        }

        if (!parse_int_field(string(fields[4]), tx.unidades) || tx.unidades < 0) {
            ++summary.invalid_unidades;
            continue;
        }

        if (!parse_double_field(string(fields[5]), tx.porcentaje_descuento)) {
            ++summary.invalid_descuento;
            continue;
        }

        if (!parse_double_field(string(fields[6]), tx.monto_aplicado) || tx.monto_aplicado < 0) {
            ++summary.invalid_monto;
            continue;
        }

        tx.boleta.assign(fields[7]);
        tx.local.assign(fields[8]);
        tx.codigo_cliente.assign(fields[9]);

        if (!validate_uuid_format(tx.codigo_cliente)) {
            ++summary.invalid_uuid;
            continue;
        }

        ++summary.valid_rows;
        on_row(tx);
    }

    if (!header_skipped && line_number > 0) {
        summary.missing_header = true;
    }

    return summary;
}

void parse_csv_uuids_streaming(const string& filepath, const UuidCallback& on_uuid) {
    ifstream input(filepath);
    if (!input.is_open()) {
        return;
    }

    string line;
    line.reserve(256);
    int line_number = 0;
    bool header_skipped = false;
    char delimiter = ',';

    while (getline(input, line)) {
        ++line_number;
        if (line.empty()) {
            continue;
        }
        if (line.back() == '\r') {
            line.pop_back();
        }
        if (line_number == 1) {
            delimiter = detect_delimiter(line);
        }

        array<string_view, EXPECTED_COLUMNS> fields;
        if (!split_csv_line_to_views(line, delimiter, fields)) {
            continue;
        }

        if (!header_skipped) {
            if (is_header_line(fields)) {
                header_skipped = true;
                continue;
            }
            header_skipped = true;
        }

        if (!is_valid_date(fields[0])) {
            continue;
        }

        int unidades = 0;
        double descuento = 0.0;
        double monto = 0.0;
        if (!parse_int_field(string(fields[4]), unidades) || unidades < 0) {
            continue;
        }
        if (!parse_double_field(string(fields[5]), descuento)) {
            continue;
        }
        if (!parse_double_field(string(fields[6]), monto) || monto < 0) {
            continue;
        }

        string uuid(fields[9]);
        if (!validate_uuid_format(uuid)) {
            continue;
        }

        on_uuid(move(uuid));
    }
}

long long scan_csv_files_for_uuids(const vector<string>& files,
                                   const function<bool(const string&)>& should_enqueue,
                                   const function<void(string)>& enqueue_uuid) {
    atomic<long long> unique_seen{0};

    int scan_threads;
    if (omp_in_parallel()) {
        scan_threads = min(32, max(1, static_cast<int>(files.size())));
    } else {
        scan_threads = min(static_cast<int>(files.size()), omp_get_max_threads());
    }

#pragma omp parallel for num_threads(scan_threads) schedule(dynamic)
    for (size_t i = 0; i < files.size(); ++i) {
        parse_csv_uuids_streaming(files[i], [&](string uuid) {
            unique_seen.fetch_add(1, memory_order_relaxed);
            if (should_enqueue(uuid)) {
                enqueue_uuid(move(uuid));
            }
        });
    }

    return unique_seen.load(memory_order_relaxed);
}
