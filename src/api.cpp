#include "api.h"

#include "csv.h"
#include "logger.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <omp.h>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_set>

using namespace std;

namespace {

string extract_json_string(const string& json, const string& key) {
    const string patterns[] = {"\"" + key + "\":\"", "\"" + key + "\": \""};
    for (const auto& pattern : patterns) {
        size_t pos = json.find(pattern);
        if (pos != string::npos) {
            pos += pattern.size();
            size_t end = json.find('"', pos);
            if (end != string::npos) {
                return json.substr(pos, end - pos);
            }
        }
    }
    return "";
}

struct CurlResponse {
    string body;
    long http_code = 0;
};

size_t write_callback_impl(char* contents, size_t size, size_t nmemb, void* userp) {
    size_t total = size * nmemb;
    auto* response = static_cast<CurlResponse*>(userp);
    response->body.append(contents, total);
    return total;
}

static size_t shard_index(const string& key) {
    return std::hash<string>{}(key) % CACHE_SHARD_COUNT;
}

static size_t seen_shard_index(const string& key) {
    return std::hash<string>{}(key) % SEEN_SHARD_COUNT;
}

long parse_env_long(const char* name, long fallback) {
    const char* value = getenv(name);
    if (!value || value[0] == '\0') return fallback;
    try { return stol(value); } catch (...) { return fallback; }
}

int parse_env_int(const char* name, int fallback) {
    return static_cast<int>(parse_env_long(name, fallback));
}

int get_max_concurrent() {
    const int env = parse_env_int("API_MAX_CONCURRENT", API_MAX_CONCURRENT_DEFAULT);
    return clamp(env, 1, 128);
}

string rut_for_api(string rut) {
    rut.erase(remove(rut.begin(), rut.end(), '.'), rut.end());
    return rut;
}

mutex console_mutex;

void console_print(const string& text) {
    lock_guard<mutex> lock(console_mutex);
    fputs(text.c_str(), stdout);
    fflush(stdout);
}

void print_scan_summary(long long unique_count, double scan_seconds, long long cached_count,
                        long long to_query) {
    ostringstream out;
    out << fixed << setprecision(2);
    out << "UUID unicos encontrados: " << unique_count << " en " << scan_seconds << "s\n";
    out << "=== ETAPA 3: Resolucion UUID -> genero (solo faltantes) ===\n";
    out << "UUID unicos: " << unique_count << " | en cache: " << cached_count
        << " | por consultar: " << to_query << '\n';
    console_print(out.str());
}

void print_progress_snapshot(long long resolved_total, long long total_target, double velocity,
                             long long eta_seconds, long long elapsed_seconds) {
    ostringstream out;
    out << fixed << setprecision(1);
    out << "UUID resueltos: " << resolved_total << "/" << total_target
        << " | velocidad: " << velocity << "/s"
        << " | ETA: " << eta_seconds << "s"
        << " | transcurrido: " << elapsed_seconds << "s\n";
    console_print(out.str());
}

string make_cache_line(const string& uuid, const string& gender) {
    string line;
    line.reserve(uuid.size() + gender.size() + 2);
    line  = uuid;
    line += '|';
    line += gender;
    line += '\n';
    return line;
}

}  // namespace

size_t ApiClient::write_callback(void* contents, size_t size, size_t nmemb, void* userp) {
    return write_callback_impl(static_cast<char*>(contents), size, nmemb, userp);
}

ApiClient::ApiClient(const string& base_url, const string& email, const string& rut,
                     long timeout_seconds)
    : base_url_(base_url),
      email_(email),
      rut_display_(rut),
      rut_api_(rut_for_api(rut)),
      cache_path_("uuid_cache.txt"),
      timeout_seconds_(parse_env_long("API_TIMEOUT", timeout_seconds)),
      max_concurrent_(get_max_concurrent()) {}

ApiClient::~ApiClient() {
    stop_disk_writer();
}

string ApiClient::display_rut() const { return rut_display_; }
int    ApiClient::max_concurrent()  const { return max_concurrent_; }
long   ApiClient::timeout_seconds() const { return timeout_seconds_; }

void ApiClient::acquire_request_slot() {
    while (true) {
        int current = active_requests_.load(memory_order_relaxed);
        if (current < max_concurrent_) {
            if (active_requests_.compare_exchange_weak(
                    current, current + 1,
                    memory_order_acquire,
                    memory_order_relaxed)) {
                return;
            }
        }
        this_thread::yield();
    }
}

void ApiClient::release_request_slot() {
    active_requests_.fetch_sub(1, memory_order_release);
}

CURL* ApiClient::get_thread_curl() {
    static thread_local CURL* curl = nullptr;
    static thread_local bool configured = false;

    if (!curl) {
        curl = curl_easy_init();
        configured = false;
    }

    if (curl && !configured) {
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback_impl);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        configured = true;
    }

    return curl;
}

bool ApiClient::try_get_cached(const string& uuid, string& gender_out) const {
    const size_t idx = shard_index(uuid);
    const auto& shard = cache_shards_[idx];
    shared_lock<shared_mutex> lock(shard.mutex);
    const auto it = shard.map.find(uuid);
    if (it == shard.map.end()) return false;
    gender_out = it->second;
    return true;
}

void ApiClient::open_cache_writer() {
    if (cache_writer_.is_open()) return;
    cache_writer_.open(cache_path_, ios::app | ios::out);
    cache_writes_since_flush_.store(0, memory_order_relaxed);
    start_disk_writer();
}

void ApiClient::start_disk_writer() {
    if (disk_writer_running_.exchange(true, memory_order_acq_rel)) return;

    disk_writer_thread_ = thread([this]() {
        vector<string> batch;
        batch.reserve(512);
        long long writes_since_flush = 0;

        while (true) {
            batch.clear();
            {
                unique_lock<mutex> lock(disk_queue_mutex_);
                disk_queue_cv_.wait_for(lock, chrono::milliseconds(250), [this]() {
                    return !disk_write_queue_.empty() ||
                           !disk_writer_running_.load(memory_order_acquire);
                });

                while (!disk_write_queue_.empty() && batch.size() < 512) {
                    batch.push_back(move(disk_write_queue_.front()));
                    disk_write_queue_.pop();
                }

                if (disk_write_queue_.size() < DISK_QUEUE_HIGH_WATER / 2) {
                    disk_queue_cv_.notify_one();
                }

                const bool done = !disk_writer_running_.load(memory_order_acquire) &&
                                  disk_write_queue_.empty() && batch.empty();
                if (done) break;
            }

            if (batch.empty()) continue;

            if (cache_writer_.is_open()) {
                for (const string& line : batch) {
                    cache_writer_ << line;
                }
                writes_since_flush += static_cast<long long>(batch.size());
                if (writes_since_flush >= CACHE_FLUSH_EVERY) {
                    cache_writer_.flush();
                    writes_since_flush = 0;
                }
            }
        }

        if (cache_writer_.is_open()) cache_writer_.flush();
    });
}

void ApiClient::stop_disk_writer() {
    if (!disk_writer_running_.load(memory_order_acquire)) return;
    disk_writer_running_.store(false, memory_order_release);
    disk_queue_cv_.notify_all();
    if (disk_writer_thread_.joinable()) disk_writer_thread_.join();
}

void ApiClient::enqueue_cache_line(string line) {
    unique_lock<mutex> lock(disk_queue_mutex_);
    disk_queue_cv_.wait(lock, [this]() {
        return disk_write_queue_.size() < DISK_QUEUE_HIGH_WATER;
    });
    disk_write_queue_.push(move(line));
    disk_queue_cv_.notify_one();
}

void ApiClient::flush_cache_writer() {
    stop_disk_writer();
    if (cache_writer_.is_open()) cache_writer_.flush();
}

void ApiClient::flush_disk_cache() {
    flush_cache_writer();
}

void ApiClient::store_cache_entry(const string& uuid, const string& gender) {
    string line = make_cache_line(uuid, gender);

    const size_t idx = shard_index(uuid);
    auto& shard = cache_shards_[idx];
    {
        unique_lock<shared_mutex> lock(shard.mutex);
        shard.map.emplace(uuid, gender);
    }

    enqueue_cache_line(move(line));
    resolved_count_.fetch_add(1, memory_order_relaxed);
}

FetchResult ApiClient::fetch_gender_from_api_once(const string& uuid) {
    FetchResult result;
    acquire_request_slot();

    const string url = base_url_ + "/v1/person/" + uuid;
    CurlResponse response;
    CURL* curl = get_thread_curl();
    if (!curl) {
        release_request_slot();
        Logger::instance().accumulate_api_error("curl");
        result.status = Result::TEMP_ERROR;
        return result;
    }

    const string auth = "Authorization: Bearer " + jwt_;
    struct curl_slist* headers = curl_slist_append(nullptr, auth.c_str());

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_seconds_);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 3L);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

    const CURLcode code = curl_easy_perform(curl);
    if (code == CURLE_OPERATION_TIMEDOUT) {
        Logger::instance().accumulate_api_error("timeout");
        curl_slist_free_all(headers);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, nullptr);
        release_request_slot();
        result.status = Result::TEMP_ERROR;
        return result;
    }
    if (code != CURLE_OK) {
        Logger::instance().accumulate_api_error("curl");
        curl_slist_free_all(headers);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, nullptr);
        release_request_slot();
        result.status = Result::TEMP_ERROR;
        return result;
    }

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.http_code);
    curl_slist_free_all(headers);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, nullptr);
    release_request_slot();

    api_calls_.fetch_add(1, memory_order_relaxed);

    if (response.http_code == 401 || response.http_code == 403) {
        Logger::instance().accumulate_api_error("http");
        result.status = Result::AUTH_ERROR;
        return result;
    }
    if (response.http_code == 404) {
        Logger::instance().accumulate_uuid_not_found(uuid);
        result.status = Result::NOT_FOUND;
        return result;
    }
    if (response.http_code >= 500) {
        Logger::instance().accumulate_api_error("http");
        result.status = Result::TEMP_ERROR;
        return result;
    }
    if (response.http_code != 200) {
        Logger::instance().accumulate_api_error("http");
        result.status = Result::TEMP_ERROR;
        return result;
    }

    result.gender = extract_json_string(response.body, "gender");
    if (result.gender.empty()) {
        Logger::instance().accumulate_api_error("empty_gender");
        result.status = Result::NOT_FOUND;
        return result;
    }

    result.status = Result::OK;
    return result;
}

FetchResult ApiClient::fetch_gender_from_api(const string& uuid) {
    bool reauthed = false;

    for (int attempt = 0; attempt < API_MAX_TEMP_RETRIES; ++attempt) {
        FetchResult result = fetch_gender_from_api_once(uuid);

        if (result.status == Result::OK || result.status == Result::NOT_FOUND)
            return result;

        if (result.status == Result::AUTH_ERROR) {
            if (!reauthed && authenticate(false)) {
                reauthed = true;
                --attempt;
                continue;
            }
            return result;
        }

        if (attempt + 1 < API_MAX_TEMP_RETRIES)
            this_thread::sleep_for(chrono::milliseconds(50 * static_cast<int>(attempt + 1)));
    }

    return {Result::TEMP_ERROR, ""};
}

bool ApiClient::authenticate(bool verbose) {
    const string url = base_url_ + "/v1/login/authenticate";
    ostringstream body;
    body << "{\"email\":\"" << email_ << "\",\"rut\":\"" << rut_api_ << "\"}";
    const string json_body = body.str();

    acquire_request_slot();
    CurlResponse response;
    CURL* curl = get_thread_curl();
    if (!curl) {
        release_request_slot();
        Logger::instance().accumulate_api_error("curl");
        return false;
    }

    struct curl_slist* headers = curl_slist_append(nullptr, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(json_body.size()));
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 40L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 20L);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

    const CURLcode code = curl_easy_perform(curl);
    if (code == CURLE_OPERATION_TIMEDOUT) {
        Logger::instance().accumulate_api_error("timeout");
    } else if (code != CURLE_OK) {
        Logger::instance().accumulate_api_error("curl");
    } else {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.http_code);
    }

    curl_slist_free_all(headers);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, nullptr);
    curl_easy_setopt(curl, CURLOPT_POST, 0L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, nullptr);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0L);
    release_request_slot();

    ostringstream login_out;
    login_out << "===== LOGIN =====\n"
              << "EMAIL: " << email_ << '\n'
              << "RUT: " << rut_display_ << '\n'
              << "HTTP: " << response.http_code << '\n'
              << "BODY: " << response.body << '\n'
              << "=================\n";
    if (verbose) console_print(login_out.str());

    if (response.http_code != 200) {
        Logger::instance().accumulate_api_error("http");
        return false;
    }

    jwt_ = extract_json_string(response.body, "jwt");
    if (jwt_.empty()) {
        Logger::instance().accumulate_api_error("http");
        return false;
    }

    return true;
}

PipelineStats ApiClient::resolve_uuids_pipeline(const vector<string>& csv_files) {
    PipelineStats stats;
    open_cache_writer();
    pipeline_running_.store(true, memory_order_relaxed);
    resolved_count_.store(0, memory_order_relaxed);
    instant_resolved_.store(0, memory_order_relaxed);

    queue<string> work_queue;
    mutex queue_mutex;
    condition_variable queue_not_empty;
    condition_variable queue_not_full;
    const size_t max_queue = static_cast<size_t>(max_concurrent_) * 16;
    atomic<bool> producer_done{false};

    array<unordered_set<string>, SEEN_SHARD_COUNT> seen_shards;
    array<mutex, SEEN_SHARD_COUNT> seen_mutexes;
    for (auto& shard : seen_shards) shard.reserve(50'000);

    queue<string> retry_queue;
    mutex retry_mutex;

    atomic<long long> skipped_cache{0};
    atomic<long long> queued{0};
    atomic<long long> window_resolved{0};

    const double pipeline_start = omp_get_wtime();
    const double csv_start = pipeline_start;

    const long long report_interval = 10'000;
    atomic<long long> total_unique_uuids{0};
    atomic<long long> cached_at_scan{0};
    atomic<long long> api_start_ns{0};
    atomic<long long> last_reported_api_resolved{0};
    atomic<double>    last_report_wtime{0.0};
    atomic<long long> last_report_api_resolved{0};
    atomic<bool>      metrics_baseline_set{false};
    atomic<long long> unique_total{0};

    const int n_workers = max_concurrent_;
    const int n_threads = n_workers + 2;
    atomic<int> pipeline_remaining{n_workers + 1}; 

    const int saved_active_levels = omp_get_max_active_levels();
    if (saved_active_levels < 2) omp_set_max_active_levels(2);

#pragma omp parallel num_threads(n_threads) \
    shared(work_queue, queue_mutex, queue_not_empty, queue_not_full, producer_done, \
           seen_shards, seen_mutexes, retry_queue, retry_mutex, \
           skipped_cache, queued, window_resolved, csv_files, stats, \
           unique_total, total_unique_uuids, cached_at_scan, api_start_ns, \
           metrics_baseline_set, last_reported_api_resolved, last_report_wtime, \
           last_report_api_resolved, pipeline_remaining, csv_start, max_queue)
    {
        const int tid = omp_get_thread_num();

        if (tid == 0) {
            while (pipeline_running_.load(memory_order_relaxed)) {
                this_thread::sleep_for(chrono::seconds(1));

                {
                    const int retry_budget = max(1, max_concurrent_ / 4);
                    int moved = 0;
                    while (moved < retry_budget) {
                        string uuid;
                        {
                            lock_guard<mutex> rlock(retry_mutex);
                            if (retry_queue.empty()) break;
                            uuid = move(retry_queue.front());
                            retry_queue.pop();
                        }
                        {
                            unique_lock<mutex> wlock(queue_mutex);
                            if (work_queue.size() >= max_queue) {
                                lock_guard<mutex> rlock(retry_mutex);
                                retry_queue.push(move(uuid));
                                break;
                            }
                            work_queue.push(move(uuid));
                            pending_uuids_.fetch_add(1, memory_order_relaxed);
                        }
                        queue_not_empty.notify_one();
                        ++moved;
                    }
                }

                // Progreso
                const long long target       = total_unique_uuids.load(memory_order_relaxed);
                const long long start_ns     = api_start_ns.load(memory_order_relaxed);
                const long long cached       = cached_at_scan.load(memory_order_relaxed);
                const long long api_resolved = resolved_count_.load(memory_order_relaxed);

                if (target <= 0 || start_ns <= 0) continue;

                const double phase_start = static_cast<double>(start_ns) * 1e-9;
                const double now = omp_get_wtime();

                if (!metrics_baseline_set.load(memory_order_relaxed)) {
                    last_report_wtime.store(now, memory_order_relaxed);
                    last_report_api_resolved.store(api_resolved, memory_order_relaxed);
                    metrics_baseline_set.store(true, memory_order_relaxed);
                    continue;
                }

                const long long current_block = api_resolved / report_interval;
                const long long last_block =
                    last_reported_api_resolved.load(memory_order_relaxed) / report_interval;
                if (current_block <= last_block) continue;

                last_reported_api_resolved.store(current_block * report_interval,
                                                 memory_order_relaxed);

                const double prev_wtime      = last_report_wtime.load(memory_order_relaxed);
                const long long prev_api     = last_report_api_resolved.load(memory_order_relaxed);
                const double delta_t         = now - prev_wtime;
                const long long delta_r      = api_resolved - prev_api;
                double velocity = (delta_t > 0.0 && delta_r > 0)
                                  ? static_cast<double>(delta_r) / delta_t : 0.0;

                const long long remaining_api = max(0LL, (target - cached) - api_resolved);
                const long long eta_seconds   = velocity > 0.0
                    ? static_cast<long long>(static_cast<double>(remaining_api) / velocity + 0.5)
                    : 0LL;

                last_report_wtime.store(now, memory_order_relaxed);
                last_report_api_resolved.store(api_resolved, memory_order_relaxed);

                print_progress_snapshot(cached + api_resolved, target, velocity, eta_seconds,
                                        static_cast<long long>(now - phase_start + 0.5));
            }

        } else if (tid == 1) {
            scan_csv_files_for_uuids(
                csv_files,
                [this, &seen_shards, &seen_mutexes, &skipped_cache, &unique_total](
                    const string& uuid) -> bool {
                    const size_t si = seen_shard_index(uuid);
                    {
                        lock_guard<mutex> lock(seen_mutexes[si]);
                        if (!seen_shards[si].insert(uuid).second) return false;
                    }
                    unique_total.fetch_add(1, memory_order_relaxed);
                    string cached;
                    if (try_get_cached(uuid, cached)) {
                        skipped_cache.fetch_add(1, memory_order_relaxed);
                        cache_hits_.fetch_add(1, memory_order_relaxed);
                        return false;
                    }
                    return true;
                },
                [&](string uuid) {
                    {
                        unique_lock<mutex> lock(queue_mutex);
                        queue_not_full.wait(lock, [&]() {
                            return work_queue.size() < max_queue;
                        });
                        work_queue.push(move(uuid));
                        pending_uuids_.fetch_add(1, memory_order_relaxed);
                        queued.fetch_add(1, memory_order_relaxed);
                    }
                    queue_not_empty.notify_all();
                });

            stats.unique_uuids_seen = unique_total.load(memory_order_relaxed);
            stats.csv_scan_seconds  = omp_get_wtime() - csv_start;

            const long long queued_count        = queued.load(memory_order_relaxed);
            const long long skipped_cache_count = skipped_cache.load(memory_order_relaxed);

            print_scan_summary(stats.unique_uuids_seen, stats.csv_scan_seconds,
                               skipped_cache_count, queued_count);

            cached_at_scan.store(skipped_cache_count, memory_order_relaxed);
            total_unique_uuids.store(stats.unique_uuids_seen, memory_order_relaxed);
            api_start_ns.store(static_cast<long long>(omp_get_wtime() * 1e9),
                               memory_order_relaxed);
            metrics_baseline_set.store(false, memory_order_relaxed);

            producer_done.store(true, memory_order_relaxed);
            queue_not_empty.notify_all();  

            if (pipeline_remaining.fetch_sub(1, memory_order_acq_rel) == 1) {
                pipeline_running_.store(false, memory_order_relaxed);
            }

        } else {
            while (true) {
                string uuid;
                {
                    unique_lock<mutex> lock(queue_mutex);
                    queue_not_empty.wait(lock, [&]() {
                        return !work_queue.empty() || producer_done.load(memory_order_relaxed);
                    });
                    if (work_queue.empty()) {
                        if (producer_done.load(memory_order_relaxed)) {
                            bool retry_empty;
                            {
                                lock_guard<mutex> rlock(retry_mutex);
                                retry_empty = retry_queue.empty();
                            }
                            if (retry_empty) break;
                        }
                        continue;
                    }
                    uuid = move(work_queue.front());
                    work_queue.pop();
                    queue_not_full.notify_one();
                }

                pending_uuids_.fetch_sub(1, memory_order_relaxed);

                string cached;
                if (try_get_cached(uuid, cached)) {
                    cache_hits_.fetch_add(1, memory_order_relaxed);
                    continue;
                }

                const FetchResult fetched = fetch_gender_from_api(uuid);
                if (fetched.status == Result::OK) {
                    store_cache_entry(uuid, fetched.gender);
                    window_resolved.fetch_add(1, memory_order_relaxed);
                } else if (fetched.status == Result::NOT_FOUND) {
                    store_cache_entry(uuid, NOT_FOUND_MARKER);
                    window_resolved.fetch_add(1, memory_order_relaxed);
                } else {
                    lock_guard<mutex> lock(retry_mutex);
                    retry_queue.push(move(uuid));
                }
            }

            if (pipeline_remaining.fetch_sub(1, memory_order_acq_rel) == 1) {
                pipeline_running_.store(false, memory_order_relaxed);
            }
        }
    }

    omp_set_max_active_levels(saved_active_levels);

    
    while (true) {
        string uuid;
        {
            lock_guard<mutex> lock(retry_mutex);
            if (retry_queue.empty()) break;
            uuid = move(retry_queue.front());
            retry_queue.pop();
        }
        string cached;
        if (try_get_cached(uuid, cached)) continue;

        const FetchResult fetched = fetch_gender_from_api(uuid);
        if (fetched.status == Result::OK) {
            store_cache_entry(uuid, fetched.gender);
        } else if (fetched.status == Result::NOT_FOUND) {
            store_cache_entry(uuid, NOT_FOUND_MARKER);
        } else {
            lock_guard<mutex> lock(retry_mutex);
            retry_queue.push(move(uuid));
            this_thread::sleep_for(chrono::milliseconds(100));
        }
    }

    stats.unique_uuids_seen = unique_total.load(memory_order_relaxed);
    const long long queued_count        = queued.load(memory_order_relaxed);
    const long long skipped_cache_count = skipped_cache.load(memory_order_relaxed);
    const long long final_resolved      = resolved_count_.load(memory_order_relaxed);
    const long long total_target        = total_unique_uuids.load(memory_order_relaxed);
    const long long cached_count        = cached_at_scan.load(memory_order_relaxed);
    const double api_phase_start =
        static_cast<double>(api_start_ns.load(memory_order_relaxed)) * 1e-9;

    if (final_resolved > 0 && total_target > 0 &&
        final_resolved != last_reported_api_resolved.load(memory_order_relaxed)) {
        const double now         = omp_get_wtime();
        const double prev_wtime  = last_report_wtime.load(memory_order_relaxed);
        const long long prev_api = last_report_api_resolved.load(memory_order_relaxed);
        double velocity = 0.0;
        if (prev_wtime > 0.0) {
            const double delta_t    = now - prev_wtime;
            const long long delta_r = final_resolved - prev_api;
            if (delta_t > 0.0 && delta_r > 0)
                velocity = static_cast<double>(delta_r) / delta_t;
        }
        const long long remaining_api =
            max(0LL, (total_target - cached_count) - final_resolved);
        const long long eta_seconds = velocity > 0.0
            ? static_cast<long long>(static_cast<double>(remaining_api) / velocity + 0.5)
            : 0LL;
        print_progress_snapshot(cached_count + final_resolved, total_target, velocity,
                                eta_seconds,
                                static_cast<long long>(now - api_phase_start + 0.5));
    }

    stats.api_wall_seconds    = omp_get_wtime() - pipeline_start;
    stats.uuids_queued        = queued_count;
    stats.uuids_skipped_cache = skipped_cache_count;
    flush_cache_writer();

    return stats;
}

string ApiClient::lookup_gender(const string& uuid) const {
    const size_t idx = shard_index(uuid);
    const auto& shard = cache_shards_[idx];
    shared_lock<shared_mutex> lock(shard.mutex);
    const auto it = shard.map.find(uuid);
    if (it != shard.map.end()) {
        cache_hits_.fetch_add(1, memory_order_relaxed);
        return (it->second == NOT_FOUND_MARKER) ? "" : it->second;
    }
    return "";
}

void ApiClient::load_disk_cache(const string& path) {
    ifstream input(path);
    if (!input.is_open()) return;

    vector<pair<string, string>> entries;
    entries.reserve(3'500'000);
    string line;
    line.reserve(128);
    while (getline(input, line)) {
        if (line.empty()) continue;
        const size_t sep = line.find('|');
        if (sep == string::npos) continue;
        entries.emplace_back(line.substr(0, sep), line.substr(sep + 1));
    }
    cache_path_ = path;

    for (auto& entry : entries) {
        const size_t idx = shard_index(entry.first);
        auto& shard = cache_shards_[idx];
        unique_lock<shared_mutex> lock(shard.mutex);
        shard.map.emplace(move(entry.first), move(entry.second));
    }
}

void ApiClient::save_disk_cache_force(const string& path) const {
    vector<pair<string, string>> snapshot;
    snapshot.reserve(3'500'000);
    for (size_t i = 0; i < CACHE_SHARD_COUNT; ++i) {
        const auto& shard = cache_shards_[i];
        shared_lock<shared_mutex> lock(shard.mutex);
        for (const auto& entry : shard.map)
            snapshot.emplace_back(entry.first, entry.second);
    }
    ofstream output(path, ios::trunc);
    if (!output.is_open()) return;
    for (const auto& entry : snapshot)
        output << entry.first << '|' << entry.second << '\n';
}

long long ApiClient::cache_hits()     const { return cache_hits_.load(memory_order_relaxed); }
long long ApiClient::api_calls()      const { return api_calls_.load(memory_order_relaxed); }
long long ApiClient::resolved_count() const { return resolved_count_.load(memory_order_relaxed); }
long long ApiClient::pending_uuids()  const { return pending_uuids_.load(memory_order_relaxed); }

long long ApiClient::cache_size() const {
    long long total = 0;
    for (size_t i = 0; i < CACHE_SHARD_COUNT; ++i) {
        const auto& shard = cache_shards_[i];
        shared_lock<shared_mutex> lock(shard.mutex);
        total += static_cast<long long>(shard.map.size());
    }
    return total;
}
