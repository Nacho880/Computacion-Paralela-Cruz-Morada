#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <curl/curl.h>
#include <fstream>
#include <functional>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;

constexpr const char* NOT_FOUND_MARKER = "NOT_FOUND";
constexpr size_t CACHE_SHARD_COUNT = 64;
constexpr size_t SEEN_SHARD_COUNT = 64;
constexpr int API_MAX_CONCURRENT_DEFAULT = 96;
constexpr long API_TIMEOUT_DEFAULT = 30;
constexpr int API_MAX_TEMP_RETRIES = 3;

enum class Result {
    OK,
    NOT_FOUND,
    AUTH_ERROR,
    TEMP_ERROR
};

struct FetchResult {
    Result status = Result::TEMP_ERROR;
    string gender;
};

struct PipelineStats {
    double csv_scan_seconds = 0.0;
    double api_wall_seconds = 0.0;
    long long unique_uuids_seen = 0;
    long long uuids_queued = 0;
    long long uuids_skipped_cache = 0;
};

class ApiClient {
public:
    ApiClient(const string& base_url, const string& email, const string& rut,
              long timeout_seconds = API_TIMEOUT_DEFAULT);
    ~ApiClient();

    bool authenticate(bool verbose = true);
    PipelineStats resolve_uuids_pipeline(const vector<string>& csv_files);
    string lookup_gender(const string& uuid) const;

    void load_disk_cache(const string& path = "uuid_cache.txt");
    void flush_disk_cache();
    void save_disk_cache_force(const string& path = "uuid_cache.txt") const;

    string display_rut() const;
    int max_concurrent() const;
    long timeout_seconds() const;
    long long cache_hits() const;
    long long api_calls() const;
    long long resolved_count() const;
    long long pending_uuids() const;
    long long cache_size() const;

private:
    FetchResult fetch_gender_from_api_once(const string& uuid);
    FetchResult fetch_gender_from_api(const string& uuid);
    bool try_get_cached(const string& uuid, string& gender_out) const;
    void store_cache_entry(const string& uuid, const string& gender);
    void enqueue_cache_line(string line);       // ← recibe string ya formateado
    void flush_cache_writer();
    void open_cache_writer();
    void start_disk_writer();
    void stop_disk_writer();
    CURL* get_thread_curl();
    void acquire_request_slot();
    void release_request_slot();
    static size_t write_callback(void* contents, size_t size, size_t nmemb, void* userp);

    string base_url_;
    string email_;
    string rut_display_;
    string rut_api_;
    string jwt_;
    string cache_path_;
    long timeout_seconds_;
    int max_concurrent_;

    struct CacheShard {
        unordered_map<string, string> map;
        mutable shared_mutex mutex;
    };

    array<CacheShard, CACHE_SHARD_COUNT> cache_shards_;

    mutex disk_queue_mutex_;
    condition_variable disk_queue_cv_;
    queue<string> disk_write_queue_;
    atomic<bool> disk_writer_running_{false};
    thread disk_writer_thread_;
    ofstream cache_writer_;
    atomic<long long> cache_writes_since_flush_{0};
    static constexpr long  CACHE_FLUSH_EVERY      = 5000;
    static constexpr size_t DISK_QUEUE_HIGH_WATER = 16384;  

    atomic<int> active_requests_{0};

    mutable atomic<long long> cache_hits_{0};
    atomic<long long> api_calls_{0};
    atomic<long long> resolved_count_{0};
    atomic<long long> pending_uuids_{0};
    atomic<long long> instant_resolved_{0};
    atomic<bool> pipeline_running_{false};
};