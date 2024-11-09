#include <hdr/hdr_histogram.h>

#include <chrono>
#include <thread>
#include <unordered_map>

#include "../../utils/properties.h"
#include "../../utils/ratelimit.h"
#include "../lazylog_cli.h"

using namespace lazylog;
using namespace std::chrono;

std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations;

void writer_thread(int thd_id, hdr_histogram* histogram, const Properties& prop, int64_t ops_limit) {
    std::cout << "[append_bench]: starting thread " << thd_id << " ..." << std::endl;

    RateLimiter *rlim = nullptr;
    if (ops_limit > 0) {
        rlim = new RateLimiter(ops_limit, ops_limit);
    }

    LazyLogClient cli;
    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    uint64_t request_size = std::stoll(prop.GetProperty("request_size_bytes", "1024"));
    uint64_t node_id = std::stoll(prop.GetProperty("node_id", "0"));
    int threads = std::stoll(prop.GetProperty("threadcount", "1"));

    num_requests_and_durations.insert({thd_id, {0, 0}});

    Properties modified_p = prop;
    uint64_t client_id = node_id * threads + thd_id;
    modified_p.SetProperty("dur_log.client_id", std::to_string(client_id));
    std::cout << "[append_bench]: setting client id " << client_id << std::endl;
    cli.Initialize(modified_p);

    uint64_t idx = 0;
    std::string data(request_size, 'A');
    auto begin = high_resolution_clock::now();
    while (true) {
        if (rlim) rlim->Consume(1);
    
        auto start = high_resolution_clock::now();
        auto ret = cli.AppendEntryAll(data);
        hdr_record_value_atomic(histogram, duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
        idx++;
        if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) break;
    }
    num_requests_and_durations[thd_id] = {idx,
                                          duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
    std::cout << "[append_bench]: thread " << thd_id << " done" << std::endl;
    return;
}

long double compute_throughput() {
    long double tput = 0.0;
    for (auto& p : num_requests_and_durations) {
        tput += static_cast<long double>(p.second.first) * 1.0e9 / p.second.second;
    }
    return tput;
}

int main(int argc, const char* argv[]) {
    hdr_histogram* histogram;
    hdr_init(1, INT64_C(3600000000), 3, &histogram);

    Properties prop;
    ParseCommandLine(argc, argv, prop);

    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    uint64_t client_id = std::stoll(prop.GetProperty("node_id", "0"));
    int threads = std::stoll(prop.GetProperty("threadcount", "1"));
    const int64_t ops_limit = std::stoi(prop.GetProperty("limit.ops", "0"));
    int64_t per_thread_ops = ops_limit / threads;

    std::cout << "[append_bench]: running " << threads << " threads, each executing for " << runtime_secs
              << " seconds..." << std::endl;

    std::vector<std::thread> writer_threads;
    for (int i = 0; i < threads; i++) {
        writer_threads.emplace_back(std::move(std::thread(writer_thread, i, histogram, std::ref(prop), per_thread_ops)));
    }
    for (auto& t : writer_threads) {
        t.join();
    }

    std::cout << "[append_bench]: write throughput " << compute_throughput() << " ops/sec" << std::endl;

    std::cout << "[append_bench]: latency metrics " << std::endl;
    hdr_percentiles_print(histogram, stdout, 5, 1, CLASSIC);
    std::cout << "[append_bench]: percentile latencies " << std::endl
              << "\tp50: " << hdr_value_at_percentile(histogram, 50.0) << std::endl
              << "\tp95: " << hdr_value_at_percentile(histogram, 95.0) << std::endl
              << "\tp99: " << hdr_value_at_percentile(histogram, 99.0) << std::endl
              << "\tp99.9: " << hdr_value_at_percentile(histogram, 99.9) << std::endl;
    hdr_close(histogram);
    return 0;
}