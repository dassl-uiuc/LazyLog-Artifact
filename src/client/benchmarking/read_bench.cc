#include <hdr/hdr_histogram.h>

#include <chrono>
#include <thread>
#include <unordered_map>

#include "../../utils/properties.h"
#include "../lazylog_cli.h"

using namespace lazylog;
using namespace std::chrono;

std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations;

void reader_thread(int thd_id, hdr_histogram* histogram, const Properties& prop) {
    std::cout << "[read_bench]: starting thread " << thd_id << " ..." << std::endl;

    LazyLogClient cli;
    uint64_t request_count = std::stoll(prop.GetProperty("request_count", "10000"));
    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    num_requests_and_durations.insert({thd_id, {0, 0}});

    cli.Initialize(prop);

    uint64_t idx = 0;
    uint64_t num_request = 0;
    auto begin = high_resolution_clock::now();
    while (true) {
        std::string data;
        auto start = high_resolution_clock::now();
        assert(cli.ReadEntry(idx, data));
        hdr_record_value_atomic(histogram, duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
        idx = (idx + 1) % request_count;
        num_request++;
        if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) break;
    }
    num_requests_and_durations[thd_id] = {num_request,
                                          duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
    std::cout << "[read_bench]: thread " << thd_id << " done" << std::endl;
    return;
}

double compute_throughput(uint64_t num_requests) {
    double tput;
    for (auto& p : num_requests_and_durations) {
        tput += double(p.second.first) * 1.0e9 / p.second.second;
    }
    return tput;
}

int main(int argc, const char* argv[]) {
    hdr_histogram* histogram;
    hdr_init(1, INT64_C(3600000000), 3, &histogram);

    Properties prop;
    ParseCommandLine(argc, argv, prop);

    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    uint64_t request_count = std::stoll(prop.GetProperty("request_count", "10000"));
    int threads = std::stoll(prop.GetProperty("threadcount", "1"));

    std::cout << "[read_bench]: running " << threads << " threads, each executing for " << runtime_secs << " seconds..."
              << std::endl;

    std::vector<std::thread> reader_threads;
    for (int i = 0; i < threads; i++) {
        reader_threads.emplace_back(std::move(std::thread(reader_thread, i, histogram, std::ref(prop))));
    }
    for (auto& t : reader_threads) {
        t.join();
    }

    std::cout << "[read-bench]: read throughput " << compute_throughput(request_count) << " ops/sec" << std::endl;

    std::cout << "[read-bench]: latency metrics " << std::endl;
    hdr_percentiles_print(histogram, stdout, 5, 1, CLASSIC);
    std::cout << "[read-bench]: percentile latencies " << std::endl
              << "\tp50: " << hdr_value_at_percentile(histogram, 50.0) << std::endl
              << "\tp95: " << hdr_value_at_percentile(histogram, 95.0) << std::endl
              << "\tp99: " << hdr_value_at_percentile(histogram, 99.0) << std::endl
              << "\tp99.9: " << hdr_value_at_percentile(histogram, 99.9) << std::endl;
    hdr_close(histogram);
    return 0;
}