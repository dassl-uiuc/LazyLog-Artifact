#include <hdr/hdr_histogram.h>

#include <chrono>
#include <thread>
#include <unordered_map>

#include "../../utils/properties.h"
#include "../lazylog_scalable_cli.h"

using namespace lazylog;
using namespace std::chrono;

std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations;

void reader_thread(int thd_id, hdr_histogram* histogram, const Properties& prop) {
    std::cout << "[mixed_bench]: starting reader thread " << thd_id << " ..." << std::endl;

    LazyLogScalableClient cli;
    auto modified_p = prop;
    modified_p.SetProperty("dur_log.client_id", std::to_string(thd_id));
    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    uint64_t request_count = std::stoll(prop.GetProperty("request_count", "10000"));
    uint64_t batch_size = std::stoll(prop.GetProperty("batch_size", "25"));
    num_requests_and_durations.insert({thd_id, {0, 0}});

    cli.Initialize(modified_p);

    uint64_t num_requests = 0;
    uint64_t idx = 0;
    auto begin = high_resolution_clock::now();
    if (batch_size != 1) {
        while (true) {
            std::vector<LogEntry> entries;
            auto start = high_resolution_clock::now();
            if (idx + batch_size > request_count) idx = 0;
            while (!cli.ReadEntries(idx, idx + batch_size - 1, entries)) {
                if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) {
                    goto end;
                };
            }
            hdr_record_value_atomic(histogram,
                                    duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
            idx = (idx + batch_size) % request_count;
            num_requests += batch_size;
            if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) break;
        }
    } else {
        while (true) {
            std::string data;
            auto start = high_resolution_clock::now();
            while (!cli.ReadEntry(idx, data)) {
                if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) {
                    goto end;
                };
            }
            hdr_record_value_atomic(histogram,
                                    duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
            idx = (idx + 1) % request_count;
            num_requests++;
            if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) break;
        }
    }
end:
    num_requests_and_durations[thd_id] = {num_requests,
                                          duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
    std::cout << "[mixed_bench]: reader thread " << thd_id << " done reading " << num_requests << " entries"
              << std::endl;
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

    std::cout << "[read_bench]: read throughput " << compute_throughput() << " ops/sec" << std::endl;

    std::cout << "[read_bench]: latency metrics " << std::endl;
    hdr_percentiles_print(histogram, stdout, 5, 1, CLASSIC);
    std::cout << "[read_bench]: percentile latencies " << std::endl
              << "\tp50: " << hdr_value_at_percentile(histogram, 50.0) << std::endl
              << "\tp95: " << hdr_value_at_percentile(histogram, 95.0) << std::endl
              << "\tp99: " << hdr_value_at_percentile(histogram, 99.0) << std::endl
              << "\tp99.9: " << hdr_value_at_percentile(histogram, 99.9) << std::endl;
    hdr_close(histogram);
    return 0;
}