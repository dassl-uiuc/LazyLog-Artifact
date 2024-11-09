#include <hdr/hdr_histogram.h>

#include <chrono>
#include <thread>
#include <unordered_map>

#include "../../utils/properties.h"
#include "../lazylog_cli.h"
#include "../lazylog_scalable_cli.h"

using namespace lazylog;
using namespace std::chrono;

std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations_reads;
std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations_writes;

std::vector<LogEntry> global_entries;

void reader_thread(int thd_id, hdr_histogram* histogram, const Properties& prop) {
    std::cout << "[mixed_bench]: starting reader thread " << thd_id << " ..." << std::endl;

    LazyLogScalableClient cli;
    auto modified_p = prop;
    modified_p.SetProperty("dur_log.client_id", std::to_string(thd_id));
    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    num_requests_and_durations_reads.insert({thd_id, {0, 0}});

    cli.Initialize(modified_p);

    uint64_t idx = 0;
    auto begin = high_resolution_clock::now();
    while (true) {
        std::vector<LogEntry> entries;
        auto start = high_resolution_clock::now();
        while (!cli.ReadEntries(idx, idx + 50 - 1, entries)) {
            if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) break;
            start = high_resolution_clock::now();
        };
        // if (entries.size() != 0) global_entries.insert(global_entries.end(), entries.begin(), entries.end());
        hdr_record_value_atomic(histogram, duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
        idx += 50;
        if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) break;
    }
    num_requests_and_durations_reads[thd_id] = {
        idx, duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
    std::cout << "[mixed_bench]: reader thread " << thd_id << " done reading " << idx << " entries" << std::endl;
    return;
}

void writer_thread(int thd_id, hdr_histogram* histogram, const Properties& prop) {
    std::cout << "[mixed_bench]: starting writer thread " << thd_id << " ..." << std::endl;

    LazyLogScalableClient cli;
    auto modified_p = prop;
    modified_p.SetProperty("dur_log.client_id", std::to_string(thd_id));
    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    num_requests_and_durations_writes.insert({thd_id, {0, 0}});

    cli.Initialize(modified_p);

    uint64_t idx = 0;

    std::string data(4096, 'A');
    auto begin = high_resolution_clock::now();
    while (true) {
        auto start = high_resolution_clock::now();
        cli.AppendEntryAll(data);
        hdr_record_value_atomic(histogram, duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
        idx++;
        if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) break;
    }
    num_requests_and_durations_writes[thd_id] = {
        idx, duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
    std::cout << "[mixed_bench]: writer thread " << thd_id << " done after executing " << idx << " requests"
              << std::endl;
    return;
}

long double compute_read_throughput() {
    long double tput = 0.0;
    for (auto& p : num_requests_and_durations_reads) {
        tput += static_cast<long double>(p.second.first) * 1.0e9 / p.second.second;
    }
    return tput;
}

long double compute_write_throughput() {
    long double tput = 0.0;
    for (auto& p : num_requests_and_durations_writes) {
        tput += static_cast<long double>(p.second.first) * 1.0e9 / p.second.second;
    }
    return tput;
}

int main(int argc, const char* argv[]) {
    hdr_histogram* read_histogram;
    hdr_init(1, INT64_C(3600000000), 3, &read_histogram);
    hdr_histogram* write_histogram;
    hdr_init(1, INT64_C(3600000000), 3, &write_histogram);

    Properties prop;
    ParseCommandLine(argc, argv, prop);

    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    int read_threads = std::stoll(prop.GetProperty("read_threadcount", "1"));
    int write_threads = std::stoll(prop.GetProperty("write_threadcount", "1"));

    std::cout << "[mixed_bench]: running " << read_threads << " reader and " << write_threads
              << " writer threads, each executing for " << runtime_secs << " seconds..." << std::endl;

    std::vector<std::thread> reader_threads;
    std::vector<std::thread> writer_threads;
    int thd_id = 0;
    for (int i = 0; i < read_threads; i++) {
        reader_threads.emplace_back(std::move(std::thread(reader_thread, thd_id, read_histogram, std::ref(prop))));
        thd_id++;
    }
    for (int i = 0; i < write_threads; i++) {
        writer_threads.emplace_back(std::move(std::thread(writer_thread, thd_id, write_histogram, std::ref(prop))));
        thd_id++;
    }
    for (auto& t : reader_threads) {
        t.join();
    }
    for (auto& t : writer_threads) {
        t.join();
    }

    // verify
    // for (uint64_t i = 0; i < global_entries.size(); i++) {
    //     if (!(global_entries[i].log_idx == i)) {
    //         std::cout << "[mixed_bench]: verification failed at index " << i << std::endl;
    //         return 1;
    //     }
    // }

    std::cout << "[mixed_bench]: read throughput " << compute_read_throughput() << " ops/sec" << std::endl;
    std::cout << "[mixed_bench]: write throughput " << compute_write_throughput() << " ops/sec " << std::endl;

    std::cout << "[mixed_bench]: read latency metrics " << std::endl;
    hdr_percentiles_print(read_histogram, stdout, 5, 1, CLASSIC);
    std::cout << "[mixed_bench]: read percentile latencies " << std::endl
              << "\tp50: " << hdr_value_at_percentile(read_histogram, 50.0) << std::endl
              << "\tp95: " << hdr_value_at_percentile(read_histogram, 95.0) << std::endl
              << "\tp99: " << hdr_value_at_percentile(read_histogram, 99.0) << std::endl
              << "\tp99.9: " << hdr_value_at_percentile(read_histogram, 99.9) << std::endl;
    hdr_close(read_histogram);

    std::cout << "[mixed_bench]: write latency metrics " << std::endl;
    hdr_percentiles_print(write_histogram, stdout, 5, 1, CLASSIC);
    std::cout << "[mixed_bench]: write percentile latencies " << std::endl
              << "\tp50: " << hdr_value_at_percentile(write_histogram, 50.0) << std::endl
              << "\tp95: " << hdr_value_at_percentile(write_histogram, 95.0) << std::endl
              << "\tp99: " << hdr_value_at_percentile(write_histogram, 99.0) << std::endl
              << "\tp99.9: " << hdr_value_at_percentile(write_histogram, 99.9) << std::endl;
    hdr_close(write_histogram);
    return 0;
}