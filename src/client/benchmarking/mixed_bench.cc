#include <hdr/hdr_histogram.h>

#include <chrono>
#include <thread>
#include <unordered_map>

#include "../../utils/properties.h"
#include "../lazylog_cli.h"

using namespace lazylog;
using namespace std::chrono;

std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations_reads;
std::unordered_map<int, std::pair<uint64_t, uint64_t>> num_requests_and_durations_writes;

void reader_thread(int thd_id, hdr_histogram* histogram, const Properties& prop) {
    std::cout << "[mixed_bench]: starting reader thread " << thd_id << " ..." << std::endl;

    LazyLogClient cli;
    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    num_requests_and_durations_reads.insert({thd_id, {0, 0}});

    cli.Initialize(prop);

    uint64_t idx = 0;
    auto begin = high_resolution_clock::now();
    while (true) {
        std::string data;
        auto start = high_resolution_clock::now();
        assert(cli.ReadEntry(idx, data));
        hdr_record_value_atomic(histogram, duration_cast<nanoseconds>(high_resolution_clock::now() - start).count());
        idx++;
        if (duration_cast<seconds>(high_resolution_clock::now() - begin).count() >= runtime_secs) break;
    }
    num_requests_and_durations_reads[thd_id] = {
        idx, duration_cast<nanoseconds>(high_resolution_clock::now() - begin).count()};
    std::cout << "[mixed_bench]: reader thread " << thd_id << " done" << std::endl;
    return;
}

void writer_thread(int thd_id, hdr_histogram* histogram, const Properties& prop) {
    std::cout << "[mixed_bench]: starting writer thread " << thd_id << " ..." << std::endl;

    LazyLogClient cli;
    uint64_t runtime_secs = std::stoll(prop.GetProperty("runtime_secs", "180"));
    num_requests_and_durations_writes.insert({thd_id, {0, 0}});

    cli.Initialize(prop);

    uint64_t idx = 0;
    std::string data(4095, 'A');

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
    std::cout << "[mixed_bench]: writer thread " << thd_id << " done" << std::endl;
    return;
}

double compute_read_throughput() {
    double tput;
    for (auto& p : num_requests_and_durations_reads) {
        tput += double(p.second.first) * 1.0e9 / p.second.second;
    }
    return tput;
}

double compute_write_throughput() {
    double tput;
    for (auto& p : num_requests_and_durations_writes) {
        tput += double(p.second.first) * 1.0e9 / p.second.second;
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
    int threads = std::stoll(prop.GetProperty("threadcount", "1"));

    std::cout << "[mixed_bench]: running " << threads << " reader and writer threads, each executing for "
              << runtime_secs << " seconds..." << std::endl;

    std::vector<std::thread> reader_threads;
    std::vector<std::thread> writer_threads;
    for (int i = 0; i < threads; i++) {
        reader_threads.emplace_back(std::move(std::thread(reader_thread, i, read_histogram, std::ref(prop))));
    }
    for (int i = 0; i < threads; i++) {
        writer_threads.emplace_back(std::move(std::thread(writer_thread, i, write_histogram, std::ref(prop))));
    }
    for (auto& t : reader_threads) {
        t.join();
    }
    for (auto& t : writer_threads) {
        t.join();
    }

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