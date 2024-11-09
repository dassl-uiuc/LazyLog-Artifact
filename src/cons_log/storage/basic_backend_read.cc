#include <hdr/hdr_histogram.h>

#include <thread>

#include "../../rpc/common.h"
#include "naive_backend.h"
using namespace lazylog;
using namespace std::chrono;

std::unordered_map<int, uint64_t> read_durs;
uint64_t write_dur = 0;

void set_entries(std::vector<LogEntry> &entries, int round, int batch_size) {
    char *payload = new char[4097];
    for (int i = 0; i < batch_size; i++) {
        sprintf(payload, "%d", i + batch_size * round);
        entries[i].data = payload;
        entries[i].data.resize(4096);
        entries[i].size = entries[i].data.size();
    }
    delete payload;
}

bool verify_entry(LogEntry &e, uint64_t idx) { return atoll(e.data.c_str()) == idx; }

void reader_thread(Properties &prop, hdr_histogram *histogram, int thread_id) {
    int batch_size = std::stoi(prop.GetProperty("batch", "100000"));
    int round = std::stoi(prop.GetProperty("round", "1"));

    read_durs.insert({thread_id, 0});

    NaiveReadBackend be(thread_id);
    be.InitializeBackend(prop);

    uint64_t idx = 0;
    auto begin = high_resolution_clock::now();
    while (idx < batch_size * round) {
        LogEntry e;
        auto start = high_resolution_clock::now();
        be.ReadEntry(idx, e);
        // assert(verify_entry(e, idx));
        hdr_record_value_atomic(histogram, duration_cast<microseconds>(high_resolution_clock::now() - start).count());
        idx++;
    }
    read_durs[thread_id] = duration_cast<microseconds>(high_resolution_clock::now() - begin).count();
    be.FinalizeBackend();
    return;
}

void writer_thread(Properties &prop, hdr_histogram *histogram) {
    int batch_size = std::stoi(prop.GetProperty("batch", "100000"));
    int round = std::stoi(prop.GetProperty("round", "1"));

    char payload[4097];
    memset(payload, 42, 4096);
    payload[4096] = '\0';
    NaiveBackend be;
    std::vector<LogEntry> entries(batch_size, payload);
    be.InitializeBackend(prop);

    uint64_t idx = 0;
    auto begin = high_resolution_clock::now();
    for (int i = 0; i < round; i++) {
        // set_entries(entries, i, batch_size);
        for (auto &e : entries) {
            e.log_idx = idx++;
            e.flags = 1;
        }
        auto start = high_resolution_clock::now();
        be.AppendBatch(entries);
        hdr_record_value(histogram, duration_cast<microseconds>(high_resolution_clock::now() - start).count());
    }
    write_dur = duration_cast<microseconds>(high_resolution_clock::now() - begin).count();
    be.FinalizeBackend();
    return;
}

int main(int argc, const char *argv[]) {
    hdr_histogram *write_histogram;
    hdr_histogram *read_histogram;
    hdr_init(1, INT64_C(3600000000), 3, &write_histogram);
    hdr_init(1, INT64_C(3600000000), 3, &read_histogram);

    Properties prop;
    ParseCommandLine(argc, argv, prop);
    int batch_size = std::stoi(prop.GetProperty("batch", "100000"));
    int round = std::stoi(prop.GetProperty("round", "1"));
    int threads = std::stoi(prop.GetProperty("threadcount", "1"));

    std::cout << "batch: " << batch_size << ", round: " << round << ", threads: " << threads << std::endl;
    std::thread w_thd(writer_thread, std::ref(prop), write_histogram);

    std::vector<std::thread> reader_threads;
    for (int i = 0; i < threads; i++) {
        reader_threads.emplace_back(std::move(std::thread(reader_thread, std::ref(prop), read_histogram, i)));
    }

    w_thd.join();

    for (auto &thd : reader_threads) {
        thd.join();
    }

    double read_tput = 0;
    for (int i = 0; i < threads; i++) {
        read_tput += static_cast<double>(round) * batch_size * 1000000 / read_durs[i];
    }

    std::cout << "Read throughput: " << read_tput << " /s" << std::endl;
    std::cout << "Write throughput: " << static_cast<double>(round) * batch_size * 1000000 / write_dur << " /s"
              << std::endl;

    std::cout << "Read latency metrics" << std::endl;
    hdr_percentiles_print(read_histogram, stdout, 5, 1, CLASSIC);
    hdr_close(read_histogram);

    std::cout << "Write latency metrics" << std::endl;
    hdr_percentiles_print(write_histogram, stdout, 5, 1, CLASSIC);
    hdr_close(write_histogram);
    return 0;
}
