#include <hdr/hdr_histogram.h>

#include "naive_backend.h"
#include "kafka_backend.h"

int main(int argc, const char *argv[]) {
    using namespace lazylog;
    using namespace std::chrono;

    hdr_histogram *histogram;
    hdr_init(1, INT64_C(3600000000), 3, &histogram);

    Properties prop;
    ParseCommandLine(argc, argv, prop);
    int batch_size = std::stoi(prop.GetProperty("batch", "100000"));
    int round = std::stoi(prop.GetProperty("round", "1"));

    char payload[4097];
    memset(payload, 42, 4096);
    payload[4096] = '\0';
    KafkaBackend be;
    std::vector<LogEntry> entries(batch_size, payload);
    be.InitializeBackend(prop);

    uint64_t idx = 0;
    auto begin = high_resolution_clock::now();
    for (int i = 0; i < round; i++) {
        for (auto &e : entries) {
            e.log_idx = idx++;
            e.flags = 1;
        }
        auto start = high_resolution_clock::now();
        be.AppendBatch(entries);
        hdr_record_value(histogram, duration_cast<microseconds>(high_resolution_clock::now() - start).count());
    }
    auto dur = duration_cast<microseconds>(high_resolution_clock::now() - begin).count();

    std::cout << "Throughput: " << static_cast<double>(round) * batch_size * 1000000 / dur << " /s" << std::endl;

    hdr_percentiles_print(histogram, stdout, 5, 1, CLASSIC);
    hdr_close(histogram);

    return 0;
}