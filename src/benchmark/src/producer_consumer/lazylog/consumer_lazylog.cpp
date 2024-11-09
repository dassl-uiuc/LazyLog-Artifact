#include <client/lazylog_cli.h>
#include <utils/properties.h>

#include <chrono>
#include <commons.hpp>
#include <functional>
#include <future>
#include <lazylog/consumer_lazylog.hpp>
#include <string>
#include <thread>

namespace OpenMsgCpp {

extern std::unordered_map<uint64_t, std::chrono::_V2::steady_clock::time_point> produce_time;

consumerLazylog::consumerLazylog(workload load, Properties prop) : consumer(load) {
    log_info("LazyLog consumer constructor");
    Properties modified_prop = prop;
    auto numProducer = std::stoi(load.getConfig("producersPerTopic"));
    modified_prop.SetProperty("dur_log.client_id", std::to_string(numProducer));
    log_info("setting consumer client_id to " + std::to_string(numProducer));
    llClient.Initialize(modified_prop);
    currIdx = 0;
    try {
        restInterval = std::stoi(load.getConfig("restInterval"));
    } catch (const std::exception &e) {
        log_warn("restInterval Not set: setting to default 0");
        restInterval = 0;
    }
    try {
        batchSize = std::stoi(load.getConfig("batchSize"));
    } catch (const std::exception &e) {
        log_warn("batchSize Not set: setting to default 5000");
        batchSize = 5000;
    }
    consumeDelay = std::stoull(load.getConfig("consumerDelayMilli"));
}

consumerLazylog::consumerLazylog(workload load) : consumer(load) {
    log_info("LazyLog consumer constructor");
    currIdx = 0;
    try {
        restInterval = std::stoi(load.getConfig("restInterval"));
    } catch (const std::exception &e) {
        log_warn("restInterval Not set: setting to default 0");
        restInterval = 0;
    }
    try {
        batchSize = std::stoi(load.getConfig("batchSize"));
    } catch (const std::exception &e) {
        log_warn("batchSize Not set: setting to default 5000");
        batchSize = 5000;
    }
}

void consumerLazylog::initClient(Properties prop) { llClient.Initialize(prop); }

void consumerLazylog::run() {
    log_info("Runnig Lazylog consumer for " + load.getConfig("testDurationMinutes") + "m...");
    auto msgSize = std::stoi(load.getConfig("messageSize"));
    payload = read_all_bytes(load.getConfig("payloadFile"), msgSize);
    auto duration = std::chrono::minutes(std::stoi(load.getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - startTime < duration) {
        for (int i = 0; i < batchSize; i++) {
            if (!consume()) {
                log_warn("fail to consume logIdx " + std::to_string(currIdx));
            }
            if (std::chrono::steady_clock::now() - startTime > duration) {
                break;
            }
        }
        if (restInterval > 0) {
            sleep_for_nanoseconds(std::chrono::nanoseconds(restInterval));
        }
    }

    log_info("consume " + std::to_string(currIdx - 1) + " entries in " + load.getConfig("testDurationMinutes") + "m");
    if (!latencyLog.empty()) {
        write_to_file(latencyLog, latency);
        log_info("Written latency to file " + latencyLog);
    }
}

int consumerLazylog::consume(std::atomic<int> &tail, std::vector<int> &readLat,
                             std::vector<std::chrono::high_resolution_clock::time_point> &writeTime) {
    std::string data;
    if (currIdx >= tail.load()) return -1;
    auto start = std::chrono::high_resolution_clock::now();
    if (llClient.ReadEntry(currIdx, data)) {
        auto duration = std::chrono::high_resolution_clock::now() - writeTime[currIdx];
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
        readLat.push_back(static_cast<int>(nanoseconds.count()));

        currIdx++;
        return 1;
    }
    return -1;
}

int consumerLazylog::consume(std::atomic<int> *tail, std::vector<int> *readLat,
                             std::vector<std::chrono::high_resolution_clock::time_point> *writeTime) {
    std::string data;
    if (currIdx >= tail->load()) return -1;
    auto start = std::chrono::high_resolution_clock::now();
    if (llClient.ReadEntry(currIdx, data)) {
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
        readLat->push_back(static_cast<int>(nanoseconds.count()));

        currIdx++;
        return 1;
    }
    return -1;
}

// int consumerLazylog::specConsume(std::vector<int> *readLat, std::vector<int> *specReadLat) {
//     std::string data;
//     int type;
//     auto start = std::chrono::high_resolution_clock::now();
//     auto ret = llClient.ConcurrentRead(currIdx, data, type);
//     // auto ret = llClient.SpecReadEntry(currIdx, data);
//     if (ret > 0) {
//         auto duration = std::chrono::high_resolution_clock::now() - start;
//         auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
//         if (type == 2) {
//             readLat->push_back(static_cast<int>(nanoseconds.count()));
//         } else {
//             specReadLat->push_back(static_cast<int>(nanoseconds.count()));
//         }
//         currIdx++;
//         return type;
//         // assert(compare(data));
//     }
//     return ret;
// }

// int consumerLazylog::specConsume(std::vector<std::chrono::high_resolution_clock::time_point> *fetchTime) {
//     std::string data;
//     int type;
//     auto ret = llClient.ConcurrentRead(currIdx, data, type);
//     // auto ret = llClient.SpecReadEntry(currIdx, data);
//     if (ret > 0) {
//         fetchTime->push_back(std::chrono::high_resolution_clock::now());
//         currIdx++;
//         return type;
//         // assert(compare(data));
//     }
//     return ret;
// }

int consumerLazylog::consume() {
    std::string data;
    auto start = std::chrono::high_resolution_clock::now();
    if (llClient.ReadEntry(currIdx, data)) {
        if (!latencyLog.empty()) {
            auto duration = std::chrono::high_resolution_clock::now() - start;
            auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
            latency.push_back(static_cast<int>(nanoseconds.count()));
        }
        currIdx++;
        // return compare(data);
        return 1;
    }
    return -1;
}

int consumerLazylog::consume(std::vector<std::chrono::high_resolution_clock::time_point> *fetchTime) {
    std::string data;
    if (llClient.ReadEntry(currIdx, data)) {
        fetchTime->push_back(std::chrono::high_resolution_clock::now());
        currIdx++;
        // return compare(data);
        return 1;
    }
    return -1;
}

int consumerLazylog::consume(std::atomic<int> *tail, hdr_histogram *histogram, hdr_histogram *histogram2,
                             std::mutex *mtx) {
    std::string data;
    std::chrono::_V2::steady_clock::time_point time;
    if (currIdx >= tail->load()) return -1;
    {
        std::lock_guard<std::mutex> lock(*mtx);
        time = produce_time[currIdx];
    }

    while (std::chrono::steady_clock::now() < time + std::chrono::milliseconds(consumeDelay));

    {
        std::lock_guard<std::mutex> lock(*mtx);
        produce_time.erase(currIdx);
    }

    auto start = std::chrono::high_resolution_clock::now();
    if (llClient.ReadEntry(currIdx, data)) {
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
        hdr_record_value_atomic(histogram, nanoseconds.count());
        hdr_record_value_atomic(histogram2, nanoseconds.count());
        currIdx++;
        return 1;
    }
    return -1;
}

int consumerLazylog::consume(std::atomic<int> *tail, hdr_histogram *histogram, std::mutex *mtx) {
    std::string data;
    std::chrono::_V2::steady_clock::time_point time;
    if (currIdx >= tail->load()) return -1;
    {
        std::lock_guard<std::mutex> lock(*mtx);
        time = produce_time[currIdx];
    }

    while (std::chrono::steady_clock::now() < time + std::chrono::milliseconds(consumeDelay));

    {
        std::lock_guard<std::mutex> lock(*mtx);
        produce_time.erase(currIdx);
    }

    auto start = std::chrono::high_resolution_clock::now();
    if (llClient.ReadEntry(currIdx, data)) {
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
        hdr_record_value_atomic(histogram, nanoseconds.count());
        currIdx++;
        return 1;
    }
    return -1;
}

uint64_t consumerLazylog::getRemoteTail() {
    auto tail = std::get<0>(llClient.GetTail());
    return tail > 0 ? tail - 1 : 0;
}

std::tuple<uint64_t, uint64_t, uint64_t> consumerLazylog::getRemoteTailTuple() { return llClient.GetTail(); }

uint64_t consumerLazylog::getCurrIdx() { return currIdx; }
}  // namespace OpenMsgCpp