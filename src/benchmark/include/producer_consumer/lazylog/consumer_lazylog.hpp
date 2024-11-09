#pragma once

#include <client/lazylog_cli.h>
#include <hdr/hdr_histogram.h>
#include <utils/properties.h>

#include <atomic>
#include <chrono>
#include <consumer.hpp>
#include <string>
#include <workload.hpp>

using namespace lazylog;

namespace OpenMsgCpp {
class consumerLazylog : public consumer {
   public:
    consumerLazylog() {};
    consumerLazylog(workload load);
    consumerLazylog(workload load, Properties prop);
    void initClient(Properties prop);
    void run() override;
    int consume(std::atomic<int> &tail, std::vector<int> &readLat,
                std::vector<std::chrono::high_resolution_clock::time_point> &writeTime);
    int consume(std::atomic<int> *tail, std::vector<int> *readLat,
                std::vector<std::chrono::high_resolution_clock::time_point> *writeTime);
    // int specConsume(std::vector<int> *readLat, std::vector<int> *specReadLat);
    // int specConsume(std::vector<std::chrono::high_resolution_clock::time_point> *fetchTime);
    uint64_t getRemoteTail();
    std::tuple<uint64_t, uint64_t, uint64_t> getRemoteTailTuple();
    uint64_t getCurrIdx();
    int consume() override;
    int consume(std::vector<std::chrono::high_resolution_clock::time_point> *fetchTime);

    int consume(std::atomic<int> *tail, hdr_histogram *histogram, hdr_histogram *histogram2, std::mutex *mtx);
    int consume(std::atomic<int> *tail, hdr_histogram *histogram, std::mutex *mtx);

   private:
    LazyLogClient llClient;
    int restInterval;
    int batchSize;
    uint64_t currIdx;
    uint64_t consumeDelay;
};
}  // namespace OpenMsgCpp