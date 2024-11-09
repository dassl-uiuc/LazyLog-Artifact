#pragma once

#include <client/lazylog_cli.h>
#include <utils/properties.h>

#include <atomic>
#include <chrono>
#include <producer.hpp>
#include <string>
#include <workload.hpp>

using namespace lazylog;

namespace OpenMsgCpp {
class producerLazylog : public producer {
   public:
    producerLazylog(workload load, Properties prop);
    producerLazylog(workload load);
    void initClient(Properties prop);
    int produce(std::atomic<int> &tail, std::vector<std::chrono::high_resolution_clock::time_point> &writeTime);
    int produce(std::atomic<int> *tail, std::vector<std::chrono::high_resolution_clock::time_point> *writeTime);
    int produceSync(std::atomic<int> *tail, std::vector<std::chrono::high_resolution_clock::time_point> *writeTime);
    int produce() override;
    int produce(std::atomic<int> *tail);

    void rpcOnce();

    LazyLogClient llClient;
};
}  // namespace OpenMsgCpp