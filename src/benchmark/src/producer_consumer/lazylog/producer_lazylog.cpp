#include <client/lazylog_cli.h>
#include <utils/properties.h>

#include <commons.hpp>
#include <lazylog/producer_lazylog.hpp>
#include <string>

namespace OpenMsgCpp {
producerLazylog::producerLazylog(workload load, Properties prop) : producer(load) {
    llClient.Initialize(prop);
    numEntries = 0;
    log_info("LazyLog producer constructor");
}

producerLazylog::producerLazylog(workload load) : producer(load) {
    numEntries = 0;
    log_info("LazyLog producer constructor");
}

void producerLazylog::initClient(Properties prop) { llClient.Initialize(prop); }

int producerLazylog::produce(std::atomic<int> &tail,
                             std::vector<std::chrono::high_resolution_clock::time_point> &writeTime) {
    llClient.AppendEntryAll(message);
    writeTime.push_back(std::chrono::high_resolution_clock::now());
    tail++;
    return 1;
}

int producerLazylog::produce(std::atomic<int> *tail,
                             std::vector<std::chrono::high_resolution_clock::time_point> *writeTime) {
    llClient.AppendEntryAll(message);
    if (writeTime) writeTime->push_back(std::chrono::high_resolution_clock::now());
    (*tail)++;
    return 1;
}

int producerLazylog::produceSync(std::atomic<int> *tail,
                                 std::vector<std::chrono::high_resolution_clock::time_point> *writeTime) {
    llClient.AppendEntryAll(message);
    if (writeTime) writeTime->push_back(std::chrono::high_resolution_clock::now());
    (*tail)++;
    return 1;
}

int producerLazylog::produce() {
    auto start = std::chrono::high_resolution_clock::now();
    llClient.AppendEntryAll(message);
    if (!latencyLog.empty()) {
        auto duration = std::chrono::high_resolution_clock::now() - start;
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
        latency.push_back(static_cast<int>(nanoseconds.count()));
    }
    numEntries++;
    return 1;
}

int producerLazylog::produce(std::atomic<int> *tail) {
    llClient.AppendEntryAll(message);
    (*tail)++;
    return 1;
}

void producerLazylog::rpcOnce() { log_warn("not implemented"); }
}  // namespace OpenMsgCpp