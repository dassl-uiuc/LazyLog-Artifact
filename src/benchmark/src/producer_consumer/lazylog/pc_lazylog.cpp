#include <client/lazylog_cli.h>
#include <utils/properties.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <commons.hpp>
#include <lazylog/consumer_lazylog.hpp>
#include <lazylog/pc_lazylog.hpp>
#include <lazylog/producer_lazylog.hpp>
#include <string>
#include <thread>
#include <workload.hpp>

using namespace lazylog;

namespace OpenMsgCpp {

std::unordered_map<uint64_t, std::chrono::_V2::steady_clock::time_point> produce_time;

inline void pc_consumerThread(std::atomic<int> *tail, workload *load, std::vector<int> *tailLen,
                              std::vector<int> *latency, std::vector<int> *getTailLat, Properties *prop) {
    consumerLazylog consumer(*load, *prop);
    log_info("consumerThread runnning for " + load->getConfig("testDurationMinutes") + "m...");
    auto rate = std::stoi(load->getConfig("consumerRate"));
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    int perMsgDuration = 1000000000;
    int sleepTime = std::stoi(load->getConfig("consumerWaitNano"));
    auto sleep = std::chrono::nanoseconds(sleepTime);
    RateLimiter *limiter = new RateLimiter(rate, rate);
    // RealtimeTput *tputer = new RealtimeTput(5000000000);
    if (sleepTime > 0) {
        while (std::chrono::steady_clock::now() - startTime < duration) {
            int len = 0;
            auto startTail = std::chrono::high_resolution_clock::now();
            auto currTail = consumer.getRemoteTail();
            auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - startTail);
            getTailLat->push_back(static_cast<int>(nanoseconds.count()));
            // auto currTail = tail->load();
            while (consumer.getCurrIdx() < (int)currTail) {
                consumer.consume(tail, latency, nullptr);
                len++;
            }
            tailLen->push_back(len);
            sleep_for_nanoseconds(sleep);
        }
    } else if (rate > 0) {
        perMsgDuration /= rate;
        while (std::chrono::steady_clock::now() - startTime < duration) {
            limiter->Consume(1);
            auto startPer = std::chrono::high_resolution_clock::now();
            if (consumer.consume(tail, latency, nullptr)) {
                auto consumeTime = std::chrono::high_resolution_clock::now() - startPer;
                // if (consumeTime < std::chrono::nanoseconds(perMsgDuration)) {
                //     sleep_for_nanoseconds(std::chrono::nanoseconds(perMsgDuration) - consumeTime);
                // }
            }
            // tputer->record(1);
        }
    } else {
        while (std::chrono::steady_clock::now() - startTime < duration) {
            consumer.consume(tail, latency, nullptr);
            // tputer->record(1);
        }
    }

    delete limiter;
    // tputer->stop();
    // delete tputer;

    auto tput = ((float)consumer.getCurrIdx() - 1.0) / std::stof(load->getConfig("testDurationMinutes")) / 60.0;
    log_info("consume " + std::to_string(consumer.getCurrIdx() - 1) + " entries in " +
             load->getConfig("testDurationMinutes") + "m, " + std::to_string(tput) + " ops");
}

inline void pc_consumerThreadLag(std::atomic<int> *tail, workload *load, hdr_histogram *latency,
                                 std::vector<int> *tailLen, hdr_histogram *getTailLatency, Properties *prop,
                                 std::mutex *mtx) {
    consumerLazylog consumer(*load, *prop);
    log_info("consumerThread runnning for " + load->getConfig("testDurationMinutes") + "m...");
    uint64_t rate = std::stoull(load->getConfig("consumerRate"));
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    uint64_t sleepTime = std::stoull(load->getConfig("consumerSleepNano"));
    auto sleep = std::chrono::nanoseconds(sleepTime);
    if (sleepTime > 0) {
        while (std::chrono::steady_clock::now() - startTime < duration) {
            uint64_t len = 0;
            auto startTail = std::chrono::high_resolution_clock::now();
            auto currTail = consumer.getRemoteTail();
            auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now() - startTail);
            hdr_record_value_atomic(getTailLatency, nanoseconds.count());
            while (consumer.getCurrIdx() < currTail) {
                consumer.consume(tail, latency, mtx);
                len++;
            }
            tailLen->push_back(len);
            sleep_for_nanoseconds(sleep);
        }
    } else if (rate > 0) {
        // RateLimiter rlim(rate, rate);
        while (std::chrono::steady_clock::now() - startTime < duration) {
            // rlim.Consume(1);
            consumer.consume(tail, latency, getTailLatency, mtx);
        }
    } else {
        while (std::chrono::steady_clock::now() - startTime < duration) {
            consumer.consume(tail, latency, getTailLatency, mtx);
        }
    }

    auto tput = (static_cast<long double>(consumer.getCurrIdx()) - 1.0) /
                std::stold(load->getConfig("testDurationMinutes")) / 60.0;
    log_info("consume " + std::to_string(consumer.getCurrIdx() - 1) + " entries in " +
             load->getConfig("testDurationMinutes") + "m, " + std::to_string(tput) + " ops");
}

inline void pc_producerThreadLag(std::atomic<int> *tail, workload *load, Properties *prop, hdr_histogram *latency,
                                 hdr_histogram *overall_latency, std::mutex *mtx) {
    uint64_t localNum = 0;
    producerLazylog producer(*load, *prop);
    log_info("producerThread runnning for " + load->getConfig("testDurationMinutes") + "m...");
    auto rate = std::stoull(load->getConfig("producerRate"));
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    uint64_t perMsgDuration = 1000000000;
    RateLimiter *rateLimiter = nullptr;

    if (rate > 0) {
        rateLimiter = new RateLimiter(rate, rate);
    }
    while (std::chrono::steady_clock::now() - startTime < duration) {
        if (rate > 0) {
            rateLimiter->Consume(1);
        }
        auto startPer = std::chrono::high_resolution_clock::now();
        producer.produce(tail);
        auto timeProduced = std::chrono::high_resolution_clock::now();
        {
            std::lock_guard<std::mutex> lock(*mtx);
            produce_time.insert(std::make_pair(localNum, std::chrono::steady_clock::now()));
        }
        auto produceTime = timeProduced - startPer;
        hdr_record_value_atomic(latency, std::chrono::duration_cast<std::chrono::nanoseconds>(produceTime).count());
        hdr_record_value_atomic(overall_latency,
                                std::chrono::duration_cast<std::chrono::nanoseconds>(produceTime).count());
        localNum++;
    }

    auto tput = (static_cast<long double>(localNum) - 1.0) / std::stold(load->getConfig("testDurationMinutes")) / 60.0;
    log_info("produce " + std::to_string(localNum) + " entries in " + load->getConfig("testDurationMinutes") + "m, " +
             std::to_string(tput) + " ops");

    if (rate > 0) {
        delete rateLimiter;
    }
}

inline void pc_consumerFetchThread(std::atomic<int> *produceIndex, std::atomic<int> *fetchIndex, workload *load,
                                   Properties *prop, std::vector<int> *latency) {
    consumerLazylog consumer(*load, *prop);
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    log_info("consumerFetchThread runnning for " + load->getConfig("testDurationMinutes") + "...");

    while (std::chrono::steady_clock::now() - startTime < duration) {
        if (consumer.consume(produceIndex, latency, nullptr)) *fetchIndex = consumer.getCurrIdx();
    }

    log_info("consumerFetchThread finish with # entries: " + std::to_string(consumer.getCurrIdx()));
}

inline void pc_consumerProcessThread(std::atomic<int> *fetchIndex, workload *load, Properties *prop,
                                     std::vector<std::chrono::high_resolution_clock::time_point> *consumeTime) {
    consumerLazylog consumer(*load, *prop);
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    int sleepTime = std::stoi(load->getConfig("consumerSleepNano"));
    auto sleep = std::chrono::nanoseconds(sleepTime);
    uint64_t currIdx = 0;
    log_info("consumerProcessThread runnning for " + load->getConfig("testDurationMinutes") + "m, mimic process time " +
             load->getConfig("consumerSleepNano") + "...");
    int iter = 0;
    while (std::chrono::steady_clock::now() - startTime < duration) {
        auto oldIdx = currIdx;
        currIdx = fetchIndex->load();
        if (oldIdx >= currIdx) {
            sleep_for_nanoseconds(std::chrono::nanoseconds(100));
            continue;
        }

        sleep_for_nanoseconds(sleep);

        auto finishConsume = std::chrono::high_resolution_clock::now();
        if (consumeTime) {
            for (int tmp = oldIdx; tmp < currIdx; tmp++) {
                consumeTime->push_back(finishConsume);
            }
        }
        iter++;
    }

    auto tput = (float)currIdx / std::stof(load->getConfig("testDurationMinutes")) / 60.0;
    auto avgBatch = (float)currIdx / (float)iter;
    log_info("consume " + std::to_string(currIdx) + " entries in " + load->getConfig("testDurationMinutes") + "m, " +
             std::to_string(tput) + " ops");
    log_info("avg batch size: " + std::to_string(avgBatch));
}

inline void pc_consumerCheckThread(std::atomic<int> *fetchIndex, workload *load, Properties *prop,
                                   std::vector<int> *latency) {
    consumerLazylog consumer(*load, *prop);
    auto rate = std::stoi(load->getConfig("consumerRate"));
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    int perMsgDuration = 1000000000;
    int checkNum = 0;
    if (rate > 0) perMsgDuration /= rate;
    log_info("consumerCheckThread runnning for " + load->getConfig("testDurationMinutes") + "m...");

    while (std::chrono::steady_clock::now() - startTime < duration) {
        if (rate > 0) {
            auto startPer = std::chrono::high_resolution_clock::now();
            *fetchIndex = consumer.getRemoteTail();
            auto checkTail = std::chrono::high_resolution_clock::now() - startPer;
            latency->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(checkTail).count());
            if (checkTail < std::chrono::nanoseconds(perMsgDuration)) {
                sleep_for_nanoseconds(std::chrono::nanoseconds(perMsgDuration) - checkTail);
            }
        } else {
            *fetchIndex = consumer.getRemoteTail();
        }
        checkNum++;
    }

    auto tput = ((float)checkNum - 1.0) / std::stof(load->getConfig("testDurationMinutes")) / 60.0;
    log_info("consumerCheckThread finish with # entries: " + std::to_string(checkNum) +
             ", tput: " + std::to_string(tput) + " ops");
}

inline void pc_consumerBasicThread(std::atomic<int> *fetchIndex, workload *load, Properties *prop,
                                   std::vector<int> *latency) {
    consumerLazylog consumer(*load, *prop);
    consumer.setLatLog("a");
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    log_info("consumerBasicThread runnning for " + load->getConfig("testDurationMinutes") + "m...");

    while (std::chrono::steady_clock::now() - startTime < duration) {
        if (consumer.getCurrIdx() < fetchIndex->load()) consumer.consume();
    }

    auto tput = ((float)consumer.getCurrIdx() - 1.0) / std::stof(load->getConfig("testDurationMinutes")) / 60.0;
    log_info("consumerBasicThread finish with # entries: " + std::to_string(consumer.getCurrIdx()) +
             ", tput: " + std::to_string(tput) + " ops");

    for (auto it : consumer.latency) {
        latency->push_back(it);
    }
}

inline void pc_producerThread(std::atomic<int> *tail, workload *load, Properties *prop,
                              std::vector<std::chrono::high_resolution_clock::time_point> *produceTimePoint) {
    auto localNum = 0;
    producerLazylog producer(*load, *prop);
    log_info("producerThread runnning for " + load->getConfig("testDurationMinutes") + "m...");
    auto rate = std::stoi(load->getConfig("producerRate"));
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    int perMsgDuration = 1000000000;
    RateLimiter *limiter = new RateLimiter(rate, rate);
    while (std::chrono::steady_clock::now() - startTime < duration) {
        if (rate > 0) {
            limiter->Consume(1);
            producer.produce(tail, produceTimePoint);
        } else {
            producer.produce(tail, produceTimePoint);
        }
        localNum++;
    }

    delete limiter;
    auto tput = ((float)localNum - 1.0) / std::stof(load->getConfig("testDurationMinutes")) / 60.0;
    log_info("produce " + std::to_string(localNum) + " entries in " + load->getConfig("testDurationMinutes") + "m, " +
             std::to_string(tput) + " ops");
}

inline void pc_producerThreadOnly(std::atomic<int> *tail, workload *load, Properties *prop,
                                  std::vector<int> *produceTimeLen) {
    auto localNum = 0;
    producerLazylog producer(*load, *prop);
    log_info("producerThread runnning for " + load->getConfig("testDurationMinutes") + "m...");
    auto rate = std::stoi(load->getConfig("producerRate"));
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    int perMsgDuration = 1000000000;
    RateLimiter *limiter = new RateLimiter(rate, rate);
    while (std::chrono::steady_clock::now() - startTime < duration) {
        if (rate > 0) {
            limiter->Consume(1);
            auto startPer = std::chrono::high_resolution_clock::now();
            producer.produce(tail, nullptr);
            auto produceTime = std::chrono::high_resolution_clock::now() - startPer;
            produceTimeLen->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(produceTime).count());
        } else {
            auto startPer = std::chrono::high_resolution_clock::now();
            producer.produce(tail, nullptr);
            auto produceTime = std::chrono::high_resolution_clock::now() - startPer;
            produceTimeLen->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(produceTime).count());
        }
        localNum++;
    }

    auto tput = ((float)localNum - 1.0) / std::stof(load->getConfig("testDurationMinutes")) / 60.0;
    log_info("produce " + std::to_string(localNum) + " entries in " + load->getConfig("testDurationMinutes") + "m, " +
             std::to_string(tput) + " ops");
}

inline void pc_producerThreadSync(std::atomic<int> *tail, workload *load, Properties *prop,
                                  std::vector<int> *produceTimeLen) {
    auto localNum = 0;
    producerLazylog producer(*load, *prop);
    log_info("producerThreadSync runnning for " + load->getConfig("testDurationMinutes") + "m...");
    auto rate = std::stoi(load->getConfig("producerRate"));
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    // int perMsgDuration = 1000000000;
    // if (rate > 0) perMsgDuration /= rate;
    RateLimiter *limiter = new RateLimiter(rate, rate);

    while (std::chrono::steady_clock::now() - startTime < duration) {
        if (rate > 0) {
            limiter->Consume(1);
            auto startPer = std::chrono::high_resolution_clock::now();
            producer.produceSync(tail, nullptr);
            auto produceTime = std::chrono::high_resolution_clock::now() - startPer;
            produceTimeLen->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(produceTime).count());
        } else {
            auto startPer = std::chrono::high_resolution_clock::now();
            producer.produceSync(tail, nullptr);
            auto produceTime = std::chrono::high_resolution_clock::now() - startPer;
            produceTimeLen->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(produceTime).count());
        }
        localNum++;
    }

    delete limiter;

    auto tput = ((float)localNum - 1.0) / std::stof(load->getConfig("testDurationMinutes")) / 60.0;
    log_info("produce " + std::to_string(localNum) + " entries in " + load->getConfig("testDurationMinutes") + "m, " +
             std::to_string(tput) + " ops");
}

inline void pc_producerThreadSyncTput(std::atomic<int> *tail, workload *load, Properties *prop,
                                      std::vector<int> *produceTimeLen, RealtimeTput *tputer) {
    auto localNum = 0;
    producerLazylog producer(*load, *prop);
    log_info("producerThreadSync runnning for " + load->getConfig("testDurationMinutes") + "m...");
    auto rate = std::stoi(load->getConfig("producerRate"));
    auto duration = std::chrono::minutes(std::stoi(load->getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();
    // int perMsgDuration = 1000000000;
    // if (rate > 0) perMsgDuration /= rate;
    RateLimiter *limiter = new RateLimiter(rate, rate);

    while (std::chrono::steady_clock::now() - startTime < duration) {
        if (rate > 0) {
            limiter->Consume(1);
            auto startPer = std::chrono::high_resolution_clock::now();
            producer.produceSync(tail, nullptr);
            auto produceTime = std::chrono::high_resolution_clock::now() - startPer;
            produceTimeLen->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(produceTime).count());
        } else {
            auto startPer = std::chrono::high_resolution_clock::now();
            producer.produceSync(tail, nullptr);
            auto produceTime = std::chrono::high_resolution_clock::now() - startPer;
            produceTimeLen->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(produceTime).count());
        }
        localNum++;
        tputer->record(1);
    }

    delete limiter;

    auto tput = ((float)localNum - 1.0) / std::stof(load->getConfig("testDurationMinutes")) / 60.0;
    log_info("produce " + std::to_string(localNum) + " entries in " + load->getConfig("testDurationMinutes") + "m, " +
             std::to_string(tput) + " ops");
}

pcLazylog::pcLazylog(workload load, Properties prop)
    : prop(prop), load(load), tail(0), latLog(""), lenLog(""), offset(0) {
    log_info("pcLazylog constructor");
}

void pcLazylog::run(std::string mode) {
    auto numProducer = std::stoi(load.getConfig("producersPerTopic"));
    log_info("Spawning " + load.getConfig("producersPerTopic") + " producers...");
    std::atomic<int> fetchIdx;
    std::vector<int> tailLen, tailLat;
    std::vector<std::chrono::high_resolution_clock::time_point> produceTime, consumeTime;
    std::vector<std::vector<int> *> prod_lat;

    // for lag experiment
    hdr_histogram *producer_latency, *consumer_latency, *get_tail_latency;
    hdr_init(1, INT64_C(3600000000), 5, &producer_latency);
    hdr_init(1, INT64_C(3600000000), 5, &consumer_latency);
    hdr_init(1, INT64_C(3600000000), 5, &get_tail_latency);
    std::mutex mtx;

    RealtimeTput *tputer = new RealtimeTput(5000000000);
    if (mode != "s") {
        tputer->stop();
    }
    for (int i = 0; i < numProducer; i++) {
        if (mode == "r" || mode == "f") {
            prop.SetProperty("dur_log.client_id", std::to_string(i));
            log_info("producer client id" + std::to_string(i));
            threads.emplace_back(pc_producerThreadLag, &tail, &load, &prop, producer_latency, get_tail_latency, &mtx);
        } else if (mode == "p")
            threads.emplace_back(pc_producerThread, &tail, &load, &prop, &produceTime);
        else if (mode == "s") {
            auto a = new std::vector<int>();
            prod_lat.push_back(a);
            prop.SetProperty("dur_log.client_id", std::to_string(offset + i));
            // threads.emplace_back(pc_producerThreadSync, &tail, &load, &prop, prod_lat[i]);
            threads.emplace_back(pc_producerThreadSyncTput, &tail, &load, &prop, prod_lat[i], tputer);
        } else {
            auto a = new std::vector<int>();
            prod_lat.push_back(a);
            threads.emplace_back(pc_producerThreadOnly, &tail, &load, &prop, prod_lat[i]);
        }
    }
    log_info("All producer threads running..., starting consumer in " + load.getConfig("consumerWaitNano") + " ns");

    sleep_for_nanoseconds(std::chrono::nanoseconds(std::stoi(load.getConfig("consumerWaitNano"))));

    if (mode == "r") {
        pc_consumerThreadLag(&tail, &load, consumer_latency, &tailLen, get_tail_latency, &prop, &mtx);
    }

    for (std::thread &t : threads) {
        t.join();
    }

    log_info("mixed load finished running");

    auto tput = ((float)tail.load() - 1.0) / std::stof(load.getConfig("testDurationMinutes")) / 60.0;
    log_info("overall: produce " + std::to_string(tail.load()) + " entries in " +
             load.getConfig("testDurationMinutes") + "m, " + std::to_string(tput) + " ops");
    log_info("produceTime consumeTime: " + std::to_string(produceTime.size()) + ":" +
             std::to_string(consumeTime.size()));

    if (!latLog.empty()) {
        write_to_file(latLog, latency);
        log_info("Written latency to file " + latLog);
    }

    if (!lenLog.empty()) {
        write_to_file(lenLog, tailLen);
        log_info("Written tail length to file " + lenLog);
    }

    if (!tailLatLog.empty()) {
        write_to_file(tailLatLog, tailLat);
        log_info("Written get tail latency to file " + tailLatLog);
    }

    if (!e2eLatLog.empty()) {
        if (mode == "p") {
            auto e2elat = subtract(produceTime, consumeTime);
            write_to_file(e2eLatLog, e2elat);
        } else if (mode == "s") {
            for (int i = 0; i < prod_lat.size(); i++) {
                if (i == 0) {
                    write_to_file(e2eLatLog, *prod_lat[i]);
                } else {
                    append_to_file(e2eLatLog, *prod_lat[i]);
                }
                delete prod_lat[i];
            }
        }
        log_info("Written e2e latency to file " + e2eLatLog);
    }

    if (mode == "r") {
        std::cout << "avg producer latency (ns), avg consumer latency (ns)" << std::endl;
        std::cout << hdr_mean(producer_latency) << ", " << hdr_mean(consumer_latency) << std::endl;
    }
    hdr_close(producer_latency);
    hdr_close(consumer_latency);
    hdr_close(get_tail_latency);

    delete tputer;
}

void pcLazylog::setLatLog(std::string input) { latLog = input; }

void pcLazylog::setLenLog(std::string input) { lenLog = input; }

void pcLazylog::setTailLatLog(std::string input) { tailLatLog = input; }

void pcLazylog::setE2eLatLog(std::string input) { e2eLatLog = input; }

void pcLazylog::setOffset(int input) { offset = input; }
}  // namespace OpenMsgCpp