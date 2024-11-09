#include <chrono>
#include <thread>
#include <string>

#include <consumer.hpp>
#include <workload.hpp>
#include <commons.hpp>

namespace OpenMsgCpp {
    consumer::consumer(workload load) : load{load}, latencyLog{""} {
        auto msgSize = std::stoi(load.getConfig("messageSize"));
        payload = read_all_bytes(load.getConfig("payloadFile"), msgSize);
        // payload.push_back('\0');
        message.assign(payload.begin(), payload.end());
    }

    void consumer::setLatLog(std::string input) {
        latencyLog = input;
    }

    void consumer::run() {
        log_info("Runnig consumer");

        auto duration = std::chrono::minutes(std::stoi(load.getConfig("testDurationMinutes")));
        auto startTime = std::chrono::steady_clock::now();

        while (std::chrono::steady_clock::now() - startTime < duration) {
            consume();
        }
        if (!latencyLog.empty()) {
            write_to_file(latencyLog, latency);
            log_info("Written latency to file " + latencyLog);
        }
    }

    int consumer::compare(std::string in) {
        return in == message;
    }

    int consumer::consume() {
        sleep_for_nanoseconds(std::chrono::nanoseconds(50));
        return 1;
    }
}