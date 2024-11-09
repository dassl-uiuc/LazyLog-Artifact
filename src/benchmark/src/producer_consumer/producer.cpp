#include <chrono>
#include <commons.hpp>
#include <producer.hpp>
#include <string>
#include <thread>
#include <vector>
#include <workload.hpp>

namespace OpenMsgCpp {
producer::producer(workload load) : load{load}, latencyLog{""} {
    msgSize = std::stoi(load.getConfig("messageSize"));
    payload = read_all_bytes(load.getConfig("payloadFile"), msgSize);
    // payload.push_back('\0');
    message.assign(payload.begin(), payload.end());
    log_info("payload: " + message);
}

void producer::setLatLog(std::string input) { latencyLog = input; }

void producer::run() {
    auto msg = "Runnig producer with config: messageSize: " + load.getConfig("messageSize") +
               ", producerRate: " + load.getConfig("producerRate") + " for " + load.getConfig("testDurationMinutes") +
               "m...";
    log_info(msg);
    auto rate = std::stoi(load.getConfig("producerRate"));
    auto duration = std::chrono::minutes(std::stoi(load.getConfig("testDurationMinutes")));
    auto startTime = std::chrono::steady_clock::now();

    while (std::chrono::steady_clock::now() - startTime < duration) {
        if (rate > 0) {
            log_info("Producing...");
            auto startLoop = std::chrono::steady_clock::now();
            for (int i = 0; i < rate; i++) {
                produce();
            }
            auto produceTime = std::chrono::steady_clock::now() - startLoop;
            if (produceTime < std::chrono::seconds(1)) {
                sleep_for_nanoseconds(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::seconds(1) - produceTime));
            } else {
                log_warn("producerRate not met: " +
                         std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(produceTime).count()));
            }
        } else {
            produce();
        }
    }
    auto tput = ((float)numEntries - 1.0) / std::stof(load.getConfig("testDurationMinutes")) / 60.0;
    log_info("Producer finish with " + std::to_string(numEntries) + " entries appended, tput: " + std::to_string(tput) +
             "entries/s");
    if (!latencyLog.empty()) {
        write_to_file(latencyLog, latency);
        log_info("Written latency to file " + latencyLog);
    }
}

int producer::produce() {
    sleep_for_nanoseconds(std::chrono::nanoseconds(50));
    numEntries++;
    return 1;
}
}  // namespace OpenMsgCpp