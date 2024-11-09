#include <cassert>
#include <chrono>
#include <commons.hpp>
#include <fstream>
#include <iostream>
#include <lazylog/producer_lazylog.hpp>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace OpenMsgCpp {
bool check_payload_size(std::vector<char> buf, int size) {
    if (buf.size() != size) {
        log_error("payload size mismatch: " + std::to_string(buf.size()) + " " + std::to_string(size));
        return false;
    }
    return true;
}

void log_info(std::string msg) {
    auto currentTime = std::chrono::system_clock::now();
    auto microseconds = std::chrono::time_point_cast<std::chrono::microseconds>(currentTime).time_since_epoch().count();
    std::cout << "[benchmark: " << microseconds << "][INFO]: " << msg << std::endl;
}

void log_warn(std::string msg) {
    auto currentTime = std::chrono::system_clock::now();
    auto microseconds = std::chrono::time_point_cast<std::chrono::microseconds>(currentTime).time_since_epoch().count();
    std::cerr << "[benchmark: " << microseconds << "][WARN]: " << msg << std::endl;
}

void log_error(std::string msg) {
    auto currentTime = std::chrono::system_clock::now();
    auto microseconds = std::chrono::time_point_cast<std::chrono::microseconds>(currentTime).time_since_epoch().count();
    std::cerr << "[benchmark: " << microseconds << "][ERROR]: " << msg << std::endl;
}

std::vector<char> read_all_bytes(std::string filename, int size) {
    std::ifstream file(filename);
    std::string hexString;
    if (!file.is_open()) {
        log_error("cannot open payload file " + filename);
        assert(file.is_open());
    }

    file >> hexString;
    log_info(hexString);
    std::vector<char> buffer(hexString.begin(), hexString.end());

    file.close();

    assert(check_payload_size(buffer, size));

    return buffer;
}

void sleep_for_nanoseconds(std::chrono::nanoseconds ns) {
    auto start = std::chrono::high_resolution_clock::now();
    auto end = start + ns;

    while (std::chrono::high_resolution_clock::now() < end) {
    }
}

void sleep_for_nanoseconds_erpc(std::chrono::nanoseconds ns, producerLazylog *producer) {
    auto start = std::chrono::high_resolution_clock::now();
    auto end = start + ns;

    while (std::chrono::high_resolution_clock::now() < end) {
        producer->rpcOnce();
    }
}

void write_to_file(std::string filename, std::vector<int> array) {
    if (array.size() <= 0) {
        log_warn(filename + " corresponding array empty");
        return;
    }
    std::ofstream outFile(filename);
    if (!outFile.is_open()) {
        log_error("Failed to open the file for writing " + filename);
        return;
    }

    for (int num : array) {
        outFile << num << std::endl;
    }

    outFile.close();
}

void append_to_file(std::string filename, std::vector<int> array) {
    if (array.size() <= 0) {
        log_warn(filename + " corresponding array empty");
        return;
    }
    std::ofstream outFile(filename, std::ios::app);
    if (!outFile.is_open()) {
        log_error("Failed to open the file for writing " + filename);
        return;
    }

    for (int num : array) {
        outFile << num << std::endl;
    }

    outFile.close();
}

std::vector<int> subtract(const std::vector<std::chrono::high_resolution_clock::time_point> &a,
                          const std::vector<std::chrono::high_resolution_clock::time_point> &b) {
    assert(a.size() >= b.size());
    std::vector<int> ret;
    for (int i = 0; i < b.size(); i++) {
        auto elapsed = b[i] - a[i];
        auto time = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
        assert(time >= 0);
        ret.push_back(time);
    }
    return ret;
}

RealtimeTput::RealtimeTput(int64_t interval) {
    interval_ns_ = interval;
    interval_ = std::chrono::nanoseconds(interval_ns_);
    stop_ = false;
    reset();
    t_ = std::thread(&RealtimeTput::tputThreadFunc, this);
}

void RealtimeTput::tputThreadFunc() {
    std::chrono::microseconds sleepDuration(interval_ns_ / 1000);
    while (!stop_) {
        std::this_thread::sleep_for(sleepDuration);
        auto elapsed = std::chrono::high_resolution_clock::now() - startTime_;
        auto elapsed_ns = (double)(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
        auto tput = (double)count_.load() * 1000000000.0 / elapsed_ns;
        log_info("tput: " + std::to_string(tput) + " op/s");
        reset();
    }
}

void RealtimeTput::record(uint64_t num) { count_.fetch_add(num); }

// double RealtimeTput::recordReport(uint64_t num) {
//     double ret = -1.0;
//     record(num);
//     std::lock_guard<std::mutex> lock(mtx_);
//     auto elapsed = std::chrono::high_resolution_clock::now() - startTime_;
//     if (elapsed >= interval_) {
//         auto elapsed_ns =
//             (double)(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count());
//         ret = (double)count_.load() * 1000000000.0 / elapsed_ns;
//         reset();
//     }
//     return ret;
// }

void RealtimeTput::reset() {
    count_ = 0;
    startTime_ = std::chrono::high_resolution_clock::now();
}

void RealtimeTput::stop() {
    stop_ = true;
    t_.join();
}
}  // namespace OpenMsgCpp