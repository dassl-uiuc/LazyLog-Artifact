#pragma once

#include <string>
#include <vector>
#include <chrono>
#include <algorithm>
#include <cstdio>
#include <mutex>
#include <ratio>
#include <thread>

#include <lazylog/producer_lazylog.hpp>

namespace OpenMsgCpp {
    void log_info(std::string msg);
    void log_warn(std::string msg);
    void log_error(std::string msg);
    std::vector<char> read_all_bytes(std::string filename, int size);
    void sleep_for_nanoseconds(std::chrono::nanoseconds ns);
    void sleep_for_nanoseconds_erpc(std::chrono::nanoseconds ns, producerLazylog *producer);
    void write_to_file(std::string filename, std::vector<int> array);
    void append_to_file(std::string filename, std::vector<int> array);
    std::vector<int> subtract(const std::vector<std::chrono::high_resolution_clock::time_point> &a, 
        const std::vector<std::chrono::high_resolution_clock::time_point> &b);

    class RateLimiter {
    public:
        RateLimiter(int64_t r, int64_t b) : r_(r * TOKEN_PRECISION), b_(b * TOKEN_PRECISION), tokens_(0), last_(Clock::now()) {}

        inline void Consume(int64_t n) {
            std::unique_lock<std::mutex> lock(mutex_);

            if (r_ <= 0) {
                return;
            }

            // refill tokens
            auto now = Clock::now();
            auto diff = std::chrono::duration_cast<Duration>(now - last_);
            tokens_ = std::min(b_, tokens_ + diff.count() * r_ / 1000000000);
            last_ = now;

            // check tokens
            tokens_ -= n * TOKEN_PRECISION;

            // sleep
            if (tokens_ < 0) {
                lock.unlock();
                int64_t wait_time = -tokens_ * 1000000000 / r_;
                std::this_thread::sleep_for(std::chrono::nanoseconds(wait_time));
            }
        }

        inline void SetRate(int64_t r) {
            std::lock_guard<std::mutex> lock(mutex_);

            // refill tokens
            auto now = Clock::now();
            auto diff = std::chrono::duration_cast<Duration>(now - last_);
            tokens_ = std::min(b_, tokens_ + diff.count() * r_ * TOKEN_PRECISION / 1000000000);
            last_ = now;

            // set rate
            r_ = r * TOKEN_PRECISION;
        }

    private:
        using Clock = std::chrono::steady_clock;
        using Duration = std::chrono::nanoseconds;
        static constexpr int64_t TOKEN_PRECISION = 10000;

        std::mutex mutex_;
        int64_t r_;
        int64_t b_;
        int64_t tokens_;
        Clock::time_point last_;
    };

    class RealtimeTput {
    public:
        RealtimeTput(int64_t interval); // tput report interval in ns
        void record(uint64_t num);
        // double recordReport(uint64_t num);
        void reset();
        void stop();

    private:
        void tputThreadFunc();

        bool stop_;
        std::thread t_;
        std::atomic<uint64_t> count_;
        std::chrono::high_resolution_clock::time_point startTime_;
        int64_t interval_ns_;
        std::chrono::nanoseconds interval_;
    };
}