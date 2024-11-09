#pragma once

#include <chrono>

namespace lazylog {

using namespace std::chrono;

class Timer {
   public:
    void Start();
    uint64_t End();

   protected:
    high_resolution_clock::time_point time_;
};

}  // namespace lazylog
