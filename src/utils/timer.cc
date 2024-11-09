#include "timer.h"

namespace lazylog {

void Timer::Start() { time_ = high_resolution_clock::now(); }

uint64_t Timer::End() { return duration_cast<microseconds>(high_resolution_clock::now() - time_).count(); }

}  // namespace lazylog
