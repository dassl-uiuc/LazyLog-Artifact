#include <client/lazylog_cli.h>
#include <hdr/hdr_histogram.h>
#include <utils/properties.h>

#include <atomic>
#include <chrono>
#include <consumer.hpp>
#include <lazylog/consumer_lazylog.hpp>
#include <lazylog/producer_lazylog.hpp>
#include <string>
#include <vector>
#include <workload.hpp>

using namespace lazylog;

namespace OpenMsgCpp {
class pcLazylog {
   public:
    pcLazylog(workload load, Properties prop);
    // void run();
    void run(std::string mode = "r");
    void setLatLog(std::string input);
    void setLenLog(std::string input);
    void setTailLatLog(std::string input);
    void setE2eLatLog(std::string input);
    void setOffset(int input);

   private:
    std::atomic<int> tail;
    std::string latLog;
    std::string lenLog;
    std::string tailLatLog;
    std::string e2eLatLog;
    workload load;
    Properties prop;
    std::vector<std::chrono::high_resolution_clock::time_point> ackTime;
    std::vector<int> latency;
    std::vector<std::thread> threads;
    int offset;
};
}  // namespace OpenMsgCpp