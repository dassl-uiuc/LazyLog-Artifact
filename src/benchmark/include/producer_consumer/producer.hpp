#pragma once

#include <vector>
#include <string>

#include <workload.hpp>

namespace OpenMsgCpp {
    class producer {
    public:
        producer(workload load);
        virtual void run();
        virtual int produce();
        virtual void setLatLog(std::string input);

        workload load;
        std::vector<char> payload;
        std::vector<int> latency;
        std::string latencyLog;
        std::string message;
        int msgSize;
        int numEntries;
    };
}