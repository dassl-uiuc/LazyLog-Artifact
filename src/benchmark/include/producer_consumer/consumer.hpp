#pragma once

#include <string>

#include <workload.hpp>

namespace OpenMsgCpp {
    class consumer {
    public:
        consumer(){};
        consumer(workload load);
        virtual void run();
        virtual int consume();
        virtual int compare(std::string in);
        virtual void setLatLog(std::string input);

        workload load;
        std::vector<char> payload;
        std::vector<int> latency;
        std::string latencyLog;
        std::string message;
    };
}