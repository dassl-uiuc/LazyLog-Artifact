#pragma once

#include <string>
#include <yaml-cpp/yaml.h>

namespace OpenMsgCpp {
    class workload {
    public:
        workload(){};
        workload(const std::string& filename);

        const std::string getConfig(const std::string& key);

    private:
        YAML::Node config;
    };
}