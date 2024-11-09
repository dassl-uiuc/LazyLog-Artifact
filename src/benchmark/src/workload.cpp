#include <filesystem>
#include <cassert>
#include <yaml-cpp/yaml.h>

#include <workload.hpp>
#include <commons.hpp>

namespace fs = std::filesystem;

bool fileExists(const std::string& filename) {
    return fs::exists(filename) && fs::is_regular_file(filename);
}

namespace OpenMsgCpp {
    workload::workload(const std::string& filename) {
        assert((fileExists(filename)));
        try {
            config = YAML::LoadFile(filename);
            log_info("Load workload from " + filename);
        } catch (const std::exception& e) {
            log_error("cannot read YAML file: " + std::string(e.what()));
        }
    }

    const std::string workload::getConfig(const std::string& key) {
        const std::string ret = config[key].as<std::string>();
        return ret;
    }
}