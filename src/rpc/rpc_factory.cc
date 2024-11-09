#include "rpc_factory.h"

#include "glog/logging.h"

namespace lazylog {

const std::string RPCFactory::SVR_RPC_NAME_PROPERTY = "server.rpc_transport";
const std::string RPCFactory::SVR_RPC_NAME_PROPERTY_DEFAULT = "";
const std::string RPCFactory::CLI_RPC_NAME_PROPERTY = "client.rpc_transport";
const std::string RPCFactory::CLI_RPC_NAME_PROPERTY_DEFAULT = "";

std::map<std::string, RPCFactory::RPCCreator> &RPCFactory::Registry() {
    static std::map<std::string, RPCCreator> registry;
    return registry;
}

bool RPCFactory::RegisterRPC(const char *name, RPCCreator creator) {
    Registry()[name] = creator;
    LOG(INFO) << "Registered RPC: " << name;
    return true;
}

std::shared_ptr<RPCTransport> RPCFactory::CreateSvrRPCTransport(const Properties &p) {
    return CreateRPCTransport(p.GetProperty(SVR_RPC_NAME_PROPERTY, SVR_RPC_NAME_PROPERTY_DEFAULT));
}

std::shared_ptr<RPCTransport> RPCFactory::CreateCliRPCTransport(const Properties &p) {
    return CreateRPCTransport(p.GetProperty(CLI_RPC_NAME_PROPERTY, CLI_RPC_NAME_PROPERTY_DEFAULT));
}

std::shared_ptr<RPCTransport> RPCFactory::CreateRPCTransport(const std::string &rpc_name) {
    auto &registry = Registry();
    if (registry.find(rpc_name) == registry.end()) {
        LOG(WARNING) << "No RPC transport with name: " << rpc_name;
        return nullptr;
    }

    return (*registry[rpc_name])();
}

}  // namespace lazylog
