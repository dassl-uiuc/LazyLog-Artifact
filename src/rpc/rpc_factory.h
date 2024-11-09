#pragma once

#include <memory>
#include <map>

#include "transport.h"
#include "../utils/properties.h"

namespace lazylog {

class RPCFactory {
   public:
    static const std::string SVR_RPC_NAME_PROPERTY;
    static const std::string SVR_RPC_NAME_PROPERTY_DEFAULT;
    static const std::string CLI_RPC_NAME_PROPERTY;
    static const std::string CLI_RPC_NAME_PROPERTY_DEFAULT;
    
    using RPCCreator = std::shared_ptr<RPCTransport> (*)();
    static bool RegisterRPC(const char *name, RPCCreator creator);
    static std::shared_ptr<RPCTransport> CreateSvrRPCTransport(const Properties &p);
    static std::shared_ptr<RPCTransport> CreateCliRPCTransport(const Properties &p);

private:
    static std::shared_ptr<RPCTransport> CreateRPCTransport(const std::string &rpc_name);
    static std::map<std::string, RPCCreator> &Registry();
};

}  // namespace lazylog
