#pragma once

#include <vector>

#include "../rpc/common.h"
#include "../rpc/erpc_transport.h"

namespace lazylog {

class ConsensusLogCli : public ERPCTransport {
   public:
    virtual uint64_t DispatchEntry(const LogEntry &e) = delete;
    virtual std::vector<uint64_t> DispatchEntries(const std::vector<LogEntry> &es) = delete;  // currently not used
    virtual void ReadEntry(const uint64_t l) = 0;
    virtual void ReadEntries(const uint64_t from, const uint64_t to) = 0;
    virtual uint64_t GetNumOrderedEntries() = 0;
};

}  // namespace lazylog
