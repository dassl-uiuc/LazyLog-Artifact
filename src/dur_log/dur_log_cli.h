#pragma once

#include "../rpc/common.h"
#include "../rpc/erpc_transport.h"
#include "../rpc/rpc_token.h"

namespace lazylog {

class DurabilityLogCli : public ERPCTransport {
   public:
    virtual bool AppendEntry(const LogEntry &e) = 0;
    virtual bool AppendEntryAsync(const LogEntry &e, std::shared_ptr<RPCToken> &token) = 0;
    virtual uint64_t OrderEntry(const LogEntry &e) = delete;  // temporarily deprecated, not used for now
    virtual std::tuple<uint64_t, uint64_t, uint16_t> GetNumDurEntry() = 0;
    virtual uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint32_t max_entries_num) = 0;
    virtual uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint64_t from, uint32_t max_entries_num) = 0;
    virtual bool SetMaxOrderedSeq(uint64_t seq) = delete;  // deprecated, this is done alone with deleting ordered entries
    virtual int SpecRead(const uint64_t idx, LogEntry &e) = 0;
    virtual uint64_t DeleteOrderedEntries(std::vector<LogEntry::ReqID> &req_ids) = 0;
    virtual void DeleteOrderedEntriesAsync(std::vector<LogEntry::ReqID> &req_ids) = 0;
    /// @return the log idx of the last entry
    virtual uint64_t ProcessFetchedEntries(const std::vector<LogEntry> &es, std::vector<LogEntry::ReqID> &req_ids) = 0;
    virtual bool IsPrimary() = 0;
    virtual bool CheckAndRunOnce() = 0;

   public:
    virtual void AddPendingReq(std::shared_ptr<RPCToken> &token) = 0;
    virtual void CheckPendingReq() = 0;
};

}  // namespace lazylog
