#pragma once

#include <rpc.h>

#include <atomic>
#include <queue>

#include "dur_log_cli.h"
#include "glog/logging.h"

namespace lazylog {

class DurabilityLogERPCCli : public DurabilityLogCli {
    friend void rpc_cont_func(void *ctx, void *tag);
    friend void rpc_cont_func_async(void *ctx, void *tag);

   public:
    DurabilityLogERPCCli();
    virtual ~DurabilityLogERPCCli();

    void Initialize(const Properties &p) override {
        LOG(ERROR) << "This is a client RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void InitializeConn(const Properties &p, const std::string &svr, void *param) override;
    void Finalize() override;

    bool AppendEntry(const LogEntry &e) override;
    bool AppendEntryAsync(const LogEntry &e, std::shared_ptr<RPCToken> &token) override;
    std::tuple<uint64_t, uint64_t, uint16_t> GetNumDurEntry() override;
    uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint32_t max_entries_num) override;
    uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint64_t from, uint32_t max_entries_num) override;
    uint64_t DeleteOrderedEntries(std::vector<LogEntry::ReqID> &req_ids) override;
    int SpecRead(const uint64_t idx, LogEntry &e) override;
    void DeleteOrderedEntriesAsync(std::vector<LogEntry::ReqID> &req_ids) override;
    uint64_t ProcessFetchedEntries(const std::vector<LogEntry> &es, std::vector<LogEntry::ReqID> &req_ids) override;
    bool IsPrimary() override;
    bool CheckAndRunOnce() override;

#ifdef CORFU
    virtual uint64_t getGSN();
    virtual uint64_t getGSNBatch(uint64_t batchSize);
#endif

   public:
    void AddPendingReq(std::shared_ptr<RPCToken> &token) override;
    void CheckPendingReq() override;

   protected:
    void pollForRpcComplete();
    void notifyRpcComplete();

   protected:
    int session_num_;
    static std::unordered_map<std::string, std::atomic<uint8_t> > local_rpc_cnt_;
    bool del_nexus_on_finalize_;
    bool is_primary_;

    erpc::MsgBuffer req_;
    erpc::MsgBuffer resp_;

    bool complete_;
    std::queue<std::shared_ptr<RPCToken> > pending_reqs_;
    RPCToken rpc_tkn_;
};

}  // namespace lazylog
