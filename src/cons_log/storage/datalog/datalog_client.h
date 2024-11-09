#pragma once

#include <chrono>

#include "../../../rpc/erpc_transport.h"
#include "../../../rpc/log_entry.h"
#include "../../../rpc/rpc_token.h"
#include "glog/logging.h"

namespace lazylog {
class DataLogClient : public ERPCTransport {
    friend class LazyLogScalableClient;

   public:
    DataLogClient();

    void InitializeConn(const Properties &p, const std::string &svr, void *param) override;
    void Initialize(const Properties &p) override {
        LOG(ERROR) << "This is a client RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void Finalize() override;

    void AppendEntryShardAsync(const LogEntry &e, std::shared_ptr<RPCToken> &tkn);
    void ReplicateBatchAsync(const uint8_t *buf, size_t size, RPCToken &token);
    uint64_t SendReqIdGsnMappingAsync(const std::vector<LogEntry> &es, std::shared_ptr<RPCToken> &tkn);
    void UpdateGlobalIdxAsync(const uint64_t idx, RPCToken &tkn);
    bool ReadEntry(const uint64_t idx, LogEntry &e);
    void ReadEntriesAsync(const uint64_t start_idx, const uint64_t end_idx, std::shared_ptr<RPCToken> &tkn);
    bool GetReadResponses(std::vector<LogEntry> &entries);
    bool GetReadMetadata(const uint64_t start_idx, const uint64_t end_idx, std::vector<int> &shard_list);

   protected:
    bool getAppendStatus();

   protected:
    int session_num_;
    static std::unordered_map<std::string, std::atomic<uint8_t> > local_rpc_cnt_;
    bool del_nexus_on_finalize_;

    erpc::MsgBuffer req_;
    erpc::MsgBuffer resp_;
};

}  // namespace lazylog