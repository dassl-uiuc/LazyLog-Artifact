#pragma once

#include "../../rpc/erpc_transport.h"
#include "../../rpc/log_entry.h"
#include "../../rpc/rpc_token.h"
#include "glog/logging.h"

namespace lazylog {

class ShardClient : public ERPCTransport {
   public:
    ShardClient();
    ~ShardClient();

    void InitializeConn(const Properties &p, const std::string &svr, void *param) override;
    void Initialize(const Properties &p) override {
        LOG(ERROR) << "This is a client RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void Finalize() override;

    void AppendBatchAsync(const std::vector<LogEntry> &es, uint64_t from, uint32_t num, RPCToken &token);
    uint64_t AppendBatchAsync(const std::vector<LogEntry> &es, uint64_t from, uint64_t to, uint32_t itrv,
                              RPCToken &token);
    void UpdateGlobalIdxAsync(const uint64_t idx, RPCToken &tkn);
    bool ReadEntry(const uint64_t idx, LogEntry &e);
    void ReplicateBatchAsync(const uint8_t *buf, size_t size, RPCToken &token);

   protected:
    erpc::MsgBuffer req_;
    erpc::MsgBuffer resp_;

    bool del_nexus_on_finalize_;
    int session_num_;
};

}  // namespace lazylog
