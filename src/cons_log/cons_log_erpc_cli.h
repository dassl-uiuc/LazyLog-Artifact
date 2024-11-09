#pragma once

#include "cons_log_cli.h"
#include "glog/logging.h"

namespace lazylog {

class ConsensusLogERPCCli : public ConsensusLogCli {
    friend void rpc_cont_func_cons(void *ctx, void *tag);
   public:
    ConsensusLogERPCCli();
    ConsensusLogERPCCli(erpc::Nexus *nexus) { nexus_ = nexus; }  // this is used for initialize cons log cli within 

    void Initialize(const Properties &p) override {
        LOG(ERROR) << "This is a client RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void InitializeConn(const Properties &p, const std::string &svr_uri, void *param = nullptr) override;
    void Finalize() override;

    void ReadEntry(const uint64_t l) override;
    void ReadEntries(const uint64_t from, const uint64_t to) override;
    uint64_t GetNumOrderedEntries() override;

   protected:
    void pollForRpcComplete();
    void notifyRpcComplete();

   protected:
    int session_num_;
    static std::unordered_map<std::string, std::atomic<uint8_t> > local_rpc_cnt_;
    bool del_nexus_on_finalize_;

    erpc::MsgBuffer req_;
    erpc::MsgBuffer resp_;
    bool complete_;
};

}  // namespace lazylog
