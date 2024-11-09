#include "cons_log_erpc_cli.h"

#include "../rpc/rpc_factory.h"

namespace lazylog {

std::unordered_map<std::string, std::atomic<uint8_t> > ConsensusLogERPCCli::local_rpc_cnt_;

void cons_cli_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

void rpc_cont_func_cons(void *_ctx, void *tag) { reinterpret_cast<ConsensusLogERPCCli *>(tag)->notifyRpcComplete(); }

ConsensusLogERPCCli::ConsensusLogERPCCli() : del_nexus_on_finalize_(true) {}

void ConsensusLogERPCCli::InitializeConn(const Properties &p, const std::string &server_uri, void *param) {
    {
        std::lock_guard<std::mutex> lock(init_lk_);
        if (!nexus_) {
            const std::string client_uri = p.GetProperty(PROP_CL_CLI_URI, PROP_CL_CLI_URI_DEFAULT);
            nexus_ = new erpc::Nexus(client_uri);
        } else {
            del_nexus_on_finalize_ = false;  // nexus is not created here
        }
    }

    uint8_t local_rpc_id;
    if (!rpc_) {
        local_rpc_id = CL_CLI_RPCID_OFFSET + global_rpc_id_.fetch_add(1);  // use range [64, 127] for CL client rpc id
        const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, this, local_rpc_id, cons_cli_sm_handler, phy_port);

        LOG(INFO) << "RPC object created on behalf of CL client";
    }
    rpc_use_cnt_.fetch_add(1);

    if (local_rpc_cnt_.find(server_uri) == local_rpc_cnt_.end()) {
        local_rpc_cnt_[server_uri] = 0;
    }
    uint8_t remote_rpc_id = CL_SVR_RPCID_OFFSET + local_rpc_cnt_[server_uri].fetch_add(1);
    session_num_ = rpc_->create_session(server_uri, remote_rpc_id);

    while (!rpc_->is_connected(session_num_)) {
        LOG(INFO) << "Connecting to consensus log eRPC server...";
        rpc_->run_event_loop_once();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG(INFO) << "Connected to consensus log eRPC server at " << server_uri << "[" << (int)local_rpc_id << "->"
              << (int)remote_rpc_id << "]";

    const int msg_size = std::stoull(p.GetProperty(PROP_CL_MSG_SIZE, PROP_CL_MSG_SIZE_DEFAULT));
    req_ = rpc_->alloc_msg_buffer_or_die(msg_size);
    resp_ = rpc_->alloc_msg_buffer_or_die(msg_size);
}

void ConsensusLogERPCCli::Finalize() {
    rpc_->free_msg_buffer(resp_);
    rpc_->free_msg_buffer(req_);
    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
    if (del_nexus_on_finalize_ && global_rpc_id_.fetch_sub(1) == 1) {  // todo?
        delete nexus_;
        nexus_ = nullptr;
    }
}

void ConsensusLogERPCCli::ReadEntry(const uint64_t l) {}

void ConsensusLogERPCCli::ReadEntries(const uint64_t from, const uint64_t to) {}

uint64_t ConsensusLogERPCCli::GetNumOrderedEntries() { return 0; }

void ConsensusLogERPCCli::pollForRpcComplete() {
    while (!complete_) {
        RunERPCOnce();
    }
    complete_ = false;
}

void ConsensusLogERPCCli::notifyRpcComplete() { complete_ = true; }

const bool registered = RPCFactory::RegisterRPC("conslog_erpc_cli", []() {
    return std::dynamic_pointer_cast<RPCTransport>(std::make_shared<ConsensusLogERPCCli>());
});

}  // namespace lazylog
