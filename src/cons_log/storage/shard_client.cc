#ifdef CORFU
#include <random>
#endif

#include "shard_client.h"

#include "../../rpc/common.h"

namespace lazylog {

void shd_cli_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void*) {}

void shd_rpc_cont_func_async(void* _ctx, void* tag) { reinterpret_cast<RPCToken*>(tag)->SetComplete(); }

#ifdef CORFU
ShardClient::ShardClient() : del_nexus_on_finalize_(true), remote_id_offset_(0) {}
#else
ShardClient::ShardClient() : del_nexus_on_finalize_(true) {}
#endif

ShardClient::~ShardClient() {}

#ifdef CORFU
inline std::mt19937 get_prng() {
    std::random_device r;
    std::seed_seq seed{r(), r(), r(), r(), r(), r(), r(), r()};
    return std::mt19937(seed);
}

void ShardClient::SetRemoteIdOffset(int input) {
    remote_id_offset_ = input;
}
#endif

void ShardClient::InitializeConn(const Properties& p, const std::string& svr_uri, void* param) {
#ifdef CORFU
    int rpc_id, remote_rpc_id;
    if (param != nullptr) {
        // reader thread init
        auto thread_id = (*static_cast<int*>(param));
        rpc_id = thread_id + CL_CLI_RPCID_OFFSET;
        auto remote_thread_count = std::stoi(p.GetProperty("shard.threadcount", "1"));
        auto gen = get_prng();
        std::uniform_int_distribution<int> distribution(0, remote_thread_count - 1);
        remote_rpc_id = distribution(gen) + CL_CLI_RPCID_OFFSET + remote_id_offset_;
    } else {
        rpc_id = 0;
        remote_rpc_id = 0;
    }
#else
    auto rpc_id = param == nullptr ? 0 : (*static_cast<int*>(param));
#endif

    {
        std::lock_guard<std::mutex> lock(init_lk_);
        if (!nexus_) {
            const std::string cli_uri = p.GetProperty(PROP_SHD_CLI_URI, PROP_SHD_CLI_URI_DEFAULT);
            nexus_ = new erpc::Nexus(cli_uri);
            LOG(INFO) << "Nexus bind to " << cli_uri;
        } else {
            del_nexus_on_finalize_ = false;
        }
    }

    if (!rpc_) {
        const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, nullptr, rpc_id, shd_cli_sm_handler,
                                               phy_port);  // todo: rpcid?
        LOG(INFO) << "RPC object created";
    }
    rpc_use_cnt_.fetch_add(1);

#ifdef CORFU
    session_num_ = rpc_->create_session(svr_uri, remote_rpc_id);
#else
    session_num_ = rpc_->create_session(svr_uri, rpc_id);
#endif

    while (!rpc_->is_connected(session_num_)) {
        LOG(INFO) << "Connecting to shard server...";
        rpc_->run_event_loop_once();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG(INFO) << "Connected to shard server at " << svr_uri;

    const int msg_size = std::stoull(p.GetProperty(PROP_SHD_MSG_SIZE, PROP_SHD_MSG_SIZE_DEFAULT));
    req_ = rpc_->alloc_msg_buffer_or_die(msg_size);
#ifdef CORFU
    resp_ = rpc_->alloc_msg_buffer_or_die(msg_size);
#else
    resp_ = rpc_->alloc_msg_buffer_or_die(PAGE_SIZE);
#endif
}

void ShardClient::Finalize() {
    rpc_->free_msg_buffer(resp_);
    rpc_->free_msg_buffer(req_);
    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
    if (del_nexus_on_finalize_) {
        delete nexus_;
        nexus_ = nullptr;
    }
}

void ShardClient::AppendBatchAsync(const std::vector<LogEntry>& es, uint64_t from, uint32_t num, RPCToken& token) {
    size_t len = MultiSerializer(es, from, num, req_.buf_);

    rpc_->resize_msg_buffer(&req_, len);
    rpc_->enqueue_request(session_num_, APPEND_BATCH, &req_, &resp_, shd_rpc_cont_func_async, &token);
}

uint64_t ShardClient::AppendBatchAsync(const std::vector<LogEntry>& es, uint64_t from, uint64_t to, uint32_t itrv,
                                       RPCToken& token) {
    uint64_t actual_end = to;
    size_t len = MultiSerializer(es, from, actual_end, itrv, req_.buf_);

    rpc_->resize_msg_buffer(&req_, len);
    rpc_->enqueue_request(session_num_, APPEND_BATCH, &req_, &resp_, shd_rpc_cont_func_async, &token);
    return actual_end;
}

void ShardClient::UpdateGlobalIdxAsync(const uint64_t idx, RPCToken& tkn) {
    *reinterpret_cast<uint64_t*>(req_.buf_) = idx;
    rpc_->resize_msg_buffer(&req_, sizeof(uint64_t));

    rpc_->enqueue_request(session_num_, UPDATE_GLBL_IDX, &req_, &resp_, shd_rpc_cont_func_async, &tkn);
}

#ifdef CORFU
bool ShardClient::AppendEntry(const LogEntry& e) {
    size_t len = Serializer(e, req_.buf_);
    rpc_->resize_msg_buffer(&req_, len);

    RPCToken tkn;
    rpc_->enqueue_request(session_num_, APPEND_ENTRY_BE, &req_, &resp_, shd_rpc_cont_func_async, &tkn);

    while (!tkn.Complete()) {
        RunERPCOnce();
    }

    if (resp_.get_data_size() <= sizeof(Status)) {
        return false;
    }
    return true;
}
#endif

bool ShardClient::ReadEntry(const uint64_t idx, LogEntry& e) {
    *reinterpret_cast<uint64_t*>(req_.buf_) = idx;
    rpc_->resize_msg_buffer(&req_, sizeof(uint64_t));

    RPCToken tkn;
    rpc_->enqueue_request(session_num_, READ_ENTRY_BE, &req_, &resp_, shd_rpc_cont_func_async, &tkn);

    while (!tkn.Complete()) {
        RunERPCOnce();
    }

    if (resp_.get_data_size() <= sizeof(Status)) {
        return false;
    } else {
        Deserializer(e, resp_.buf_);
        return true;
    }
}

void ShardClient::ReplicateBatchAsync(const uint8_t* buf, size_t size, RPCToken& token) {
    memcpy(req_.buf_, buf, size);

    rpc_->resize_msg_buffer(&req_, size);
    rpc_->enqueue_request(session_num_, REP_BATCH, &req_, &resp_, shd_rpc_cont_func_async, &token);
}

}  // namespace lazylog
