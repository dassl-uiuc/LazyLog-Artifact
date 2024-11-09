#include "dur_log_erpc_cli.h"

#include "../rpc/rpc_factory.h"

namespace lazylog {

std::unordered_map<std::string, std::atomic<uint8_t> > DurabilityLogERPCCli::local_rpc_cnt_;

void dur_cli_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

void rpc_cont_func(void *_ctx, void *tag) { reinterpret_cast<DurabilityLogERPCCli *>(tag)->notifyRpcComplete(); }

void rpc_cont_func_async(void *_ctx, void *tag) { reinterpret_cast<RPCToken *>(tag)->SetComplete(); }

DurabilityLogERPCCli::DurabilityLogERPCCli() : del_nexus_on_finalize_(true), is_primary_(false), complete_(false) {}

DurabilityLogERPCCli::~DurabilityLogERPCCli() {}

void DurabilityLogERPCCli::InitializeConn(const Properties &p, const std::string &server_uri, void *param) {
    is_primary_ = (server_uri == p.GetProperty(PROP_DL_PRI_URI, PROP_DL_PRI_URI_DEFAULT));

    {
        std::lock_guard<std::mutex> lock(init_lk_);
        if (!nexus_) {
            const std::string client_uri = p.GetProperty(PROP_DL_CLI_URI, PROP_DL_CLI_URI_DEFAULT);
            nexus_ = new erpc::Nexus(client_uri);
            LOG(INFO) << "Nexus bind to " << client_uri;
        } else {
            del_nexus_on_finalize_ = false;  // nexus is not created here
        }
    }

    uint8_t local_rpc_id;
    if (!rpc_) {
        if (!param)
            local_rpc_id = DL_CLI_RPCID_OFFSET + global_rpc_id_.fetch_add(1);  // use range [0, 63] for DL client rpc id
        else
            local_rpc_id = CL_RSV_RPCID;  // use 254 for client rpc id

        const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, nullptr, local_rpc_id, dur_cli_sm_handler, phy_port);
        LOG(INFO) << "RPC object created on behalf of DL cli";
    }
    rpc_use_cnt_.fetch_add(1);

    if (local_rpc_cnt_.find(server_uri) == local_rpc_cnt_.end()) {
        local_rpc_cnt_[server_uri] = 0;
    }
    uint8_t remote_rpc_id;
    uint64_t server_rpc_count = std::stoi(p.GetProperty("dur_log.threadcount", "1"));
    bool user_provided_id = p.ContainsKey("dur_log.client_id");
    if (!param)
        remote_rpc_id =
            DL_SVR_RPCID_OFFSET + (user_provided_id ? std::stoll(p.GetProperty("dur_log.client_id")) % server_rpc_count
                                                    : local_rpc_cnt_[server_uri].fetch_add(1) %
                                                          server_rpc_count);  // corresponding remote rpc id
    else
        remote_rpc_id = DL_RSV_RPCID;  // corresponding remote rpc id 191
    session_num_ = rpc_->create_session(server_uri, remote_rpc_id);

    while (!rpc_->is_connected(session_num_)) {
        LOG(INFO) << "Connecting to durability log eRPC server " << server_uri << "[" << (int)rpc_->get_rpc_id() << "->"
                  << (int)remote_rpc_id << "]";
        rpc_->run_event_loop_once();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (remote_rpc_id == DL_RSV_RPCID)
        LOG(INFO) << "Connected to exclusive DL eRPC server at " << server_uri << "[" << (int)rpc_->get_rpc_id() << "->"
                  << (int)remote_rpc_id << "]";
    else
        LOG(INFO) << "Connected to DL eRPC server at " << server_uri << "[" << (int)rpc_->get_rpc_id() << "->"
                  << (int)remote_rpc_id << "]";

    const int msg_size = std::stoull(p.GetProperty(PROP_DL_MSG_SIZE, PROP_DL_MSG_SIZE_DEFAULT));
    req_ = rpc_->alloc_msg_buffer_or_die(msg_size);
    resp_ = rpc_->alloc_msg_buffer_or_die(msg_size);
}

void DurabilityLogERPCCli::Finalize() {
    rpc_->free_msg_buffer(resp_);
    rpc_->free_msg_buffer(req_);
    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
    if (del_nexus_on_finalize_ && global_rpc_id_.fetch_sub(1) == 1) {
        delete nexus_;
        nexus_ = nullptr;
    }
}

bool DurabilityLogERPCCli::AppendEntry(const LogEntry &e) {
    size_t len = Serializer(e, req_.buf_);

    rpc_->resize_msg_buffer(&req_, len);
    rpc_->enqueue_request(session_num_, APPEND_ENTRY, &req_, &resp_, rpc_cont_func, this);

    pollForRpcComplete();

    if (resp_.get_data_size() < sizeof(uint64_t)) return false;

    return true;
}

bool DurabilityLogERPCCli::AppendEntryAsync(const LogEntry &e, std::shared_ptr<RPCToken> &token) {
    size_t len = Serializer(e, req_.buf_);

    rpc_->resize_msg_buffer(&req_, len);
    rpc_->enqueue_request(session_num_, APPEND_ENTRY, &req_, &resp_, rpc_cont_func_async, token.get());

    return true;
}

std::tuple<uint64_t, uint64_t, uint16_t> DurabilityLogERPCCli::GetNumDurEntry() {
    if (!IsPrimary()) LOG(WARNING) << "Not getting tail from a primary DL server";

    *reinterpret_cast<int *>(req_.buf_) = 0;
    rpc_->resize_msg_buffer(&req_, sizeof(int));

    rpc_->enqueue_request(session_num_, GET_N_DUR_ENTRY, &req_, &resp_, rpc_cont_func, this);

    pollForRpcComplete();

    uint64_t n_dur = *reinterpret_cast<uint64_t *>(resp_.buf_);
    uint64_t n_ordered = *reinterpret_cast<uint64_t *>(resp_.buf_ + sizeof(uint64_t));
    uint16_t view = *reinterpret_cast<uint16_t *>(resp_.buf_ + 2 * sizeof(uint64_t));
    return {n_dur, n_ordered, view};
}

uint32_t DurabilityLogERPCCli::FetchUnorderedEntries(std::vector<LogEntry> &e, uint32_t max_entries_num) {
    *reinterpret_cast<uint32_t *>(req_.buf_) = max_entries_num;
    rpc_->resize_msg_buffer(&req_, sizeof(max_entries_num));

    rpc_->enqueue_request(session_num_, FETCH_UNORDERED_ENTRIES, &req_, &resp_, rpc_cont_func,
                          this);  // todo: need a larger response buffer

    pollForRpcComplete();

    return MultiDeserializer(e, resp_.buf_);
}

uint32_t DurabilityLogERPCCli::FetchUnorderedEntries(std::vector<LogEntry> &e, uint64_t _from,
                                                     uint32_t max_entries_num) {
    return FetchUnorderedEntries(e, max_entries_num);
}

uint64_t DurabilityLogERPCCli::DeleteOrderedEntries(std::vector<LogEntry::ReqID> &req_ids) {
    size_t len = ReqIdSerializer(req_ids, req_.buf_);

    rpc_->resize_msg_buffer(&req_, len);
    rpc_->enqueue_request(session_num_, DEL_ORDERED_ENTRIES, &req_, &resp_, rpc_cont_func, this);

    pollForRpcComplete();

    if (resp_.get_data_size() < sizeof(uint64_t)) return 0;

    return *reinterpret_cast<uint64_t *>(resp_.buf_);
}

int DurabilityLogERPCCli::SpecRead(const uint64_t idx, LogEntry &e) {
    rpc_->resize_msg_buffer(&req_, sizeof(uint64_t));

    *reinterpret_cast<uint64_t*>(req_.buf_) = idx;

    rpc_->enqueue_request(session_num_, SPEC_READ, &req_, &resp_, rpc_cont_func, this);

    pollForRpcComplete();

    if (resp_.get_data_size() < MetaDataSize()) return *reinterpret_cast<int*>(resp_.buf_);

    return Deserializer(e, resp_.buf_);
}

void DurabilityLogERPCCli::DeleteOrderedEntriesAsync(std::vector<LogEntry::ReqID> &req_ids) {
    rpc_tkn_.Reset();
    size_t len = ReqIdSerializer(req_ids, req_.buf_);

    rpc_->resize_msg_buffer(&req_, len);  // todo: may need a larger req buffer
    rpc_->enqueue_request(session_num_, DEL_ORDERED_ENTRIES, &req_, &resp_, rpc_cont_func_async, &rpc_tkn_);
}

uint64_t DurabilityLogERPCCli::ProcessFetchedEntries(const std::vector<LogEntry> &es,
                                                 std::vector<LogEntry::ReqID> &req_ids) { return 0; }

bool DurabilityLogERPCCli::IsPrimary() { return is_primary_; }

bool DurabilityLogERPCCli::CheckAndRunOnce() {
    if (rpc_tkn_.Complete()) {
        return true;
    } else {
        RunERPCOnce();
        return rpc_tkn_.Complete();
    }
}

void DurabilityLogERPCCli::AddPendingReq(std::shared_ptr<RPCToken> &token) { pending_reqs_.push(token); }

void DurabilityLogERPCCli::CheckPendingReq() {
    if (pending_reqs_.empty()) return;
    RunERPCOnce();
    if (pending_reqs_.front()->Complete()) pending_reqs_.pop();
}

void DurabilityLogERPCCli::pollForRpcComplete() {
    while (!complete_) {
        RunERPCOnce();
    }
    complete_ = false;
}

void DurabilityLogERPCCli::notifyRpcComplete() { complete_ = true; }

#ifdef CORFU
uint64_t DurabilityLogERPCCli::getGSN() {
    // size_t len = Serializer(e, req_.buf_);

    rpc_->resize_msg_buffer(&req_, 0);
    rpc_->enqueue_request(session_num_, GET_GSN, &req_, &resp_, rpc_cont_func, this);

    pollForRpcComplete();
    if (resp_.get_data_size() < sizeof(uint64_t)) return UINT64_MAX;

    return *reinterpret_cast<uint64_t *>(resp_.buf_);
}

uint64_t DurabilityLogERPCCli::getGSNBatch(uint64_t batchSize) {
    rpc_->resize_msg_buffer(&req_, sizeof(uint64_t));

    *reinterpret_cast<uint64_t *>(req_.buf_) = batchSize;

    rpc_->enqueue_request(session_num_, GET_GSN_BATCH, &req_, &resp_, rpc_cont_func, this);

    pollForRpcComplete();

    if (resp_.get_data_size() < sizeof(uint64_t)) return UINT64_MAX;

    return *reinterpret_cast<uint64_t *>(resp_.buf_);
}
#endif

const bool registered = RPCFactory::RegisterRPC("durlog_erpc_cli", []() {
    return std::dynamic_pointer_cast<RPCTransport>(std::make_shared<DurabilityLogERPCCli>());
});

}  // namespace lazylog
