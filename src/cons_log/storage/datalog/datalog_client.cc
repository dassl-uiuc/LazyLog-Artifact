#include "datalog_client.h"

#include "../../../rpc/common.h"

namespace lazylog {

std::unordered_map<std::string, std::atomic<uint8_t> > DataLogClient::local_rpc_cnt_ = {};

void datalog_cli_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void*) {}

void datalog_rpc_cont_func_async(void* _ctx, void* tag) { reinterpret_cast<RPCToken*>(tag)->SetComplete(); }

DataLogClient::DataLogClient() : del_nexus_on_finalize_(true) {}

void DataLogClient::InitializeConn(const Properties& p, const std::string& svr, void* param) {
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

    // called from clients
    uint8_t local_rpc_id = param == nullptr ? global_rpc_id_.fetch_add(1) + SHD_CLI_RPCID_OFFSET : 0;
    if (!rpc_) {
        const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, nullptr, local_rpc_id, datalog_cli_sm_handler, phy_port);
        LOG(INFO) << "RPC object created";
    }
    rpc_use_cnt_.fetch_add(1);

    if (local_rpc_cnt_.find(svr) == local_rpc_cnt_.end()) {
        local_rpc_cnt_[svr] = 0;
    }
    uint64_t shard_thread_count = std::stoi(p.GetProperty("shard.threadcount", "1"));
    uint8_t remote_rpc_id =
        param == nullptr ? local_rpc_cnt_[svr].fetch_add(1) % shard_thread_count + SHD_SVR_RPCID_OFFSET : 0;
    if (param == nullptr) {
        bool user_provided_id = p.ContainsKey("dur_log.client_id");
        uint64_t client_id = std::stoll(p.GetProperty("dur_log.client_id"));
        remote_rpc_id = SHD_SVR_RPCID_OFFSET + client_id % shard_thread_count;
    }

    session_num_ = rpc_->create_session(svr, remote_rpc_id);
    while (!rpc_->is_connected(session_num_)) {
        LOG(INFO) << "Connecting to Datalog eRPC server " << svr << "[" << (int)rpc_->get_rpc_id() << "->"
                  << (int)remote_rpc_id << "]";
        RunERPCOnce();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG(INFO) << "Connected to Datalog eRPC server at " << svr << "[" << (int)rpc_->get_rpc_id() << "->"
              << (int)remote_rpc_id << "]";

    const int msg_size = std::stoull(p.GetProperty(PROP_SHD_MSG_SIZE, PROP_SHD_MSG_SIZE_DEFAULT));
    req_ = rpc_->alloc_msg_buffer_or_die(msg_size);
    resp_ = rpc_->alloc_msg_buffer_or_die(msg_size);
}

void DataLogClient::Finalize() {
    rpc_->free_msg_buffer(resp_);
    rpc_->free_msg_buffer(req_);
    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
    if (del_nexus_on_finalize_ && global_rpc_id_.fetch_sub(1) == 1) {
        delete nexus_;
        nexus_ = nullptr;
    }
}

void DataLogClient::ReplicateBatchAsync(const uint8_t* buf, size_t size, RPCToken& token) {
    memcpy(req_.buf_, buf, size);

    rpc_->resize_msg_buffer(&req_, size);
    rpc_->enqueue_request(session_num_, REP_BATCH, &req_, &resp_, datalog_rpc_cont_func_async, &token);
}

void DataLogClient::AppendEntryShardAsync(const LogEntry& e, std::shared_ptr<RPCToken>& tkn) {
    rpc_->resize_msg_buffer(&req_, Serializer(e, req_.buf_));

    rpc_->enqueue_request(session_num_, APPEND_ENTRY_SHD, &req_, &resp_, datalog_rpc_cont_func_async, tkn.get());
}

void DataLogClient::UpdateGlobalIdxAsync(const uint64_t idx, RPCToken& tkn) {
    *reinterpret_cast<uint64_t*>(req_.buf_) = idx;
    rpc_->resize_msg_buffer(&req_, sizeof(uint64_t));
    rpc_->enqueue_request(session_num_, UPDATE_GLBL_IDX, &req_, &resp_, datalog_rpc_cont_func_async, &tkn);
}

bool DataLogClient::ReadEntry(const uint64_t idx, LogEntry& e) {
    *reinterpret_cast<uint64_t*>(req_.buf_) = idx;
    rpc_->resize_msg_buffer(&req_, sizeof(uint64_t));

    RPCToken tkn;
    rpc_->enqueue_request(session_num_, READ_ENTRY_BE, &req_, &resp_, datalog_rpc_cont_func_async, &tkn);

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

void DataLogClient::ReadEntriesAsync(const uint64_t start_idx, const uint64_t end_idx, std::shared_ptr<RPCToken>& tkn) {
    *reinterpret_cast<uint64_t*>(req_.buf_) = start_idx;
    *reinterpret_cast<uint64_t*>(req_.buf_ + sizeof(uint64_t)) = end_idx;
    rpc_->resize_msg_buffer(&req_, 2 * sizeof(uint64_t));

    rpc_->enqueue_request(session_num_, READ_BATCH, &req_, &resp_, datalog_rpc_cont_func_async, tkn.get());
}

bool DataLogClient::GetReadResponses(std::vector<LogEntry>& entries) {
    if (resp_.get_data_size() <= sizeof(Status)) {
        return false;
    } else {
        MultiDeserializer(entries, resp_.buf_);
        return true;
    }
}

bool DataLogClient::GetReadMetadata(const uint64_t start_idx, const uint64_t end_idx, std::vector<int>& shard_list) {
    *reinterpret_cast<uint64_t*>(req_.buf_) = start_idx;
    *reinterpret_cast<uint64_t*>(req_.buf_ + sizeof(uint64_t)) = end_idx;
    rpc_->resize_msg_buffer(&req_, 2 * sizeof(uint64_t));

    RPCToken tkn;
    rpc_->enqueue_request(session_num_, GET_READ_METADATA, &req_, &resp_, datalog_rpc_cont_func_async, &tkn);

    auto start = std::chrono::high_resolution_clock::now();
    while (!tkn.Complete()) {
        RunERPCOnce();
    }

    if (resp_.get_data_size() <= sizeof(Status)) {
        return false;
    } else {
        MultiIntDeserializer(shard_list, resp_.buf_);
        return true;
    }
}

uint64_t DataLogClient::SendReqIdGsnMappingAsync(const std::vector<LogEntry>& es, std::shared_ptr<RPCToken>& tkn) {
    uint32_t num = es.size();
    uint64_t end_idx = es.back().log_idx;
    if (num > 1 && es.back().client_id == 0) {
        num = num - 1;
        end_idx = (es.end() - 2)->log_idx;
    }
    rpc_->resize_msg_buffer(&req_, MultiSerializer(es, 0, num, req_.buf_));

    rpc_->enqueue_request(session_num_, ORDER_BATCH, &req_, &resp_, datalog_rpc_cont_func_async, tkn.get());

    return end_idx;
}

bool DataLogClient::getAppendStatus() { return (*reinterpret_cast<Status*>(resp_.buf_) == Status::OK); }

}  // namespace lazylog
