#include "ll_kv.h"

#include "glog/logging.h"
#include "signal.h"

void sigint_handler(int _signum) { lazylog::RPCTransport::Stop(); }

namespace lazylog {

absl::flat_hash_map<std::string, std::string> KVTransport::kv_store_ = {};
uint64_t KVTransport::next_idx_ = 0;
uint64_t KVTransport::total_append_time_ = 0;
uint64_t KVTransport::total_appned_ = 0;


void kv_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

void KVTransport::InitializeConn(const Properties &p, const std::string &svr, void *param) {
    LOG(ERROR) << "This is a server RPC transport";
    throw Exception("Wrong rpc transport type");
}

KVTransport::KVTransport() {}

KVTransport::~KVTransport() {}

void KVTransport::Initialize(const Properties &p) {
    const std::string server_uri = p.GetProperty(PROP_KV_SVR_URI, PROP_KV_SVR_URI_DEFAULT);
    LOG(INFO) << server_uri;

    nexus_ = new erpc::Nexus(server_uri, 0, 0);
    LOG(INFO) << "Nexus bind to " << server_uri;
    
    nexus_->register_req_func(KV_INSERT, InsertHandler);
    nexus_->register_req_func(KV_READ, ReadHandler);

    const int n_th = std::stoi(p.GetProperty("threadcount", "1"));

    for (int i = 0; i < n_th; i++) {
        server_thread_.emplace_back(std::move(std::thread(KVTransport::server_func, nexus_, i + 1, &p)));
    }

    if (p.GetProperty("reader", "false") == "true") {
        playlog_thread_ = std::thread(KVTransport::playlog_func, nexus_, &p);
        LOG(INFO) << "Playlog thread created";
    }
}

void KVTransport::Finalize() {
    if (playlog_thread_.joinable()) playlog_thread_.join();
    for (auto &t : server_thread_) t.join();

    LOG(INFO) << "average append time: " << static_cast<double>(total_append_time_) / total_appned_ << " us";
}

void KVTransport::server_func(erpc::Nexus *nexus, int th_id, const Properties *p) {
    const uint8_t phy_port = std::stoi(p->GetProperty("erpc.phy_port", "0"));

    LazyLogClient *ll_cli = new LazyLogClient;

    if (!rpc_) {
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, ll_cli, DL_CLI_RPCID_OFFSET + th_id, kv_sm_handler, phy_port);
        LOG(INFO) << "RPC object created on behalf of KV server";
    }
    rpc_use_cnt_.fetch_add(1);

    ll_cli->Initialize(*p);

    while (run_) {
        rpc_->run_event_loop(1000);
    }

    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
}

void KVTransport::playlog_func(erpc::Nexus *nexus, const Properties *p) {
    const uint8_t phy_port = std::stoi(p->GetProperty("erpc.phy_port", "0"));

    LazyLogClient *ll_cli = new LazyLogClient;
    ll_cli->Initialize(*p);

    while (run_) {
        uint64_t tail = std::get<1>(ll_cli->GetTail());
        for (auto i = next_idx_; i < tail; i++) {
            std::string data;
            ll_cli->ReadEntry(i, data);

            uint8_t op = *reinterpret_cast<uint8_t *>(data.data() + data.size() - 1);
            if (op == KV_INSERT) {
                std::string key = DeserializeKey(data.data());
                size_t offset = key.size() + sizeof(uint32_t);
                std::string value(data.substr(offset, data.size() - offset - 1));

                kv_store_[key] = value;
            } else {
                LOG(ERROR) << "Unknown operation" << op;
            }
        }
        LOG_IF(INFO, next_idx_ != tail) << "Playlog thread read " << tail - next_idx_ << " entries";
        next_idx_ = tail;
    }
}

void KVTransport::InsertHandler(erpc::ReqHandle *req_handle, void *context) {
    LazyLogClient *ll_cli = reinterpret_cast<LazyLogClient *>(context);
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    *reinterpret_cast<uint8_t*>(req->buf_ + req->get_data_size()) = KV_INSERT;

    auto data = std::string((char *)req->buf_, req->get_data_size() + sizeof(uint8_t));

    auto start = std::chrono::high_resolution_clock::now();
    ll_cli->AppendEntryAll(data);
    total_append_time_ += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start).count();
    total_appned_++;

    *reinterpret_cast<int *>(resp.buf_) = 0;
    rpc_->resize_msg_buffer(&resp, sizeof(int));
    rpc_->enqueue_response(req_handle, &resp);
}

void KVTransport::UpdateHandler(erpc::ReqHandle *req_handle, void *context) {
    InsertHandler(req_handle, context);
}

void KVTransport::ReadHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    std::string key = DeserializeKey((char *)(req->buf_));
    auto it = kv_store_.find(key);
    if (it != kv_store_.end()) {
        std::string &value = it->second;
        rpc_->resize_msg_buffer(&resp, value.size() + sizeof(int));
        *reinterpret_cast<int *>(resp.buf_) = 0;
        memcpy(resp.buf_ + sizeof(int), value.data(), value.size());
    } else {
        rpc_->resize_msg_buffer(&resp, sizeof(int));
        *reinterpret_cast<int *>(resp.buf_) = 2;
    }

    rpc_->enqueue_response(req_handle, &resp);
}

std::string KVTransport::DeserializeKey(const char *p) {
  uint32_t len = *reinterpret_cast<const uint32_t *>(p);
  std::string key(p + sizeof(uint32_t), len);
  return key;
}

}  // namespace lazylog

int main(int argc, const char *argv[]) {
    using namespace lazylog;

    signal(SIGINT, sigint_handler);

    Properties prop;
    ParseCommandLine(argc, argv, prop);

    KVTransport kv;
    kv.Initialize(prop);
    kv.Finalize();

    return 0;
}