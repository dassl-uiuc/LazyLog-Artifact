#include "erpc_dl_transport.h"

#include "dur_log_flat.h"
#include "../rpc/common.h"
#include "../rpc/rpc_factory.h"

#include "glog/logging.h"

namespace lazylog {

DurabilityLog *ERPCDurLogTransport::dur_log_ = nullptr;

void svr_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

ERPCDurLogTransport::ERPCDurLogTransport() {
    if (!dur_log_) {
        dur_log_ = new DurabilityLogFlat();
    }
}

ERPCDurLogTransport::~ERPCDurLogTransport() {
    if (dur_log_)
        delete dur_log_;
}

void ERPCDurLogTransport::Initialize(const Properties &p) {
    const std::string server_uri = p.GetProperty(PROP_DL_SVR_URI, PROP_DL_SVR_URI_DEFAULT);
    nexus_ = new erpc::Nexus(server_uri, 0, 0);

    nexus_->register_req_func(APPEND_ENTRY, AppendEntryHandler);
    nexus_->register_req_func(ORDER_ENTRY, OrderEntryHandler);
    nexus_->register_req_func(GET_N_DUR_ENTRY, GetNumDurEntryHandler);
    nexus_->register_req_func(FETCH_UNORDERED_ENTRIES, FetchUnorderedEntriesHandler);
    nexus_->register_req_func(DEL_ORDERED_ENTRIES, DeleteOrderedEntriesHandler);
    nexus_->register_req_func(SPEC_READ, SpecReadHandler);
#ifdef CORFU
    nexus_->register_req_func(GET_GSN, GetGSNHandler);
    nexus_->register_req_func(GET_GSN_BATCH, GetGSNBatchHandler);
#endif

    const int n_th = std::stoi(p.GetProperty("threadcount", "1"));

    for (int i = 0; i < n_th; i++) {
        server_threads_.emplace_back(std::move(std::thread(ERPCDurLogTransport::server_func, nexus_, i, &p)));
    }

    server_threads_.emplace_back(std::move(std::thread(ERPCDurLogTransport::cl_server_func, &p)));

    dur_log_->Initialize(p, nexus_);
}

void ERPCDurLogTransport::Finalize() {
    for (auto &t : server_threads_) {
        t.join();
    }

    dur_log_->Finalize();

    if (nexus_) delete nexus_;
}

void ERPCDurLogTransport::server_func(erpc::Nexus *nexus, int th_id, const Properties *p) {
    const uint8_t phy_port = std::stoi(p->GetProperty("erpc.phy_port", "0"));
    if (!rpc_)
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus, nullptr, DL_SVR_RPCID_OFFSET + th_id,
                                               svr_sm_handler, phy_port);  // use range [128, 191] for server rpc id
    rpc_use_cnt_.fetch_add(1);

    const size_t msg_size = std::stoull(p->GetProperty(PROP_DL_MSG_SIZE, PROP_DL_MSG_SIZE_DEFAULT));
    rpc_->set_pre_resp_msgbuf_size(msg_size);

    LOG(INFO) << "Durability log ERPC DL server transport start, thread " << th_id;

    while (run_) {
        rpc_->run_event_loop(1000);
    }

    if (rpc_use_cnt_.fetch_sub(1) == 1)
        delete rpc_;
}

void ERPCDurLogTransport::AppendEntryHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    LogEntry e;
    Deserializer(e, req->buf_);
    e.flags = 0;
    uint64_t pri_seq = dur_log_->AppendEntry(e);

    rpc_->resize_msg_buffer(&resp, sizeof(pri_seq));
    *reinterpret_cast<uint64_t *>(resp.buf_) = pri_seq;

    rpc_->enqueue_response(req_handle, &resp);
}

void ERPCDurLogTransport::OrderEntryHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    LogEntry e;
    Deserializer(e, req->buf_);
    e.flags = 0;
    uint64_t seq = dur_log_->AppendEntry(e);

    rpc_->resize_msg_buffer(&resp, sizeof(seq));
    *reinterpret_cast<uint64_t *>(resp.buf_) = seq;

    rpc_->enqueue_response(req_handle, &resp);
}

void ERPCDurLogTransport::GetNumDurEntryHandler(erpc::ReqHandle *req_handle, void *context) {
    // only on primary
    auto &resp = req_handle->pre_resp_msgbuf_;

    *reinterpret_cast<uint64_t *>(resp.buf_) = dur_log_->GetNumDurEntry();
    *reinterpret_cast<uint64_t *>(resp.buf_ + sizeof(uint64_t)) = dur_log_->GetNumOrderedEntry();
    *reinterpret_cast<uint16_t *>(resp.buf_ + 2 * sizeof(uint64_t)) = dur_log_->GetView();

    rpc_->resize_msg_buffer(&resp, 2 * sizeof(uint64_t) + sizeof(uint16_t));
    rpc_->enqueue_response(req_handle, &resp);
}

void ERPCDurLogTransport::SpecReadHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    const uint64_t idx = *reinterpret_cast<uint64_t*>(req->buf_);

    LogEntry e;
    int ret_v;
    if ((ret_v = dur_log_->SpecReadEntry(idx, e)) <= 0) {
        rpc_->resize_msg_buffer(&resp, sizeof(int));
        *reinterpret_cast<int*>(resp.buf_) = ret_v;
    } else {
        rpc_->resize_msg_buffer(&resp, Serializer(e, resp.buf_));
    }

    rpc_->enqueue_response(req_handle, &resp);
}

#ifdef CORFU
void ERPCDurLogTransport::GetGSNHandler(erpc::ReqHandle *req_handle, void *context) {
    // only on primary
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    *reinterpret_cast<uint64_t *>(resp.buf_) = dur_log_->GetGSN();

    rpc_->resize_msg_buffer(&resp, sizeof(uint64_t));
    rpc_->enqueue_response(req_handle, &resp);
}

void ERPCDurLogTransport::GetGSNBatchHandler(erpc::ReqHandle *req_handle, void *context) {
    // only on primary
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;
    const uint64_t batchSize = *reinterpret_cast<uint64_t *>(req->buf_);

    *reinterpret_cast<uint64_t *>(resp.buf_) = dur_log_->GetGSN(batchSize);

    rpc_->resize_msg_buffer(&resp, sizeof(uint64_t));
    rpc_->enqueue_response(req_handle, &resp);
}
#endif

void ERPCDurLogTransport::cl_server_func(const Properties *p) {
    const uint8_t phy_port = std::stoi(p->GetProperty("erpc.phy_port", "0"));
    if (!rpc_)
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, nullptr, DL_RSV_RPCID, svr_sm_handler, phy_port);  // use 191 for reserved server rpc id
    rpc_use_cnt_.fetch_add(1);

    LOG(INFO) << "Durability log ERPC exclusive DL server transport start on reserved thread";

    while (run_)
        rpc_->run_event_loop(1000);
    
    if (rpc_use_cnt_.fetch_sub(1) == 1)
        delete rpc_;
}

void ERPCDurLogTransport::FetchUnorderedEntriesHandler(erpc::ReqHandle *req_handle, void *context) {
    // todo: only on primary
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->dyn_resp_msgbuf_;

    std::vector<LogEntry> unordered_entries;
    uint32_t max_entries_num = *reinterpret_cast<uint32_t*>(req->buf_);
    uint32_t num = dur_log_->FetchUnorderedEntries(unordered_entries, max_entries_num);

    resp = rpc_->alloc_msg_buffer(num * 2048);  // TODO: need a way to determine buf size
    size_t total_entries_size = MultiSerializer(unordered_entries, resp.buf_);
    rpc_->resize_msg_buffer(&resp, total_entries_size);

    rpc_->enqueue_response(req_handle, &resp);
}

void ERPCDurLogTransport::DeleteOrderedEntriesHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    std::vector<LogEntry::ReqID> req_ids;
    ReqIdDeserializer(req_ids, req->buf_);

    uint64_t n_del = dur_log_->DelOrderedEntries(req_ids);
    *reinterpret_cast<uint64_t *>(resp.buf_) = n_del;
    rpc_->resize_msg_buffer(&resp, sizeof(uint64_t));

    rpc_->enqueue_response(req_handle, &resp);
}

const bool registered = RPCFactory::RegisterRPC(
    "durlog_erpc_svr",
    []() { return std::dynamic_pointer_cast<RPCTransport>(std::make_shared<ERPCDurLogTransport>()); }
);

}  // namespace lazylog
