#include "erpc_cl_transport.h"

#include "../rpc/common.h"
#include "../rpc/rpc_factory.h"
#include "glog/logging.h"

namespace lazylog {

ConsensusLog *ERPCConsLogTransport::cons_log_ = nullptr;

void svr_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

ERPCConsLogTransport::ERPCConsLogTransport() : pipelined_(false) {
    if (!cons_log_) {
        cons_log_ = new ConsensusLog();
    }
}

ERPCConsLogTransport::~ERPCConsLogTransport() {
    if (cons_log_) delete cons_log_;
}

void ERPCConsLogTransport::Initialize(const Properties &p) {
    const std::string server_uri = p.GetProperty(PROP_CL_SVR_URI, PROP_CL_SVR_URI_DEFAULT);
    nexus_ = new erpc::Nexus(server_uri, 0, 0);

    // nexus_->register_req_func(DISPATCH_ENTRY, DispatchEntryHandler);
    // nexus_->register_req_func(DISPATCH_ENTRIES, DispatchEntriesHandler);  // currently not used
    nexus_->register_req_func(READ_ENTRY, ReadEntryHandler);
    nexus_->register_req_func(READ_ENTRIES, ReadEntriesHandler);
    nexus_->register_req_func(GET_N_ORD_ENTRY, GetNumOrderedEntriesHandler);

    const int n_th =
        0;  // std::stoi(p.GetProperty("threadcount", "1"));  // currently conslog is not serving any rpc requests

    for (int i = 0; i < n_th; i++) {
        server_threads_.emplace_back(std::move(std::thread(ERPCConsLogTransport::server_func, nexus_, i, &p)));
    }

    pipelined_ = (p.GetProperty("cons_log.pipeline", "false") == "true");

    cons_log_->Initialize(p, nexus_);

    if (pipelined_) {
        fetch_th_ = std::thread([this]() {
            LOG(INFO) << "Fetch thread started";
            while (run_) {
                cons_log_->fetch();
            }
            LOG(INFO) << "Fetch thread stopped";
        });
        LOG(INFO) << "Store thread started";
        while (run_) {
            cons_log_->store(
                run_);  // don't start a new threads to run because we need to use the rpc objects in this thread
        }
        LOG(INFO) << "Store thread stopped";
    } else {
        while (run_) {
            cons_log_->fetchAndStore();
        }
    }
}

void ERPCConsLogTransport::Finalize() {
    for (auto &t : server_threads_) {
        t.join();
    }
    fetch_th_.join();

    cons_log_->Finalize();

    if (nexus_) delete nexus_;
}

void ERPCConsLogTransport::server_func(erpc::Nexus *nexus, int th_id, const Properties *p) {
    ServerContext c;
    const uint8_t phy_port = std::stoi(p->GetProperty("erpc.phy_port", "0"));
    if (!rpc_)
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus, static_cast<void *>(&c), CL_SVR_RPCID_OFFSET + th_id,
                                               svr_sm_handler, phy_port);  // use range [192, 255] for CL server rpc id
    rpc_use_cnt_.fetch_add(1);
    c.thread_id_ = th_id;

    LOG(INFO) << "Consensus log ERPC server transport start, thread " << th_id;

    while (run_) {
        rpc_->run_event_loop(1000);
    }

    rpc_->free_msg_buffer(c.resp_buf_);

    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
}

void ERPCConsLogTransport::ReadEntryHandler(erpc::ReqHandle *req_handle, void *context) {}

void ERPCConsLogTransport::ReadEntriesHandler(erpc::ReqHandle *req_handle, void *context) {}

void ERPCConsLogTransport::GetNumOrderedEntriesHandler(erpc::ReqHandle *req_handle, void *context) {
    auto &resp = req_handle->pre_resp_msgbuf_;

    *reinterpret_cast<uint64_t *>(resp.buf_) = cons_log_->GetNumOrderedEntries();
    rpc_->resize_msg_buffer(&resp, sizeof(uint64_t));
    rpc_->enqueue_response(req_handle, &resp);
}

const bool registered = RPCFactory::RegisterRPC("conslog_erpc_svr", []() {
    return std::dynamic_pointer_cast<RPCTransport>(std::make_shared<ERPCConsLogTransport>());
});

}  // namespace lazylog
