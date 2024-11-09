#include "erpc_transport.h"

namespace lazylog {

erpc::Nexus *ERPCTransport::nexus_ = nullptr;
std::mutex ERPCTransport::init_lk_;
std::atomic<uint8_t> ERPCTransport::global_rpc_id_(0);
thread_local erpc::Rpc<erpc::CTransport> *ERPCTransport::rpc_ = nullptr;
thread_local std::atomic<int> ERPCTransport::rpc_use_cnt_(0);

void ERPCTransport::RunERPCOnce() {
    rpc_->run_event_loop_once();
}

}  // namespace lazylog
