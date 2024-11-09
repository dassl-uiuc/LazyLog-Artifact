#pragma once

#include <rpc.h>

#include <unordered_map>
#include <atomic>
#include <mutex>

#include "transport.h"

namespace lazylog {

struct ServerContext {
    erpc::MsgBuffer resp_buf_;
    int session_num_;
    int thread_id_;
};

class ERPCTransport : public RPCTransport {
   public:
    static void RunERPCOnce();
   protected:
    static erpc::Nexus *nexus_;
    static std::mutex init_lk_;
    static std::atomic<uint8_t> global_rpc_id_;
    thread_local static erpc::Rpc<erpc::CTransport> *rpc_;
    thread_local static std::atomic<int> rpc_use_cnt_;
};

}  // namespace lazylog
