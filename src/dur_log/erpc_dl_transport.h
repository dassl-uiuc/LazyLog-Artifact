#pragma once

#include <rpc.h>

#include <vector>

#include "../rpc/erpc_transport.h"
#include "dur_log.h"
#include "glog/logging.h"

namespace lazylog {

class ERPCDurLogTransport : public ERPCTransport {
   public:
    ERPCDurLogTransport();
    ~ERPCDurLogTransport();

    void Initialize(const Properties &p) override;
    void InitializeConn(const Properties &p, const std::string &svr, void *param) override {
        LOG(ERROR) << "This is a server RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void Finalize() override;

   protected:
    std::vector<std::thread> server_threads_;

    static DurabilityLog *dur_log_;
    static void server_func(erpc::Nexus *nexus, int th_id, const Properties *p);
    static void cl_server_func(const Properties *p);

    static void AppendEntryHandler(erpc::ReqHandle *req_handle, void *context);     // called from client
    static void OrderEntryHandler(erpc::ReqHandle *req_handle, void *context);      // called from client
    static void GetNumDurEntryHandler(erpc::ReqHandle *req_handle, void *context);  // called from client
    static void SpecReadHandler(erpc::ReqHandle *req_handle, void *context);        // called from client

    static void FetchUnorderedEntriesHandler(erpc::ReqHandle *req_handle, void *context);  // called from consensus log
    static void DeleteOrderedEntriesHandler(erpc::ReqHandle *req_handle, void *context);   // called from consensus log
};

}  // namespace lazylog
