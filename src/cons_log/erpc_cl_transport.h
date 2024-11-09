#pragma once

#include <rpc.h>

#include <vector>

#include "../rpc/erpc_transport.h"
#include "cons_log.h"

namespace lazylog {

class ERPCConsLogTransport : public ERPCTransport {
   public:
    ERPCConsLogTransport();
    ~ERPCConsLogTransport();

    void Initialize(const Properties &p) override;
    void InitializeConn(const Properties &p, const std::string &svr, void *param) override {
        throw Exception("Wrong rpc transport type");
    }
    void Finalize() override;

   protected:
    std::vector<std::thread> server_threads_;

    bool pipelined_;

    std::thread fetch_th_;

    static ConsensusLog *cons_log_;
    static void server_func(erpc::Nexus *nexus, int th_id, const Properties *p);

    static void DispatchEntryHandler(erpc::ReqHandle *req_handle, void *context) = delete;
    static void DispatchEntriesHandler(erpc::ReqHandle *req_handle, void *context) = delete;  // currently not used
    static void ReadEntryHandler(erpc::ReqHandle *req_handle, void *context);
    static void ReadEntriesHandler(erpc::ReqHandle *req_handle, void *context);

    static void GetNumOrderedEntriesHandler(erpc::ReqHandle *req_handle, void *context);

};


}  // namespace lazylog
