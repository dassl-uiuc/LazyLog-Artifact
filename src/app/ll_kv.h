#pragma once

#include "../client/lazylog_cli.h"
#include "../rpc/erpc_transport.h"
#include <absl/container/flat_hash_map.h>


namespace lazylog {

const std::string PROP_KV_SVR_URI = "kv.server_uri";
const std::string PROP_KV_SVR_URI_DEFAULT = "localhost:31860";

class KVTransport : public ERPCTransport {
   public:
    KVTransport();
    ~KVTransport();

    void Initialize(const Properties &p) override;
    void InitializeConn(const Properties &p, const std::string &svr, void *param) override;
    void Finalize() override;

   protected:
    std::vector<std::thread> server_thread_;
    std::thread playlog_thread_;

    static absl::flat_hash_map<std::string, std::string> kv_store_;
    static uint64_t next_idx_;

    static void server_func(erpc::Nexus *nexus, int th_id, const Properties *p);
    static void playlog_func(erpc::Nexus *nexus, const Properties *p);

    static void InsertHandler(erpc::ReqHandle *req_handle, void *context);
    static void UpdateHandler(erpc::ReqHandle *req_handle, void *context);
    static void ReadHandler(erpc::ReqHandle *req_handle, void *context);

    static std::string DeserializeKey(const char *p);

    static uint64_t total_append_time_;
    static uint64_t total_appned_;
};

}  // namespace lazylog