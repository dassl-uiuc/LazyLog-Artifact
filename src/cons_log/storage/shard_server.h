#pragma once

#include <dirent.h>

#include <condition_variable>
#include <map>
#include <shared_mutex>
#include <thread>

#include "../../rpc/erpc_transport.h"
#include "glog/logging.h"
#include "shard_client.h"

namespace lazylog {

class ShardServerMetrics {
   public:
    std::atomic<uint64_t> num_slow_path_reads;
    std::atomic<uint64_t> num_fast_path_reads;

    friend std::ostream &operator<<(std::ostream &out, const ShardServerMetrics &B);
};

class ShardServer : public ERPCTransport {
   public:
    ShardServer();
    ~ShardServer();

    void InitializeConn(const Properties &p, const std::string &svr, void *param) override {
        LOG(ERROR) << "This is a server RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void Initialize(const Properties &p) override;
    void Finalize() override;

   protected:
    static void AppendBatchHandler(erpc::ReqHandle *req_handle, void *context);      // called from middle man
    static void ReplicateBatchHandler(erpc::ReqHandle *req_handle, void *context);   // called from shard primary
    static void ReadEntryHandler(erpc::ReqHandle *req_handle, void *context);        // called from middle man
    static void UpdateGlobalIdxHandler(erpc::ReqHandle *req_handle, void *context);  // called from middle man

    static void addToEntryCache(uint64_t base_idx, const uint8_t *buf);
    static std::string getDataFilePath(uint64_t base_idx);
    static int writeFromCacheToDisk(uint64_t base_idx);
    static int loadFromDiskToCache(uint64_t base_idx);
    static bool allRPCCompleted(std::vector<RPCToken> &tokens);
    static bool processExistingDataFiles();
    static void backgroundFsync();

#ifdef CORFU
    static void ReadBatchHandler(erpc::ReqHandle *req_handle, void *context);  // called from client
    static size_t collectBatchEntries(const uint64_t start_idx, const uint64_t end_idx, uint8_t *buf);
    static void AppendEntryHandler(erpc::ReqHandle *req_handle, void *context);  // called from client
    static void addToEntryCacheAsync(uint64_t base_idx, const uint8_t *buf);
#endif

    // static int mmapWriteToDisk(std::string &path, const std::vector<LogEntry> &es, size_t size);
    static void server_func(const Properties &p);
    static void read_server_func(const Properties &p, int t_id);

   protected:
    static std::unordered_map<std::string, std::shared_ptr<ShardClient>> backups_;
    static std::unordered_map<uint64_t, std::vector<LogEntry>> entries_cache_set_;
    static std::map<uint64_t, int> entries_fd_set_;
    static std::unordered_map<uint64_t, size_t> cache_size_;
    static size_t stripe_unit_size_;
    static int shard_num_;
    static int shard_id_;
    static std::string folder_path_;
    static std::shared_mutex cache_rw_lock_;
    static std::condition_variable_any cache_write_cv_;
    static ShardServerMetrics metrics_;
    static uint64_t replicated_index_;
    static uint64_t global_index_;
    static bool terminate_;

    std::vector<std::thread> server_threads_;
    std::thread fsync_thread_;
    bool is_primary_;
#ifdef CORFU
    static int entry_fd_;
    static uint64_t entry_size_;
    static std::unordered_map<uint64_t, std::atomic<int>> cache_size_atomic_;
#endif
};
}  // namespace lazylog
