#pragma once

#include "shard_client.h"
#include "storage_backend.h"

namespace lazylog {

class NaiveBackend : public StorageBackend {
   public:
    NaiveBackend();
    ~NaiveBackend();

    void InitializeBackend(const Properties &p) override;
    void FinalizeBackend() override;
    uint64_t AppendBatch(const std::vector<LogEntry> &es) override;
    bool ReadEntry(const uint64_t idx, LogEntry &e) override;
    void UpdateGlobalIdx(const uint64_t idx) override;

   protected:
    bool allRPCCompleted(std::vector<RPCToken> &tokens);
    void waitForAllShards(std::vector<RPCToken> &tokens);

   protected:
    // std::unordered_map<uint64_t, std::vector<LogEntry> > entries_cache_set_;
    size_t stripe_unit_size_;
    int shard_num_;
    std::unordered_map<int, std::shared_ptr<ShardClient> > shard_clients_;
};

class NaiveReadBackend : public StorageBackend {
   public:
    NaiveReadBackend(int thread_id);
    ~NaiveReadBackend();

    uint64_t AppendBatch(const std::vector<LogEntry> &es) override;
    void UpdateGlobalIdx(const uint64_t idx) override;
    void InitializeBackend(const Properties &p) override;
    void FinalizeBackend() override;
    bool ReadEntry(const uint64_t idx, LogEntry &e) override;

#ifdef CORFU
    void InitializeBackendBackup(const Properties &p, int idx = 1);
#endif

   protected:
    bool allRPCCompleted(std::vector<RPCToken> &tokens);
    void waitForAllShards(std::vector<RPCToken> &tokens);

   protected:
    // std::unordered_map<uint64_t, std::vector<LogEntry> > entries_cache_set_;
    size_t stripe_unit_size_;
    int shard_num_;
    int thread_id_;  // which server read thread to connect to (0 -> remoteRpcId:64, 1 -> remoteRpcId:65 ...)
    std::unordered_map<int, std::shared_ptr<ShardClient> > shard_clients_;
};

}  // namespace lazylog
