#pragma once

#include <condition_variable>
#include <memory>
#include <unordered_map>
#include <vector>

#include "../dur_log/dur_log_cli.h"
#include "../rpc/common.h"
#include "../utils/properties.h"
#include "storage/storage_backend.h"

namespace lazylog {

class ConsensusLog {
    friend class ERPCConsLogTransport;
   public:
    ConsensusLog();
    ~ConsensusLog();

    void Initialize(const Properties &p, void *param);
    void Finalize();

    uint64_t DispatchEntry(const LogEntry &e) = delete;
    std::vector<uint64_t> DispatchEntries(const std::vector<LogEntry> &es) = delete;  // currently not used
    void ReadEntry(const uint64_t l);
    void ReadEntries(const uint64_t from, const uint64_t to);
    uint64_t GetNumOrderedEntries();

   protected:
    // This is the main function of Consensus log which will run periodically (probably in a dedicated thread).
    bool fetchAndStore();
    void fetch();
    void store(bool &run);
    bool allDeletionCompleted();

   protected:
    struct PipelineObj {
        std::vector<LogEntry> entries_buf_;
        std::mutex lock_;
        std::condition_variable cv_full_;
        std::condition_variable cv_empty_;
        bool empty_;
        PipelineObj() : empty_(true) {}
    };
    std::vector<PipelineObj> buffers_;

   protected:
    std::shared_ptr<DurabilityLogCli> pri_dur_cli_;  // used to fetch from durability log
    std::unordered_map<std::string, std::shared_ptr<DurabilityLogCli> > dur_cli_;
    bool is_primary_;
    std::string cons_primary_server;
    std::shared_ptr<StorageBackend> backend_;
    uint64_t max_ordered_idx_;

    uint32_t max_fetch_size_;

    uint64_t total_be_size_;
    uint64_t total_fetch_time_;
    uint64_t total_append_time_;
    uint64_t total_gc_time_;
    uint64_t total_be_n_;
};

}  // namespace lazylog
