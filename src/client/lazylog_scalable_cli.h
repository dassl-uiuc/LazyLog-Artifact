#pragma once

#include <chrono>

#include "../cons_log/storage/datalog/datalog_client.h"
#include "lazylog_cli.h"
namespace lazylog {

struct LazyLogScalableClientStatistics {
    // number of reads for which metadata was already cached
    uint64_t num_cached_reads_;

    // num early reads
    uint64_t num_early_reads_;

    // number of times we polled the frontend to get tail
    uint64_t num_fe_polls_;

    // number of reads for which metadata was not cached
    uint64_t num_uncached_reads_;

    // total number of entries fetched through metadata calls
    uint64_t num_metadata_entries_fetched_;

    // total number of metadata calls
    uint64_t num_metadata_calls_;

    LazyLogScalableClientStatistics()
        : num_cached_reads_(0),
          num_fe_polls_(0),
          num_uncached_reads_(0),
          num_metadata_entries_fetched_(0),
          num_metadata_calls_(0),
          num_early_reads_(0) {}

    friend std::ostream &operator<<(std::ostream &out, LazyLogScalableClientStatistics const &metrics);
};

class LazyLogScalableClient : public LazyLogClient {
   public:
    ~LazyLogScalableClient() override;
    void Initialize(const Properties &p) override;
    std::pair<uint64_t, uint64_t> AppendEntryAll(const std::string &data) override;
    bool ReadEntry(const uint64_t idx, std::string &data) override;
    bool ReadEntries(const uint64_t from, const uint64_t to, std::vector<LogEntry> &es) override;

   protected:
    struct DataLogShard {
        std::shared_ptr<DataLogClient> pri;
        std::shared_ptr<DataLogClient> bac;
    };
    std::unordered_map<uint64_t, DataLogShard> datalog_clis_;
    LazyLogScalableClientStatistics stats_;
    uint64_t current_ordered_tail_;
    int shard_num_;
    std::vector<int> gsn_shard_list_;
    int shard_id_;
};

}  // namespace lazylog
