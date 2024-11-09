#include "dur_log.h"
#include "infinity/core/Context.h"
#include "infinity/memory/Buffer.h"
#include "infinity/queues/QueuePair.h"
#include "infinity/queues/QueuePairFactory.h"

namespace lazylog {

using namespace infinity;

class DurabilityLogFlat : public DurabilityLog {
   public:
    DurabilityLogFlat();
    ~DurabilityLogFlat() override;

    void Initialize(const Properties &p, void *param) override;
    void Finalize() override;

    uint64_t AppendEntry(LogEntry &e) override;
    uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint32_t max_entries_num) override;
    uint64_t GetNumDurEntry() override;
    uint64_t GetNumOrderedEntry() override;
    int SpecReadEntry(const uint64_t idx, LogEntry &e) override;

    const uint8_t *GetMR() { return mem_rg_raw_; }

   protected:
    uint32_t delEntriesPrimary(const std::vector<LogEntry::ReqID> &max_seq_req_ids) override;
    uint32_t delEntriesBackup(const std::vector<LogEntry::ReqID> &max_seq_req_ids) override;
    void waitForPlace(uint64_t size);

   protected:
    std::mutex idx_lk_;
    Sequencer *indexer_;  // logical index for the incoming entry
    uint64_t *ordered_idx_watermk_;  // logical index of the first unordered entry, entries with logical index less than this are ordered
    uint64_t *idx_to_seq_;
    uint64_t idx_map_elem_cnt_;
    
    uint8_t *mem_rg_raw_;
    uint64_t rg_size_;

    core::Context *context_;
    memory::Buffer *mr_;
    memory::RegionToken *mr_token_;
    queues::QueuePairFactory *qp_factory_;
    queues::QueuePair *qp_;

    std::list<LogEntry> pending_entries_;

    bool wait_;
    std::atomic<uint64_t> total_wait_ns_;
};

} // namespace lazylog
