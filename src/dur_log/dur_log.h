#pragma once

#include <absl/container/flat_hash_map.h>
#include <atomic>
#include <list>
#include <vector>
#include <mutex>
#include "../rpc/common.h"
#include "../cons_log/cons_log_cli.h"
#include "../utils/sequencer.h"
#include "../utils/properties.h"

namespace lazylog {

class DurabilityLog {
   public:
    DurabilityLog();
    virtual ~DurabilityLog();
    
    virtual void Initialize(const Properties &p, void *param);
    virtual void Finalize();

    virtual uint64_t AppendEntry(LogEntry &e);
    virtual uint64_t OrderEntry(const LogEntry &e);
    virtual uint64_t GetNumDurEntry();
    virtual uint64_t GetNumOrderedEntry();
    virtual bool FetchUnorderedEntry(LogEntry &e) = delete;  // deprecated, does not make too much sense to fetch only 1 entry
    virtual uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint32_t max_entries_num);
    virtual bool SetMaxOrderedSeq(uint64_t seq) = delete;  // deprecated, this is done alone with deleting ordered entries
    /**
     * @param max_seq_req_ids for each client id in all entries to delete, the request id with the max client sequence number
     */
    virtual uint32_t DelOrderedEntries(const std::vector<LogEntry::ReqID> &max_seq_req_ids);
    /**
     * @return true if the entry is on DL, false if the entry is no valid or has been GCed.
     */
    virtual int SpecReadEntry(const uint64_t idx, LogEntry &e);
    virtual uint16_t GetView() const;

#ifdef CORFU
    virtual uint64_t GetGSN();
    virtual uint64_t GetGSN(uint64_t batchSize);
#endif

   protected:
    virtual uint32_t delEntriesPrimary(const std::vector<LogEntry::ReqID> &max_seq_req_ids);
    virtual uint32_t delEntriesBackup(const std::vector<LogEntry::ReqID> &max_seq_req_ids);

   protected:
    absl::flat_hash_map<uint64_t, std::shared_ptr<LogEntry> > dur_log_;
    absl::flat_hash_map<LogEntry::ReqID, uint64_t> reqid_to_seq_map_;
    // std::shared_ptr<ConsensusLogCli> cons_cli_;
    Sequencer *sequencer_;
    uint64_t *ordered_watermk_;  // sequence number of the last ordered entry, initiated with -1 (UINT64_MAX)
    uint16_t view_;
    bool is_primary_;
    std::mutex lk_;

    std::list<std::shared_ptr<LogEntry> > pending_entries_ptr_;

#ifdef CORFU
    std::atomic<uint64_t> gsn_;
#endif

   private:
    Sequencer sequencer_base_;
    uint64_t ordered_watermk_base_;
};

}  // namespace lazylog
