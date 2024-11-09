#include "dur_log.h"

#include "../cons_log/cons_log_erpc_cli.h"
#include "../rpc/rpc_factory.h"
#include "glog/logging.h"

namespace lazylog {

DurabilityLog::DurabilityLog() : view_(0), is_primary_(false), ordered_watermk_base_(-1) {
    sequencer_ = &sequencer_base_;
    ordered_watermk_ = &ordered_watermk_base_;
#ifdef CORFU
    gsn_ = 0;
#endif
}

DurabilityLog::~DurabilityLog() {}

void DurabilityLog::Initialize(const Properties &p, void *param) {
    sequencer_->Reset();

    if (p.GetProperty("leader", "false") != "true") {
        LOG(INFO) << "Not a durability log leader";
        return;
    }
    is_primary_ = true;
    LOG(INFO) << "This is durability log leader";
}

void DurabilityLog::Finalize() {}

uint64_t DurabilityLog::AppendEntry(LogEntry &e) {
    std::lock_guard<std::mutex> lock(lk_);

    auto seq = sequencer_->GetNextSeq();
    e.log_idx = seq;
    auto entry = dur_log_.emplace(std::make_pair(seq, std::make_shared<LogEntry>()));
    *(entry.first->second) = e;

    auto reqid = std::make_pair(e.client_id, e.client_seq);
    reqid_to_seq_map_[reqid] = seq;

    LOG(INFO) << "LogEntry [" << e.client_id << ", " << e.client_seq << "], sequence " << seq;

    return seq;
}

uint64_t DurabilityLog::OrderEntry(const LogEntry &e) {
    LOG(WARNING) << "this function is temporarily not in use";
    return 0;

/*
    uint64_t seq = AppendEntry(e);
    std::vector<LogEntry> unordered_entries;

    for (uint64_t i = ordered_watermk_ + 1; i <= seq; i++) {
        unordered_entries.push_back(*dur_log_[i]);
    }

    auto final_seqs = cons_cli_->DispatchEntries(unordered_entries);

    for (uint64_t i = ordered_watermk_ + 1; i <= seq; i++) {
        dur_log_.erase(i);
        // TODO: also erase on other replicas
    }

    ordered_watermk_ = seq;  // todo: make sure ordered_watermk_ monotonically increase

    return final_seqs.back();
*/
}

uint64_t DurabilityLog::GetNumDurEntry() { return sequencer_->GetCurSeq() - 1; }

uint64_t DurabilityLog::GetNumOrderedEntry() { return *ordered_watermk_; }

uint32_t DurabilityLog::FetchUnorderedEntries(std::vector<LogEntry> &e, uint32_t max_entries_num) {
    uint32_t n;
    for (uint64_t i = *ordered_watermk_, n = 0; i <= sequencer_->GetCurSeq(); i++) {
        e.push_back(*(dur_log_.at(i)));
        if (++n >= max_entries_num) {
            break;
        }
    }

    return n;
}

uint32_t DurabilityLog::DelOrderedEntries(const std::vector<LogEntry::ReqID> &max_seq_req_ids) {
    if (is_primary_)
        return delEntriesPrimary(max_seq_req_ids);
    else
        return delEntriesBackup(max_seq_req_ids);
}

int DurabilityLog::SpecReadEntry(const uint64_t idx, LogEntry &e) { return 0; }

uint16_t DurabilityLog::GetView() const { return view_; }

#ifdef CORFU
uint64_t DurabilityLog::GetGSN() {
    uint64_t ret = gsn_.fetch_add(1);
    return ret;
}

uint64_t DurabilityLog::GetGSN(uint64_t batchSize) {
    uint64_t ret = gsn_.fetch_add(batchSize);
    return ret;
}
#endif

uint32_t DurabilityLog::delEntriesPrimary(const std::vector<LogEntry::ReqID> &max_seq_req_ids) {
    std::lock_guard<std::mutex> lock(lk_);
    uint64_t max_seq = 0;
    uint32_t n_del;

    for (auto &rid : max_seq_req_ids) {
        max_seq = std::max(max_seq, reqid_to_seq_map_[rid]);
    }

    /**
     * since the entries are fetched from primary, sequence range [ordered_watermk_, max_seq] must contain all entries
     * that need to be deleted and must not contain any entry that need not to be deleted
     */
    for (uint64_t s = *ordered_watermk_ + 1; s <= max_seq; s++) {
        auto it = dur_log_.find(s);
        if (it == dur_log_.end())
            continue;  // should not happen
        LogEntry::ReqID rid = std::make_pair(it->second->client_id, it->second->client_seq);
        reqid_to_seq_map_.erase(rid);
        dur_log_.erase(it);
        n_del++;
    }

    *ordered_watermk_ = max_seq;

    return n_del;
}

uint32_t DurabilityLog::delEntriesBackup(const std::vector<LogEntry::ReqID> &max_seq_req_ids) {
    std::lock_guard<std::mutex> lock(lk_);
    uint64_t max_seq = 0;
    uint32_t n_del = 0;
    std::unordered_map<uint64_t, uint64_t> cli_id_max_seq_map;
    for (auto &rid : max_seq_req_ids) {
        cli_id_max_seq_map[rid.first] = rid.second;
        max_seq = std::max(max_seq, reqid_to_seq_map_[rid]);
    }

    for (auto it = pending_entries_ptr_.begin(); it != pending_entries_ptr_.end(); ) {
        auto next_it = std::next(it);
        if ((*it)->client_seq <= cli_id_max_seq_map[(*it)->client_id]) {
            pending_entries_ptr_.erase(it);
        } else {
            // todo: add count if not removed
        }
        it = next_it;
    }

    /**
     * Sequence range [ordered_watermk_, max_seq] may contain entries that need not to be deleted, we move them to
     * `pending_entries`. Entries that need to be deleted may be at sequence greater than `max_seq`. We just leave them
     * there for now, and they will be GCed in next call to this function.
     */
    for (uint64_t s = *ordered_watermk_ + 1; s <= max_seq; s++) {
        auto it = dur_log_.find(s);
        if (it == dur_log_.end()) continue;  // should not happen
        if (it->second->client_seq > cli_id_max_seq_map[it->second->client_id]) {  // todo: cli_id_max_seq_map may not contain the client id
            pending_entries_ptr_.push_back(it->second);
        } else {
            n_del++;
        }
        LogEntry::ReqID rid = std::make_pair(it->second->client_id, it->second->client_seq);
        reqid_to_seq_map_.erase(rid);
        dur_log_.erase(it);
    }

    *ordered_watermk_ = max_seq;

    return n_del;
}

}  // namespace lazylog
