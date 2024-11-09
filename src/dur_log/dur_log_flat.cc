#include "dur_log_flat.h"

#include "../utils/utils.h"
#include "glog/logging.h"
#include "infinity/core/Configuration.h"
#include "stdlib.h"

namespace lazylog {

DurabilityLogFlat::DurabilityLogFlat()
    : idx_to_seq_(nullptr),
      idx_map_elem_cnt_(0),
      mem_rg_raw_(nullptr),
      rg_size_(0),
      context_(nullptr),
      mr_(nullptr),
      mr_token_(nullptr),
      qp_factory_(nullptr),
      qp_(nullptr),
      wait_(true),
      total_wait_ns_(0) {}

DurabilityLogFlat::~DurabilityLogFlat() {}

void DurabilityLogFlat::Initialize(const Properties& p, void* param) {
    DurabilityLog::Initialize(p, param);
    rg_size_ = std::stoi(p.GetProperty("dur_log.mr_size_m", "512")) * 1024 * 1024;
    idx_map_elem_cnt_ = rg_size_ / min_entry_size;

    if (p.GetProperty("wait", "true") == "false") wait_ = false;

    LOG(INFO) << "index map size: " << idx_map_elem_cnt_ << " mappings";

    context_ = new core::Context(p.GetProperty("rdma.dev_name", core::Configuration::DEFAULT_IB_DEVICE),
                                 core::Configuration::DEFAULT_IB_PHY_PORT);
    qp_factory_ = new queues::QueuePairFactory(context_);

    if (posix_memalign(reinterpret_cast<void**>(&mem_rg_raw_), PAGE_SIZE, rg_size_ + SAFE_RG_SIZE + PAGE_SIZE) != 0) {
        throw Exception("Unable to allocate memory region");
    }

    if (posix_memalign(reinterpret_cast<void**>(&idx_to_seq_), PAGE_SIZE, idx_map_elem_cnt_ * sizeof(*idx_to_seq_)) != 0) {
        throw Exception("Unable to allocate idx to seq map");
    }

    /**
     * memory region layout:
     * |***********entries***********| 8KB safe region |sequencer|ordered watermk|ordered idx watermk|indexer|
     */
    sequencer_ = new (mem_rg_raw_ + rg_size_ + SAFE_RG_SIZE) Sequencer;
    ordered_watermk_ = reinterpret_cast<uint64_t*>(mem_rg_raw_ + rg_size_ + SAFE_RG_SIZE + sizeof(*sequencer_));
    ordered_idx_watermk_ = ordered_watermk_ + 1;
    indexer_ = new (mem_rg_raw_ + rg_size_ + SAFE_RG_SIZE + sizeof(*sequencer_) + sizeof(*ordered_watermk_) + sizeof(*ordered_idx_watermk_)) Sequencer;
    *ordered_watermk_ = 0;
    *ordered_idx_watermk_ = 0;

    mr_ = new memory::Buffer(context_, mem_rg_raw_, rg_size_ + SAFE_RG_SIZE + PAGE_SIZE);

    LOG(INFO) << "Using flat layout log, MR size: " << rg_size_ / 1024 / 1024 << "M";

    mr_token_ = mr_->createRegionToken();

    if (p.GetProperty("test", "false") == "false" && is_primary_) {
        LOG(INFO) << "Waiting for RDMA queue pair connection";
        qp_factory_->bindToPort(std::stoi(p.GetProperty("rdma.ip_port_ctrl", "8011")));
        qp_ = qp_factory_->acceptIncomingConnection(mr_token_, sizeof(*mr_token_));
        LOG(INFO) << "RDMA queue pair connected";
    }
}

void DurabilityLogFlat::Finalize() {
    if (qp_ != nullptr) delete qp_;

    if (mr_token_ != nullptr) delete mr_token_;

    if (mr_ != nullptr) delete mr_;

    if (idx_to_seq_ != nullptr) {
        free(idx_to_seq_);
        idx_to_seq_ = nullptr;
    }

    if (mem_rg_raw_ != nullptr) {
        free(mem_rg_raw_);
        mem_rg_raw_ = nullptr;
    }

    if (qp_factory_ != nullptr) delete qp_factory_;
    if (context_ != nullptr) delete context_;

    LOG(INFO) << "Total wait time: " << total_wait_ns_.load() / 1e6 << "ms";
}

void DurabilityLogFlat::waitForPlace(uint64_t size) {
    // It must block instead of yield
    if (!wait_) return;
    while (sequencer_->GetCurSeqNonAtm() + size - *ordered_watermk_ >= rg_size_ - (PAGE_SIZE << 6)) {
        /**
         * If there is no available space to append a entry, then we shall wait. In other words, if after appending this
         * entry the total size of unordered entries will exceed the MR size, then we shall wait.
         *   The available space is not as simple as the size of a single entry. Considering the case that K threads are
         * waiting here at the same time. Then `ordered_watermk_` is updated, the condition is satisfied, and all of
         * them exist the wait. Then they will all increase the sequence number so the sequence number will be increased
         * by K*`entry_size`. We need to make sure that even after this, the total size of unordered entries won't
         * exceed MR size. So before letting go these theads, we need to have at least K*`entry_size` available space.
         * Considering the max entry size to be 4K, and max number of threads to be 32, 4K*64 should be enough. That's
         * where `(PAGE_SIZE << 6)` comes from.
         */
        ;
    }
}

uint64_t DurabilityLogFlat::AppendEntry(LogEntry& e) {
    using namespace std::chrono;
    uint64_t seq, idx;
    uint64_t e_size = GetSize(e);

    auto start = high_resolution_clock::now();
    waitForPlace(e_size);  // this shall not block the rpc to update the ordered_watermk_
    auto end = high_resolution_clock::now();
    total_wait_ns_.fetch_add(duration_cast<nanoseconds>(end - start).count());

    uint64_t offset;
    {
        std::lock_guard<std::mutex> idx_lock(idx_lk_);

        seq = sequencer_->GetCurSeqNonAtm();
        offset = seq % rg_size_;  // rewind
        /**
         * It's ok for the `flag_offset` to exceed entries region boundary, since we will copy all overflowed data to
         * the beginning shortly.
         *   Also, this gives us an advantage that you can always access any field of an entry by giving its offset
         * without worrying about whether this entry is rewinded or not, because data of the the rewinded part will also
         * be in the overflowed region (safe region), which gives you a entry that sits in a consecutive memory.
         * |4567)          (123|4567) |
         */
        SetFlag(mem_rg_raw_ + offset, 0);  // entry is not written to memory now
        idx = indexer_->GetCurSeqNonAtm();
        if (is_primary_) idx_to_seq_[idx % idx_map_elem_cnt_] = seq;
        // todo: <memory barrier> 
        indexer_->GetNextSeqNonAtm();
        sequencer_->GetNextSeqNonAtm(e_size);  // increase sequence number in the last step
    }

    e.log_idx = idx;
    e.view = view_;

    auto ety_size = Serializer(e, mem_rg_raw_ + offset);
    // todo: <memory barrier>
    /**
     * As a result, we must set the flag here rather than after the `memcpy`. This is to ensure that flag is also set in
     * the overflowed part so when you access this entry by its offset (as if it sits in a consecutive memory), you will
     * get the correct flag.
     * |_*__)      (__|_*__) |  [* stands for the flag]
     */
    SetFlag(mem_rg_raw_ + offset, 1);  // entry has been written to memory
    if (offset + ety_size > rg_size_) {
        memcpy(mem_rg_raw_, mem_rg_raw_ + rg_size_, offset + ety_size - rg_size_);  // move overflowed data to beginning
    }

    auto reqid = std::make_pair(e.client_id, e.client_seq);
    if (!is_primary_) {
        std::lock_guard<std::mutex> lock(lk_);
        reqid_to_seq_map_[reqid] = seq;  // primary does not need this to del entries
    }

    // LOG(INFO) << "[[FLAT]] LogEntry [" << e.client_id << ", " << e.client_seq << "], sequence " << seq;

    return seq;
}

uint32_t DurabilityLogFlat::FetchUnorderedEntries(std::vector<LogEntry>& e, uint32_t max_entries_num) {
    uint32_t n = 0;

    if (is_primary_) {
        LOG(WARNING) << "fetch unordered entries on primary should be cpu free";
        return 0;
    }

    for (uint64_t s = *ordered_watermk_; s < sequencer_->GetCurSeqNonAtm();) {
        e.emplace_back();
        uint64_t ofst = s % rg_size_;
        s += Deserializer(e.back(), mem_rg_raw_ + ofst);  // todo: deal with data break into 2 pieces at MR end
        if (++n >= max_entries_num) {
            break;
        }
    }

    return n;
}

uint64_t DurabilityLogFlat::GetNumDurEntry() { return indexer_->GetCurSeqNonAtm(); }

uint64_t DurabilityLogFlat::GetNumOrderedEntry() { return *ordered_idx_watermk_; }

int DurabilityLogFlat::SpecReadEntry(const uint64_t idx, LogEntry& e) {
    if (idx <= indexer_->GetCurSeqNonAtm() - idx_map_elem_cnt_ + 64)  // this index is way too low, 64 is just for safe
        return -1;
    else if (idx >= indexer_->GetCurSeqNonAtm())
        return -2;
    
    uint64_t seq = idx_to_seq_[idx % idx_map_elem_cnt_];
    uint64_t cur_seq = sequencer_->GetCurSeqNonAtm();
    if (cur_seq > rg_size_ && seq <= cur_seq - rg_size_ + (PAGE_SIZE << 6))  // we don't want to interfere with AppendEntry 
        return -1;
    uint64_t offset = seq % rg_size_;
    if (GetIdx(mem_rg_raw_ + offset) != idx)
        return 0;
    return Deserializer(e, mem_rg_raw_ + offset);    
}

uint32_t DurabilityLogFlat::delEntriesPrimary(const std::vector<LogEntry::ReqID>& max_seq_req_ids) {
    LOG(WARNING) << "del entries on primary should be cpu free";
    return 0;
}

/**
 * This function must run on a separate thread other than AppendEntry. Because AppendEntry will busy waiting and
 * the event loop, preventing incoming rpc from being handled.
 */
uint32_t DurabilityLogFlat::delEntriesBackup(const std::vector<LogEntry::ReqID>& max_seq_req_ids) {
    uint64_t max_seq = 0;
    uint32_t n_del = 0;
    std::unordered_map<uint64_t, uint64_t> cli_id_max_seq_map;
    {
        std::lock_guard<std::mutex> lock(lk_);
        for (auto& rid : max_seq_req_ids) {
            cli_id_max_seq_map[rid.first] = rid.second;
            max_seq = std::max(max_seq, reqid_to_seq_map_[rid]);
        }
    }

    for (auto it = pending_entries_.begin(); it != pending_entries_.end();) {
        auto next_it = std::next(it);
        if (it->client_seq <= cli_id_max_seq_map[it->client_id]) {
            pending_entries_.erase(it);
            n_del++;
        } else {
            // todo: add count if not removed
        }
        it = next_it;
    }

    uint64_t s;
    {
        std::lock_guard<std::mutex> lock(lk_);
        for (s = *ordered_watermk_; s <= max_seq;) {
            uint64_t ofst = s % rg_size_;
            /**
             * It's possible that at this time the entry at `s` is not ready yet, so we wait a bit.
             *   We have to wait instead of give up and break because this will leave `ordered_watermk_` on this backup
             * lagging. However, the primary won't lag. If it lags too much, entry will not be able to append on this
             * backup. And since the primary doesn't lag, no GC will be initiated, then this backup will lag forever,
             * stalling the progress.
             */
            while (GetFlagFromBuf(mem_rg_raw_ + ofst) != 1) {
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            }
            uint64_t cli_id = GetClientId(mem_rg_raw_ + ofst);
            uint64_t cli_seq = GetClientSeq(mem_rg_raw_ + ofst);

            if (cli_seq > cli_id_max_seq_map[cli_id]) {
                pending_entries_.emplace_back();
                Deserializer(pending_entries_.back(), mem_rg_raw_ + ofst);
            } else {
                n_del++;
            }

            LogEntry::ReqID rid = std::make_pair(cli_id, cli_seq);
            reqid_to_seq_map_.erase(rid);

            s += GetSizeFromBuf(mem_rg_raw_ + ofst);
            *ordered_idx_watermk_ = GetIdx(mem_rg_raw_ + ofst) + 1;
        }
    }

    *ordered_watermk_ = s;

    return n_del;
}

}  // namespace lazylog
