#include "lazylog_scalable_cli.h"

namespace lazylog {
std::ostream &operator<<(std::ostream &out, LazyLogScalableClientStatistics const &metrics) {
    out << "metrics: " << std::endl
        << "\tfraction of cached reads: "
        << (long double)metrics.num_cached_reads_ / (metrics.num_cached_reads_ + metrics.num_uncached_reads_)
        << std::endl
        << "\tavg FE polls per uncached read: " << (long double)metrics.num_fe_polls_ / metrics.num_uncached_reads_
        << std::endl
        << "\tavg metadata fetched per uncached read: "
        << (long double)metrics.num_metadata_entries_fetched_ / metrics.num_uncached_reads_ << std::endl
        << "\tavg metadata calls per uncached read: "
        << (long double)metrics.num_metadata_calls_ / metrics.num_uncached_reads_ << std::endl
        << "\tnum early reads: " << metrics.num_early_reads_ << std::endl;
    return out;
}

LazyLogScalableClient::~LazyLogScalableClient() {
    for (uint64_t i = 0; i < shard_num_; i++) {
        datalog_clis_[i].bac->Finalize();
        datalog_clis_[i].pri->Finalize();
    }

    std::cout << stats_;
}

void LazyLogScalableClient::Initialize(const Properties &p) {
    Properties modified_p = p;
    modified_p.SetProperty("init_be_cli", "false");  // do not init the original be backend
    LazyLogClient::Initialize(modified_p);

    std::vector<std::string> shd_primaries =
        SeparateValue(p.GetProperty(PROP_SHD_PRI_URI, PROP_SHD_PRI_URI_DEFAULT), ',');
    std::vector<std::string> shd_backups =
        SeparateValue(p.GetProperty(PROP_SHD_BACKUP_URI, PROP_SHD_BACKUP_URI_DEFAULT), ',');

    shard_num_ = std::stoll(p.GetProperty("shard.num", "1"));
    uint64_t client_id = std::stoll(p.GetProperty("dur_log.client_id"));
    uint64_t shard_id = client_id % shard_num_;
    LOG(INFO) << "client_id: " << client_id << " connecting to shard: " << shard_id << " for writes" << std::endl;
    for (uint64_t i = 0; i < shard_num_; i++) {
        DataLogShard shd;
        shd.pri = std::make_shared<DataLogClient>();
        shd.bac = std::make_shared<DataLogClient>();
        shd.pri->InitializeConn(p, shd_primaries[i], nullptr);
        shd.bac->InitializeConn(p, shd_backups[i], nullptr);
        datalog_clis_.emplace(i, shd);
    }
    shard_id_ = shard_id;
    current_ordered_tail_ = 0;
}

bool LazyLogScalableClient::ReadEntry(const uint64_t idx, std::string &data) {
    LogEntry e;

    uint64_t unordered_tail = 0, next_idx = 0;
    if (idx >= gsn_shard_list_.size()) {
        // index is not cached
        std::tuple<uint64_t, uint64_t, uint16_t> tail;
        next_idx = gsn_shard_list_.size();

        // keep polling FE until the tail is greater than or equal to the index we want
        while (idx >= current_ordered_tail_) {
            stats_.num_fe_polls_++;
            tail = LazyLogClient::GetTail();
            current_ordered_tail_ = std::get<1>(tail);
            unordered_tail = std::get<0>(tail);
            if (idx >= unordered_tail) {
                stats_.num_early_reads_++;
                return false;  // index has not even been appended yet
            }
            // give some time for records to accumulate
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }

        stats_.num_uncached_reads_++;
        uint64_t num_remaining = std::min(current_ordered_tail_ - next_idx, MAX_READAHEAD);
        stats_.num_metadata_entries_fetched_ += num_remaining;
        std::vector<int> entries;
        while (num_remaining != 0) {
            stats_.num_metadata_calls_++;
            uint64_t curr_batch = std::min(num_remaining, MAX_READ_METADATA_ENTRIES);
            datalog_clis_[shard_id_].pri->GetReadMetadata(next_idx, curr_batch + next_idx - 1, entries);
            gsn_shard_list_.insert(gsn_shard_list_.end(), entries.begin(), entries.end());
            entries.clear();
            num_remaining -= curr_batch;
            next_idx += curr_batch;
        }
    } else {
        stats_.num_cached_reads_++;
    }
    bool ret = datalog_clis_[gsn_shard_list_[idx]].pri->ReadEntry(idx, e);
    data = std::move(e.data);
    return true;
}

bool LazyLogScalableClient::ReadEntries(const uint64_t from, const uint64_t to, std::vector<LogEntry> &es) {
    if (to - from + 1 > MAX_READ_BATCH_ENTRIES) {
        // return false if max batch size is exceeded
        return false;
    }

    uint64_t unordered_tail = 0, next_idx = 0;
    if (to >= gsn_shard_list_.size()) {
        // index is not cached
        std::tuple<uint64_t, uint64_t, uint16_t> tail;
        next_idx = gsn_shard_list_.size();

        // keep polling FE until the tail is greater than or equal to the index we want
        while (to >= current_ordered_tail_) {
            stats_.num_fe_polls_++;
            tail = LazyLogClient::GetTail();
            current_ordered_tail_ = std::get<1>(tail);
            unordered_tail = std::get<0>(tail);
            if (to >= unordered_tail) {
                stats_.num_early_reads_++;
                return false;  // index has not even been appended yet
            }
            // give some time for records to accumulate
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }

        stats_.num_uncached_reads_++;
        uint64_t num_remaining = std::min(current_ordered_tail_ - next_idx, MAX_READAHEAD);
        std::vector<int> entries;
        stats_.num_metadata_entries_fetched_ += num_remaining;
        while (num_remaining != 0) {
            stats_.num_metadata_calls_++;
            uint64_t curr_batch = std::min(num_remaining, MAX_READ_METADATA_ENTRIES);
            datalog_clis_[shard_id_].pri->GetReadMetadata(next_idx, curr_batch + next_idx - 1, entries);
            gsn_shard_list_.insert(gsn_shard_list_.end(), entries.begin(), entries.end());
            entries.clear();
            num_remaining -= curr_batch;
            next_idx += curr_batch;
        }
    } else {
        stats_.num_cached_reads_++;
    }

    // find first and last entry for each shard
    std::pair<uint64_t, uint64_t> shard_range[shard_num_];
    std::pair<bool, bool> assigned_range[shard_num_] = {};
    for (uint64_t i = from, num_assigned = 0; i <= to && num_assigned < shard_num_; i++) {
        uint64_t shard_id = gsn_shard_list_[i];
        if (!assigned_range[shard_id].first) {
            shard_range[shard_id].first = i;
            assigned_range[shard_id].first = true;
            num_assigned++;
        }
    }
    for (uint64_t i = to, num_assigned = 0; i >= from && num_assigned < shard_num_; i--) {
        uint64_t shard_id = gsn_shard_list_[i];
        if (!assigned_range[shard_id].second) {
            shard_range[shard_id].second = i;
            assigned_range[shard_id].second = true;
            num_assigned++;
        }
        if (i == 0) {
            break;
        }
    }

    std::vector<std::shared_ptr<RPCToken> > tokens;
    std::vector<LogEntry> entries[shard_num_];
    for (uint64_t i = 0; i < shard_num_; i++) {
        if (assigned_range[i].first && assigned_range[i].second) {
            auto tkn = std::make_shared<RPCToken>();
            datalog_clis_[i].pri->ReadEntriesAsync(shard_range[i].first, shard_range[i].second, tkn);
            tokens.emplace_back(tkn);
        }
    }

    do {
        ERPCTransport::RunERPCOnce();
    } while (!allCompleted(tokens));

    // populate entries
    for (uint64_t i = 0; i < shard_num_; i++) {
        if (assigned_range[i].first && assigned_range[i].second) {
            datalog_clis_[i].pri->GetReadResponses(entries[i]);
        }
    }

    // merge entries in entries[shard_num_] based on log_idx and output a sorted vector in es
    for (uint64_t i = 0; i < shard_num_; i++) {
        if (assigned_range[i].first && assigned_range[i].second) {
            es.insert(es.end(), entries[i].begin(), entries[i].end());
        }
    }

    std::sort(es.begin(), es.end(), [](const LogEntry &a, const LogEntry &b) { return a.log_idx < b.log_idx; });

    return true;
}

std::pair<uint64_t, uint64_t> LazyLogScalableClient::AppendEntryAll(const std::string &data) {
    LogEntry e = constructLogEntry({});

    e.data = std::to_string(shard_id_);
    e.size = e.data.size();

    std::vector<std::shared_ptr<RPCToken> > tokens;
    for (auto &dc : dur_clis_) {
        auto tkn = std::make_shared<RPCToken>();
        dc.second->AppendEntryAsync(e, tkn);
        tokens.emplace_back(tkn);
    }

    e.size = data.size();
    e.data = data;
    auto tkn_pri = std::make_shared<RPCToken>();
    datalog_clis_[shard_id_].pri->AppendEntryShardAsync(e, tkn_pri);
    tokens.emplace_back(tkn_pri);
    auto tkn_bac = std::make_shared<RPCToken>();
    datalog_clis_[shard_id_].bac->AppendEntryShardAsync(e, tkn_bac);
    tokens.emplace_back(tkn_bac);

    do {
        ERPCTransport::RunERPCOnce();
    } while (!allCompleted(tokens));

    if (!datalog_clis_[shard_id_].pri->getAppendStatus()) return {0, e.client_id};

    return {e.client_id, e.client_seq};
}

}  // namespace lazylog
