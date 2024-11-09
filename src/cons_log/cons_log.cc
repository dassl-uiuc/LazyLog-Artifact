#include "cons_log.h"

#include <fstream>
#include <vector>

#include "../dur_log/dur_log_flat_cli.h"
#include "../rpc/rpc_factory.h"
#include "../utils/timer.h"
#include "cons_log_erpc_cli.h"
#include "storage/naive_backend.h"

namespace lazylog {

ConsensusLog::ConsensusLog()
    : buffers_(2),
      is_primary_(false),
      max_ordered_idx_(0),
      max_fetch_size_(0),
      total_be_size_(0),
      total_fetch_time_(0),
      total_append_time_(0),
      total_gc_time_(0),
      total_be_n_(0) {}

ConsensusLog::~ConsensusLog() {
    std::cout << "Average BE append size: " << total_be_size_ * 1.0 / total_be_n_ << std::endl
              << "Average fetch time: " << total_fetch_time_ * 1e0 / total_be_n_ << "us" << std::endl
              << "Average append time: " << total_append_time_ * 1e0 / total_be_n_ << "us" << std::endl
              << "Average GC time: " << total_gc_time_ * 1e0 / total_be_n_ << "us" << std::endl;
}

void ConsensusLog::Initialize(const Properties& p, void* param) {
    if (p.GetProperty("leader", "true") != "true") {
        LOG(INFO) << "Not a consensus log leader";
        return;
    }
    is_primary_ = true;

    LOG(INFO) << "This is consensus log leader";

    std::string pri_dur_uri = p.GetProperty(PROP_DL_PRI_URI, PROP_DL_PRI_URI_DEFAULT);

    std::vector<std::string> dur_svr_uri = SeparateValue(p.GetProperty(PROP_DL_SVR_URI, PROP_DL_SVR_URI_DEFAULT), ',');
    for (auto& d : dur_svr_uri) {
        // dur_cli_[d] = std::dynamic_pointer_cast<DurabilityLogCli>(
        //     RPCFactory::CreateCliRPCTransport(p));
        dur_cli_[d] = std::make_shared<DurabilityLogFlatCli>();
        dur_cli_[d]->InitializeConn(p, d, reinterpret_cast<void*>(1));
    }

    max_fetch_size_ = std::stoi(p.GetProperty("cons_log.max_fetch_size", "0"));

    pri_dur_cli_ = dur_cli_[pri_dur_uri];

    std::vector<std::string> datalog_uri =
        SeparateValue(p.GetProperty(PROP_SHD_PRI_URI, PROP_SHD_PRI_URI_DEFAULT), ',');
    shard_num_ = std::stoll(p.GetProperty("shard.num", "1"));
    for (uint64_t i = 0; i < shard_num_; i++) {
        datalog_clis_[datalog_uri[i]] = std::make_shared<DataLogClient>();
        datalog_clis_[datalog_uri[i]]->InitializeConn(p, datalog_uri[i], (void*)-1);
    }
}

void ConsensusLog::Finalize() {
    for (auto& sc : datalog_clis_) sc.second->Finalize();
    for (auto& dc : dur_cli_) dc.second->Finalize();
}

uint64_t ConsensusLog::GetNumOrderedEntries() { return max_ordered_idx_ - 1; }

bool ConsensusLog::fetchAndStore() {
    Timer timer;
    timer.Start();
    // Fetch Entries
    std::vector<LogEntry> entries;
    uint32_t fetch_size = pri_dur_cli_->FetchUnorderedEntries(entries, max_fetch_size_);

    if (fetch_size == 0) {
        return false;
    }

    total_fetch_time_ += timer.End();

    timer.Start();

    total_be_size_ += entries.size() - 1;

    std::vector<std::shared_ptr<RPCToken>> tokens;
    for (auto& sc : datalog_clis_) {
        auto tkn = std::make_shared<RPCToken>();
        max_ordered_idx_ = sc.second->SendReqIdGsnMappingAsync(entries, tkn);
        tokens.emplace_back(tkn);
    }

    do {
        DataLogClient::RunERPCOnce();
    } while (!allRPCCompleted(tokens));

    total_append_time_ += timer.End();
    total_be_n_++;

    timer.Start();

    uint64_t last_log_idx = 0;
    for (auto& d : dur_cli_) {
        std::vector<LogEntry::ReqID> req_ids;
        last_log_idx = d.second->ProcessFetchedEntries(entries, req_ids);
        d.second->DeleteOrderedEntriesAsync(req_ids);
    }

    while (!allDeletionCompleted())
        ;  // busy waiting

    total_gc_time_ += timer.End();

    std::vector<RPCToken> tokens_update;
    tokens_update.reserve(shard_num_);
    for (auto& sc : datalog_clis_) {
        tokens_update.emplace_back();
        sc.second->UpdateGlobalIdxAsync(last_log_idx, tokens_update.back());
    }

    do {
        DataLogClient::RunERPCOnce();
    } while (!allRPCCompleted(tokens_update));

    return true;
}

void ConsensusLog::fetch() {
    static uint64_t round = 0;
    static uint64_t fetch_begin = 0;
    Timer timer;

    auto& buf_to_store = buffers_[round % 2];
    std::unique_lock<std::mutex> lg(buf_to_store.lock_);
    while (!buf_to_store.empty_) buf_to_store.cv_empty_.wait(lg);

    timer.Start();
    uint32_t fetch_len = pri_dur_cli_->FetchUnorderedEntries(buf_to_store.entries_buf_, fetch_begin, max_fetch_size_);
    if (fetch_len == 0) return;
    total_be_size_ += buf_to_store.entries_buf_.size() - 1;
    total_fetch_time_ += timer.End();
    total_be_n_++;

    fetch_begin += fetch_len;

    buf_to_store.empty_ = false;
    buf_to_store.cv_full_.notify_one();

    round++;
}

void ConsensusLog::store(bool& run) {
    static uint64_t round = 0;
    Timer timer;

    auto& buf_to_store = buffers_[round % 2];
    std::unique_lock<std::mutex> lg(buf_to_store.lock_);
    while (buf_to_store.empty_) {
        buf_to_store.cv_full_.wait_for(lg, std::chrono::seconds(1));
        if (!run) return;
    }

    timer.Start();

    std::vector<std::shared_ptr<RPCToken>> tokens;
    for (auto& sc : datalog_clis_) {
        auto tkn = std::make_shared<RPCToken>();
        max_ordered_idx_ = sc.second->SendReqIdGsnMappingAsync(buf_to_store.entries_buf_, tkn);
        tokens.emplace_back(tkn);
    }

    do {
        DataLogClient::RunERPCOnce();
    } while (!allRPCCompleted(tokens));

    total_append_time_ += timer.End();

    timer.Start();

    uint64_t last_log_idx = 0;
    for (auto& d : dur_cli_) {
        std::vector<LogEntry::ReqID> req_ids;
        last_log_idx = d.second->ProcessFetchedEntries(buf_to_store.entries_buf_, req_ids);
        d.second->DeleteOrderedEntriesAsync(req_ids);
    }
    total_gc_time_ += timer.End();

    while (!allDeletionCompleted())
        ;  // busy waiting

    buf_to_store.entries_buf_.clear();

    buf_to_store.empty_ = true;
    buf_to_store.cv_empty_.notify_one();

    std::vector<RPCToken> tokens_update;
    tokens_update.reserve(shard_num_);
    for (auto& sc : datalog_clis_) {
        tokens_update.emplace_back();
        sc.second->UpdateGlobalIdxAsync(last_log_idx, tokens_update.back());
    }

    do {
        DataLogClient::RunERPCOnce();
    } while (!allRPCCompleted(tokens_update));

    round++;
}

bool ConsensusLog::allDeletionCompleted() {
    bool ret = true;
    for (auto& d : dur_cli_) {
        if (!d.second->CheckAndRunOnce())
            ret = false;  // even though it's not completed, we can't just exit cause we need to push the progress for
                          // each client
    }
    return ret;
}

bool ConsensusLog::allRPCCompleted(std::vector<std::shared_ptr<RPCToken>>& tokens) {
    for (auto& t : tokens) {
        if (!t->Complete()) return false;
    }
    return true;
}

bool ConsensusLog::allRPCCompleted(std::vector<RPCToken>& tokens) {
    for (auto& t : tokens) {
        if (!t.Complete()) return false;
    }
    return true;
}

}  // namespace lazylog
