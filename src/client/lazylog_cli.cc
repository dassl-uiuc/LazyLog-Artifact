#include "lazylog_cli.h"

#include <x86intrin.h>

#include <chrono>

#include "../cons_log/cons_log_erpc_cli.h"
#include "../dur_log/dur_log_erpc_cli.h"
#include "../rpc/rpc_factory.h"

namespace lazylog {

std::atomic<uint8_t> LazyLogClient::global_th_id_ = {};
std::atomic<uint64_t> LazyLogClient::global_cli_id_ = 1;

using namespace std::chrono;

LazyLogClient::LazyLogClient() : client_id_(global_cli_id_.fetch_add(1)), maj_threshold_(0), finalized_(false) {}

void LazyLogClient::Initialize(const Properties &p) {
    std::vector<std::string> dl_servers = SeparateValue(p.GetProperty(PROP_DL_SVR_URI, PROP_DL_SVR_URI_DEFAULT), ',');
    dl_primary_ = p.GetProperty(PROP_DL_PRI_URI, PROP_DL_PRI_URI);  // todo: leader election for future work
    std::vector<std::string> shd_servers =
        SeparateValue(p.GetProperty(PROP_SHD_PRI_URI, PROP_SHD_PRI_URI_DEFAULT), ',');

    const std::string cli_ip = SeparateValue(p.GetProperty(PROP_DL_CLI_URI, PROP_DL_CLI_URI_DEFAULT), ':')[0];
    in_addr addr;
    inet_pton(AF_INET, cli_ip.c_str(), &addr);
    client_id_ += (ntohl(addr.s_addr) << 6);

    for (auto &s : dl_servers) {
        // dur_clis_[s] = std::dynamic_pointer_cast<DurabilityLogCli>(RPCFactory::CreateCliRPCTransport(p));
        dur_clis_[s] = std::make_shared<DurabilityLogERPCCli>();  // TODO: use dynamic type
        dur_clis_[s]->InitializeConn(p, s, nullptr);
    }

    bool is_user_provided_id = p.ContainsKey("dur_log.client_id");
    uint64_t thread_count = std::stoull(p.GetProperty("shard.threadcount", "1"));
    if (is_user_provided_id) {
        uint64_t id = std::stoull(p.GetProperty("dur_log.client_id"));
        be_rd_cli_ = std::make_shared<NaiveReadBackend>(id % thread_count);
    } else {
        be_rd_cli_ = std::make_shared<NaiveReadBackend>(global_th_id_.fetch_add(1) % thread_count);
    }
    be_rd_cli_->InitializeBackend(p);

#ifdef CORFU
    LOG(INFO) << "Connecting to backup shard...";
    be_rd_cli_backup_ = std::make_shared<NaiveReadBackend>(global_th_id_.fetch_add(1) % thread_count);
    be_rd_cli_backup_->InitializeBackendBackup(p);

    LOG(INFO) << "Connecting to 2nd backup shard...";
    be_rd_cli_backup_2_ = std::make_shared<NaiveReadBackend>(global_th_id_.fetch_add(1) % thread_count);
    be_rd_cli_backup_2_->InitializeBackendBackup(p, 2);
#endif

    // int f = (dl_servers.size() - 1) / 2;
    // maj_threshold_ = f + (f + 1) / 2 + 1;  // super majority: f + ceil(f/2) + 1
    maj_threshold_ = dl_servers.size();  // todo: calculate the real super majority
    LOG(INFO) << "party size: " << dl_servers.size() << ", quorum size: " << maj_threshold_;
}

void LazyLogClient::Finalize() {
    be_rd_cli_->FinalizeBackend();

    for (auto dc : dur_clis_) {
        dc.second->Finalize();
    }

    finalized_ = true;
}

LazyLogClient::~LazyLogClient() {
    if (!finalized_) Finalize();
}

std::pair<uint64_t, uint64_t> LazyLogClient::AppendEntry(const std::string &data) {
    LogEntry e = constructLogEntry(data);
    for (auto dc : dur_clis_) {
        if (!dc.second->AppendEntry(e)) {
            return std::make_pair(0, e.client_id);  // Todo: server identifier in return
        }
    }

    return std::make_pair(e.client_id, e.client_seq);
}

std::pair<uint64_t, uint64_t> LazyLogClient::AppendEntryQuorum(const std::string &data) {
    LogEntry e = constructLogEntry(data);
    std::vector<std::shared_ptr<RPCToken>> tokens;
    std::shared_ptr<RPCToken> pri_token;
    for (auto &dc : dur_clis_) {
        auto token = std::make_shared<RPCToken>();
        if (!dc.second->AppendEntryAsync(e, token)) {
            return std::make_pair(0, e.client_id);
        }
        if (dc.second->IsPrimary()) {
            pri_token = token;
        } else {
            tokens.emplace_back(token);
        }
        dc.second->AddPendingReq(token);
    }

    do {
        for (auto &dc : dur_clis_) {
            dc.second->CheckPendingReq();
        }
    } while (!quorumCompleted(pri_token, tokens));

    return std::make_pair(e.client_id, e.client_seq);
}

std::pair<uint64_t, uint64_t> LazyLogClient::AppendEntryAll(const std::string &data) {
#ifdef CORFU
    std::vector<LogEntry> es;
    LogEntry e = constructLogEntry(data);
    uint64_t gsn = dur_clis_[dl_primary_]->getGSN();
    e.log_idx = gsn;
    es.push_back(e);
    be_rd_cli_->AppendBatch(es);
    be_rd_cli_backup_->AppendBatch(es);
    be_rd_cli_backup_2_->AppendBatch(es);

    return std::make_pair(e.client_id, e.client_seq);
#else
    LogEntry e = constructLogEntry(data);
    std::vector<std::shared_ptr<RPCToken>> tokens;
    for (auto &dc : dur_clis_) {
        auto token = std::make_shared<RPCToken>();
        if (!dc.second->AppendEntryAsync(e, token)) {
            return std::make_pair(0, e.client_id);
        }
        tokens.emplace_back(token);
    }

    do {
        for (auto &dc : dur_clis_) {
            dc.second->RunERPCOnce();
        }
    } while (!allCompleted(tokens));

    return std::make_pair(e.client_id, e.client_seq);
#endif
}

uint64_t LazyLogClient::OrderEntry(const std::string &data) {
    LOG(WARNING) << "Unimplemented";
    return 0;
    // return dur_clis_[dl_primary_]->OrderEntry(constructLogEntry(data));
}

bool LazyLogClient::ReadEntry(const uint64_t idx, std::string &data) {
    LogEntry e;
    bool ret = be_rd_cli_->ReadEntry(idx, e);
    data = std::move(e.data);

    return ret;
}

bool LazyLogClient::ReadEntries(const uint64_t from, const uint64_t to, std::vector<LogEntry> &es) {
    LOG(WARNING) << "This function is not implemented";

    return false;
}

int LazyLogClient::SpecReadEntry(const uint64_t idx, std::string &data) {
    LogEntry e;
    int ret_v = dur_clis_[dl_primary_]->SpecRead(idx, e);
    if (ret_v > 0) data = std::move(e.data);

    return ret_v;
}

void LazyLogClient::doProgress() { ERPCTransport::RunERPCOnce(); }

std::tuple<uint64_t, uint64_t, uint16_t> LazyLogClient::GetTail() { return dur_clis_[dl_primary_]->GetNumDurEntry(); }

LogEntry LazyLogClient::constructLogEntry(const std::string &data) {
    LogEntry e;
    e.client_id = client_id_;
    e.client_seq = seq_.GetNextSeq();
    e.log_idx = 0;
    e.size = data.size();
    e.flags = 1;
    e.data = data;  // TODO: facilitate zero-copy
    return e;
}

bool LazyLogClient::quorumCompleted(std::shared_ptr<RPCToken> pri_token,
                                    std::vector<std::shared_ptr<RPCToken>> &tokens) {
    if (!pri_token->Complete()) return false;
    int n_complete = 0;
    for (auto &t : tokens) {
        if (t->Complete()) n_complete++;
    }

    return n_complete + 1 >= maj_threshold_;
}

bool LazyLogClient::allCompleted(std::vector<std::shared_ptr<RPCToken>> &tokens) {
    for (auto &t : tokens)
        if (!t->Complete()) return false;

    return true;
}

};  // namespace lazylog
