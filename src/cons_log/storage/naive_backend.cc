#include "naive_backend.h"

#include "../../rpc/common.h"

namespace lazylog {

NaiveBackend::NaiveBackend() : stripe_unit_size_(0), shard_num_(0) {}

NaiveBackend::~NaiveBackend() {}

void NaiveBackend::InitializeBackend(const Properties& p) {
    stripe_unit_size_ = std::stoi(p.GetProperty(PROP_SHD_STRIPE_SIZE, PROP_SHD_STRIPE_SIZE_DEFAULT));
    std::vector<std::string> shd_servers =
        SeparateValue(p.GetProperty(PROP_SHD_PRI_URI, PROP_SHD_PRI_URI_DEFAULT), ',');
    shard_num_ = std::stoll(p.GetProperty("shard.num"));
    for (int i = 0; i < shard_num_; i++) {
        shard_clients_[i] = std::make_shared<ShardClient>();
        shard_clients_[i]->InitializeConn(p, shd_servers[i], nullptr);
    }
}

void NaiveBackend::FinalizeBackend() {
    for (auto& c : shard_clients_) c.second->Finalize();
}

uint64_t NaiveBackend::AppendBatch(const std::vector<LogEntry>& es) {
    // TODO: we don't want to do multiple rounds of serialize&deserialize
    auto start_idx = es.begin()->log_idx;
    auto end_idx = es.back().log_idx;
    if (es.size() > 1 && end_idx == 0) end_idx = (es.end() - 2)->log_idx;
    // |............+-+-+-+-|+-+-+-+-+-+-+-+-+-+-|+-+-+-+-+-+-+-+-+-+-|+-+-+-.............|
    // first_base_idx       <big_stripe_unit_size>                    last_base_idx
    // shard 0:     + + + + |+ + + + + + + + + + |+ + + + + + + + + + |+ + +
    //                      <  stripe_unit_size  >
    // shard 1:      - - - -| - - - - - - - - - -| - - - - - - - - - -| - - -
    uint64_t big_stripe_unit_size_ = stripe_unit_size_ * shard_num_;
    uint64_t first_base_idx = start_idx / big_stripe_unit_size_ * big_stripe_unit_size_;
    uint64_t last_base_idx = end_idx / big_stripe_unit_size_ * big_stripe_unit_size_;

    std::vector<RPCToken> tokens;
    tokens.reserve(shard_num_);  // This is to ensure elements doesn't move when emplacing

    // The max message size for eRPC is 8MB so we need to make sure every stripe unit is less than that
    // TODO: when stripe unit is greater than 8MB, split into multiple rpc
    for (uint64_t i = first_base_idx; i <= last_base_idx; i += big_stripe_unit_size_) {
        uint64_t from = i;
        uint64_t to = i + big_stripe_unit_size_ - 1;
        if (from < start_idx) from = start_idx;
        if (to > end_idx) to = end_idx;

        for (uint64_t s = 0; s < shard_num_; s++) {
            uint64_t stripe_from = from + s;
            if (stripe_from > to) break;
            uint64_t shard_id = stripe_from % shard_num_;

            tokens.emplace_back();
            uint64_t stripe_to = shard_clients_[shard_id]->AppendBatchAsync(es, stripe_from - start_idx, to - start_idx,
                                                                            shard_num_, tokens.back()) +
                                 start_idx;
            shard_clients_[shard_id]->RunERPCOnce();
            DLOG(INFO) << "Sending entries [" << stripe_from << ", " << stripe_to << "] to shard " << shard_id;
        }
        waitForAllShards(tokens);
        tokens.clear();
    }

    return end_idx;
}

bool NaiveBackend::ReadEntry(const uint64_t idx, LogEntry& e) {
    int shard_id = idx % shard_num_;
    shard_clients_[shard_id]->ReadEntry(idx, e);

    return true;
}

void NaiveBackend::UpdateGlobalIdx(const uint64_t idx) {
    std::vector<RPCToken> tokens;
    tokens.reserve(shard_num_);  // This is to ensure elements doesn't move when emplacing
    for (auto& sc : shard_clients_) {
        tokens.emplace_back();
        sc.second->UpdateGlobalIdxAsync(idx, tokens.back());
    }
    waitForAllShards(tokens);
}

bool NaiveBackend::allRPCCompleted(std::vector<RPCToken>& tokens) {
    for (auto& t : tokens) {
        if (!t.Complete()) return false;
    }
    return true;
}

void NaiveBackend::waitForAllShards(std::vector<RPCToken>& tokens) {
    while (!allRPCCompleted(tokens)) {
        ShardClient::RunERPCOnce();
    }
}

NaiveReadBackend::NaiveReadBackend(int thread_id)
    : stripe_unit_size_(0), shard_num_(0), thread_id_(thread_id + CL_CLI_RPCID_OFFSET) {}

NaiveReadBackend::~NaiveReadBackend() {}

void NaiveReadBackend::InitializeBackend(const Properties& p) {
    stripe_unit_size_ = std::stoi(p.GetProperty(PROP_SHD_STRIPE_SIZE, PROP_SHD_STRIPE_SIZE_DEFAULT));
    std::vector<std::string> shd_servers =
        SeparateValue(p.GetProperty(PROP_SHD_PRI_URI, PROP_SHD_PRI_URI_DEFAULT), ',');
    shard_num_ = std::stoll(p.GetProperty("shard.num"));
#ifdef CORFU
    std::vector<std::string> id_offsets = SeparateValue(p.GetProperty("shard.id_offset", "0"), ',');
    for (int i = 0; i < shard_num_; i++) {
        shard_clients_[i] = std::make_shared<ShardClient>();
        shard_clients_[i]->SetRemoteIdOffset(std::stoi(id_offsets[i]));
        shard_clients_[i]->InitializeConn(p, shd_servers[i], static_cast<void*>(&thread_id_));
    }
#else
    for (int i = 0; i < shard_num_; i++) {
        shard_clients_[i] = std::make_shared<ShardClient>();
        shard_clients_[i]->InitializeConn(p, shd_servers[i], static_cast<void*>(&thread_id_));
    }
#endif
}

#ifdef CORFU
void NaiveReadBackend::InitializeBackendBackup(const Properties& p, int idx) {
    stripe_unit_size_ = std::stoi(p.GetProperty(PROP_SHD_STRIPE_SIZE, PROP_SHD_STRIPE_SIZE_DEFAULT));
    std::vector<std::string> shd_servers;
    if (idx == 1)
        shd_servers = SeparateValue(p.GetProperty(PROP_SHD_BACKUP_URI, PROP_SHD_BACKUP_URI_DEFAULT), ',');
    else
        shd_servers = SeparateValue(p.GetProperty(PROP_SHD_BACKUP_URI_2, PROP_SHD_BACKUP_URI_DEFAULT), ',');
    std::vector<std::string> id_offsets = SeparateValue(p.GetProperty("shard.id_offset", "0"), ',');
    shard_num_ = std::stoi(p.GetProperty("shard.num", "1"));
    for (int i = 0; i < shard_num_; i++) {
        shard_clients_[i] = std::make_shared<ShardClient>();
        shard_clients_[i]->SetRemoteIdOffset(std::stoi(id_offsets[i]));
        shard_clients_[i]->InitializeConn(p, shd_servers[i], static_cast<void*>(&thread_id_));
    }
}
#endif

void NaiveReadBackend::FinalizeBackend() {
    for (auto& c : shard_clients_) c.second->Finalize();
}

bool NaiveReadBackend::allRPCCompleted(std::vector<RPCToken>& tokens) {
    for (auto& t : tokens) {
        if (!t.Complete()) return false;
    }
    return true;
}

void NaiveReadBackend::waitForAllShards(std::vector<RPCToken>& tokens) {
    while (!allRPCCompleted(tokens)) {
        ShardClient::RunERPCOnce();
    }
}

bool NaiveReadBackend::ReadEntry(const uint64_t idx, LogEntry& e) {
    uint64_t shard_id = idx % shard_num_;
    shard_clients_[shard_id]->ReadEntry(idx, e);

    return true;
}

void NaiveReadBackend::UpdateGlobalIdx(const uint64_t idx) {}
#ifdef CORFU
uint64_t NaiveReadBackend::AppendBatch(const std::vector<LogEntry>& es) {
    LogEntry e = es[0];
    uint32_t shard_id = e.log_idx % shard_num_;
    shard_clients_[shard_id]->AppendEntry(e);
    return 0;
}
#else
uint64_t NaiveReadBackend::AppendBatch(const std::vector<LogEntry>& es) { return false; }
#endif

}  // namespace lazylog
