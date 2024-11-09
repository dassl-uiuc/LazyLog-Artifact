#include "dur_log_flat_cli.h"

#include "infinity/core/Configuration.h"
#include "infinity/requests/CombinedReqToken.h"

namespace lazylog {

DurabilityLogFlatCli::DurabilityLogFlatCli()
    : context_(nullptr),
      mr_(nullptr),
      remote_tk_(nullptr),
      qp_factory_(nullptr),
      qp_(nullptr),
      tkn_(nullptr),
      mr_offset_for_gc_(0),
      total_fetch_b_(0),
      total_fetch_n_(0) {}

DurabilityLogFlatCli::~DurabilityLogFlatCli() {}

void DurabilityLogFlatCli::InitializeConn(const Properties& p, const std::string& svr, void* param) {
    DurabilityLogERPCCli::InitializeConn(p, svr, param);
    if (!IsPrimary()) return;

    context_ = new core::Context(p.GetProperty("rdma.dev_name", ""), core::Configuration::DEFAULT_IB_PHY_PORT);
    qp_factory_ = new queues::QueuePairFactory(context_);

    // todo: may not need so much memory
    mr_ = new memory::Buffer(context_, std::stoi(p.GetProperty("dur_log.mr_size_m", "512")) * 1024 * 1024);
    mr_offset_for_gc_ = mr_->getSizeInBytes() - PAGE_SIZE;

    if (p.GetProperty("test", "false") == "false" && is_primary_) {
        std::string server_ip = SeparateValue(svr, ':')[0];
        qp_ = qp_factory_->connectToRemoteHost(server_ip.c_str(), std::stoi(p.GetProperty("rdma.ip_port_ctrl", "8011")));
        remote_tk_ = reinterpret_cast<memory::RegionToken*>(qp_->getUserData());
        LOG(INFO) << "RDMA queue pair connected";
    }

    tkn_ = new requests::RequestToken(context_);    
}

void DurabilityLogFlatCli::Finalize() {
    if (tkn_ != nullptr) delete tkn_;
    if (qp_ != nullptr) delete qp_;
    if (mr_ != nullptr) delete mr_;

    if (qp_factory_ != nullptr) delete qp_factory_;
    if (context_ != nullptr) delete context_;

    LOG(INFO) << "Average fetching size: " << total_fetch_b_ / static_cast<double>(total_fetch_n_) << "B";

    DurabilityLogERPCCli::Finalize();
}

std::pair<uint64_t, uint64_t> DurabilityLogFlatCli::getUnorderedRange() {
    requests::RequestToken token(context_);
    uint64_t rg_size = remote_tk_->getSizeInBytes() - SAFE_RG_SIZE - PAGE_SIZE;

    // read range begin and end
    qp_->read(mr_, 0, remote_tk_, rg_size + SAFE_RG_SIZE, 2 * sizeof(uint64_t), &token);

    token.waitUntilCompleted();

    uint64_t ordered_watermk = *reinterpret_cast<uint64_t*>(mr_->getAddress() + sizeof(uint64_t));  // range begin
    uint64_t end_sequence = *reinterpret_cast<uint64_t*>(mr_->getAddress());                        // range end

    return {ordered_watermk, end_sequence};
}


uint32_t DurabilityLogFlatCli::FetchUnorderedEntries(std::vector<LogEntry>& es, uint32_t max_fetch_size) {
    return FetchUnorderedEntries(es, 0, max_fetch_size);
}

uint32_t DurabilityLogFlatCli::FetchUnorderedEntries(std::vector<LogEntry>& es, uint64_t from, uint32_t max_fetch_size) {
    requests::RequestToken token(context_);
    uint64_t rg_size = remote_tk_->getSizeInBytes() - SAFE_RG_SIZE - PAGE_SIZE;

    auto range = getUnorderedRange();
    uint64_t begin_sequence = range.first;
    uint64_t end_sequence = range.second;

    // Must fetch within valid range
    if (from >= end_sequence) return 0;
    begin_sequence = std::max(begin_sequence, from);
    if (max_fetch_size > 0) end_sequence = std::min(end_sequence, begin_sequence + max_fetch_size);

    uint64_t begin_ofst = begin_sequence % rg_size;  // offset begin
    uint64_t end_ofst = end_sequence % rg_size;       // offset end

    uint64_t read_size = end_sequence - begin_sequence;

    if (begin_ofst < end_ofst) {
        DLOG(INFO) << "Reading range [" << begin_ofst << ", " << end_ofst << "], size " << read_size;

        token.reset();

        // read from range [begin, end]
        qp_->read(mr_, 0, remote_tk_, begin_ofst, read_size, &token);

        token.waitUntilCompleted();
    } else if (begin_ofst > end_ofst) {
        DLOG(INFO) << "Rewind area. Reading range [" << begin_ofst << ", " << rg_size << "] and [0, " << end_ofst << "]";

        requests::CombinedReqToken comb_tk(context_);

        uint64_t local_ofst[2] = {0, rg_size - begin_ofst};
        uint64_t remote_ofst[2] = {begin_ofst, 0};
        uint32_t sizes[2] = {static_cast<uint32_t>(rg_size - begin_ofst), static_cast<uint32_t>(end_ofst)};
        requests::RequestToken *tokens[2] = {&comb_tk.tk1, &comb_tk.tk2};

        qp_->readTwoPlaces(mr_, local_ofst, remote_tk_, remote_ofst, sizes, tokens);

        comb_tk.WaitUntilBothCompleted();
    } else {
        return 0;
    }

    auto fetch_size = MultiDeserializerMaxSize(es, reinterpret_cast<uint8_t*>(mr_->getData()), read_size);
    if (fetch_size == 0)
        return 0;

    total_fetch_b_ += fetch_size;
    total_fetch_n_++;

    es.emplace_back();
    es.back().client_seq = begin_sequence + fetch_size;  // we use this way to pass the end sequence out

    return fetch_size;
}

uint64_t DurabilityLogFlatCli::DeleteOrderedEntries(std::vector<LogEntry::ReqID>& seqs) {
    if (IsPrimary()) {
        requests::RequestToken token(context_);
        uint64_t rg_size = remote_tk_->getSizeInBytes() - SAFE_RG_SIZE - PAGE_SIZE;

        // GC and fetch should not use overlapped region in MR!
        *reinterpret_cast<uint64_t*>(mr_->getAddress() + mr_offset_for_gc_) = seqs[0].first;
        *reinterpret_cast<uint64_t*>(mr_->getAddress() + mr_offset_for_gc_ + sizeof(uint64_t)) = seqs[1].first;

        DLOG(INFO) << "Modify ordered watermark to " << *reinterpret_cast<uint64_t*>(mr_->getAddress());

        qp_->write(mr_, mr_offset_for_gc_, remote_tk_, rg_size + SAFE_RG_SIZE + sizeof(uint64_t), 2 * sizeof(uint64_t), &token);

        token.waitUntilCompleted();

        return seqs[0].first;
    } else {
        return DurabilityLogERPCCli::DeleteOrderedEntries(seqs);
    }
}

void DurabilityLogFlatCli::DeleteOrderedEntriesAsync(std::vector<LogEntry::ReqID>& seqs) {
    if (IsPrimary()) {
        uint64_t rg_size = remote_tk_->getSizeInBytes() - SAFE_RG_SIZE - PAGE_SIZE;

        // GC and fetch should not use overlapped region in MR!
        *reinterpret_cast<uint64_t*>(mr_->getAddress() + mr_offset_for_gc_) = seqs[0].first;
        *reinterpret_cast<uint64_t*>(mr_->getAddress() + mr_offset_for_gc_ + sizeof(uint64_t)) = seqs[1].first;

        DLOG(INFO) << "Modify ordered watermark to " << *reinterpret_cast<uint64_t*>(mr_->getAddress());

        tkn_->reset();
        qp_->write(mr_, mr_offset_for_gc_, remote_tk_, rg_size + SAFE_RG_SIZE + sizeof(uint64_t), 2 * sizeof(uint64_t), tkn_);
    } else {
        DurabilityLogERPCCli::DeleteOrderedEntriesAsync(seqs);
    }
}

uint64_t DurabilityLogFlatCli::ProcessFetchedEntries(const std::vector<LogEntry>& es,
                                                 std::vector<LogEntry::ReqID>& req_ids) {
    if (IsPrimary()) {
        uint64_t end_sequence = es.back().client_seq;
        req_ids.emplace_back(end_sequence, 0);
        uint64_t end_idx = (es.end() - 2)->log_idx + 1;
        req_ids.emplace_back(end_idx, 0);
    } else {
        std::map<uint64_t, uint64_t> maxSeqForClient;
        for (const auto& entry : es) {
            if (maxSeqForClient.find(entry.client_id) == maxSeqForClient.end() ||
                entry.client_seq > maxSeqForClient[entry.client_id]) {
                maxSeqForClient[entry.client_id] = entry.client_seq;
            }
        }
        maxSeqForClient.erase(0);
        for (const auto& [client_id, cli_seq] : maxSeqForClient) {
            req_ids.emplace_back(client_id, cli_seq);
        }
    }
    return (es.end() - 2)->log_idx;
}

bool DurabilityLogFlatCli::CheckAndRunOnce() {
    if (IsPrimary())
        return tkn_->checkIfCompleted();
    else
        return DurabilityLogERPCCli::CheckAndRunOnce();
}

}  // namespace lazylog
