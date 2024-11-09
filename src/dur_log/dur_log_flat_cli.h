#include "../rpc/rpc_token.h"
#include "dur_log_erpc_cli.h"
#include "infinity/core/Context.h"
#include "infinity/memory/Buffer.h"
#include "infinity/queues/QueuePair.h"
#include "infinity/queues/QueuePairFactory.h"
#include "infinity/requests/RequestToken.h"

namespace lazylog {

using namespace infinity;

class DurabilityLogFlatCli : public DurabilityLogERPCCli {
   public:
    DurabilityLogFlatCli();
    ~DurabilityLogFlatCli() override;

    void InitializeConn(const Properties &p, const std::string &svr, void *param) override;
    void Finalize() override;

    uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint64_t from, uint32_t max_fetch_size) override;
    uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint32_t max_fetch_size) override;
    uint64_t DeleteOrderedEntries(std::vector<LogEntry::ReqID> &seqs) override;
    void DeleteOrderedEntriesAsync(std::vector<LogEntry::ReqID> &seqs) override;
    uint64_t ProcessFetchedEntries(const std::vector<LogEntry> &es, std::vector<LogEntry::ReqID> &req_ids) override;
    bool CheckAndRunOnce() override;

   protected:
    std::pair<uint64_t, uint64_t> getUnorderedRange();

   protected:
    core::Context *context_;
    memory::Buffer *mr_;
    memory::RegionToken *remote_tk_;
    queues::QueuePairFactory *qp_factory_;
    queues::QueuePair *qp_;
    requests::RequestToken *tkn_;
    uint64_t mr_offset_for_gc_;

    uint64_t total_fetch_b_;
    uint64_t total_fetch_n_;
};

}  // namespace lazylog
