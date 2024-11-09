#include "dur_log.h"

namespace lazylog {

class DurabilityLogMock : public DurabilityLog {
   public:
    DurabilityLogMock();

    uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint32_t max_entries_num) override;
    uint32_t DelOrderedEntries(const std::vector<LogEntry::ReqID> &req_ids) override;

   protected:
    uint64_t mock_client_id_;
};

}  // namespace lazylog
