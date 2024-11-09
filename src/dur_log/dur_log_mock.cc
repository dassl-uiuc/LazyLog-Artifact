#include "dur_log_mock.h"

#include <chrono>

#include "glog/logging.h"

namespace lazylog {

using namespace std::chrono;

DurabilityLogMock::DurabilityLogMock()
    : mock_client_id_(duration_cast<microseconds>(system_clock::now().time_since_epoch()).count()) {
    LOG(INFO) << "This is a mock durability log, only for test use";
}

uint32_t DurabilityLogMock::FetchUnorderedEntries(std::vector<LogEntry>& es, uint32_t max_entries_num) {
    for (int i = 0; i < max_entries_num; i++) {
        LogEntry e;
        e.client_id = mock_client_id_;
        e.client_seq = sequencer_->GetNextSeq();
        e.data = "This is a mock data";
        e.size = e.data.size();
        es.emplace_back(e);
    }

    return max_entries_num;
}

uint32_t DurabilityLogMock::DelOrderedEntries(const std::vector<LogEntry::ReqID>& req_ids) { return req_ids.size(); }

}  // namespace lazylog
