#pragma once

#include <atomic>
#include <memory>
#include <unordered_map>
#include <utility>

#include "../cons_log/cons_log_cli.h"
#include "../cons_log/storage/naive_backend.h"
#include "../dur_log/dur_log_cli.h"
#include "../rpc/common.h"
#include "../utils/properties.h"
#include "../utils/sequencer.h"

namespace lazylog {

/**
 * This need to be a singleton
 */
class LazyLogClient {
   public:
    LazyLogClient();
    virtual ~LazyLogClient();

    virtual void Initialize(const Properties &p);

    std::pair<uint64_t, uint64_t> AppendEntry(const std::string &data);
    std::pair<uint64_t, uint64_t> AppendEntryQuorum(const std::string &data);
    virtual std::pair<uint64_t, uint64_t> AppendEntryAll(const std::string &data);
    uint64_t OrderEntry(const std::string &data);
    virtual bool ReadEntry(const uint64_t idx, std::string &data);
    virtual bool ReadEntries(const uint64_t from, const uint64_t to, std::vector<LogEntry> &es);
    /**
     * @return There are 4 possible kinds of values:
     *  1. >0: a valid entry is read
     *  2. =0: the idx is within a valid range, but entry is invalid (unlikely)
     *  3. -1: the idx is smaller than the minimum unordered idx (entry has been GCed)
     *  4. -2: the idx exceeds the max idx on DL
     */
    int SpecReadEntry(const uint64_t idx, std::string &data);
    /**
     * @return 0. durable tail: index of the newest unordered entry + 1,
     *  1. ordered tail: index of the oldest unordered entry (i.e. index of the newest ordered entry + 1)
     *  2. curent view number
     * What you should fetch is within [ tail[1], tail[0] )
     */
    std::tuple<uint64_t, uint64_t, uint16_t> GetTail();

   protected:
    LogEntry constructLogEntry(const std::string &data);
    bool quorumCompleted(std::shared_ptr<RPCToken> pri_token, std::vector<std::shared_ptr<RPCToken> > &tokens);
    bool allCompleted(std::vector<std::shared_ptr<RPCToken> > &tokens);

   protected:
    // std::shared_ptr<ConsensusLogCli> cons_cli_;  // for indirect read, currently not used
    std::unordered_map<std::string, std::shared_ptr<DurabilityLogCli> > dur_clis_;
    std::shared_ptr<NaiveReadBackend> be_rd_cli_;  // for direct read

    std::string dl_primary_;

    uint64_t client_id_;
    Sequencer seq_;
    int maj_threshold_;

    static std::atomic<uint8_t> global_th_id_;
    static std::atomic<uint64_t> global_cli_id_;
};

}  // namespace lazylog
