#pragma once

#include <atomic>

namespace lazylog {

union Sequencer {
//    public:
    Sequencer();
    uint64_t GetNextSeq();
    uint64_t GetNextSeqNonAtm();
    uint64_t GetNextSeq(const uint64_t inc);
    uint64_t GetNextSeqNonAtm(const uint64_t inc);
    uint64_t GetCurSeq() const;
    uint64_t GetCurSeqNonAtm() const;
    void Reset();

//    protected:
    std::atomic<uint64_t> seq_;
    uint64_t seq_non_atomic_;
};

}  // namespace lazylog
