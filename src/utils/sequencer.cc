#include "sequencer.h"

namespace lazylog {

Sequencer::Sequencer() : seq_(0) {}

uint64_t Sequencer::GetNextSeq() { return seq_.fetch_add(1); }

uint64_t Sequencer::GetNextSeqNonAtm() { return seq_non_atomic_++; }

uint64_t Sequencer::GetNextSeq(const uint64_t inc) { return seq_.fetch_add(inc); }

uint64_t Sequencer::GetNextSeqNonAtm(const uint64_t inc) {
    uint64_t prev = seq_non_atomic_;
    seq_non_atomic_ += inc;
    return prev;
}

uint64_t Sequencer::GetCurSeq() const { return seq_.load(); }

uint64_t Sequencer::GetCurSeqNonAtm() const { return seq_non_atomic_; }

void Sequencer::Reset() {
    seq_.store(0);
    seq_non_atomic_ = 0;
}

}  // namespace lazylog
