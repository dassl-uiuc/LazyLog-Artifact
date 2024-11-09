#pragma once

#include <cppkafka/cppkafka.h>

#include "storage_backend.h"

namespace lazylog {

class KafkaBackend : public StorageBackend {
   public:
    KafkaBackend();

    virtual uint64_t AppendBatch(const std::vector<LogEntry> &es) override;

    void InitializeBackend(const Properties &p) override;
    void FinalizeBackend() override;
    bool ReadEntry(const uint64_t idx, LogEntry &e) { return false; }
    void UpdateGlobalIdx(const uint64_t idx) {}

   protected:
    cppkafka::Producer *producer_;
    int shard_num_;
    size_t stripe_unit_size_;
    std::string topic_;
    uint8_t buf_[8192];
};

}  // namespace lazylog
