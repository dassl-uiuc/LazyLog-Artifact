#pragma once

#include "../../rpc/log_entry.h"
#include "../../utils/properties.h"

namespace lazylog {

class StorageBackend {
   public:
    virtual uint64_t AppendBatch(const std::vector<LogEntry> &es) = 0;
    virtual bool AppendBatch(const uint8_t *buf, size_t sz) = delete;  // current not used
    virtual bool ReadEntry(const uint64_t idx, LogEntry &e) = 0;
    virtual void UpdateGlobalIdx(const uint64_t idx) = 0;

    // virtual void InitializeBackend(const Properties &p, bool for_reads = false) = 0;
    virtual void InitializeBackend(const Properties &p) = 0;
    virtual void FinalizeBackend() = 0;
};

}  // namespace lazylog
