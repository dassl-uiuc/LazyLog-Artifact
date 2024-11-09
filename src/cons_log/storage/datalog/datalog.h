#pragma once

#include <dirent.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>

#include "../../../rpc/circular_buffer.h"
#include "../../../rpc/common.h"
#include "../../../rpc/erpc_transport.h"
#include "datalog_client.h"
#include "glog/logging.h"

namespace lazylog {
class DataLog : public ERPCTransport {
   public:
    DataLog();
    ~DataLog();

    void InitializeConn(const Properties &p, const std::string &svr, void *param) override {
        LOG(ERROR) << "This is a server RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void Initialize(const Properties &p) override;
    void Finalize() override;

   protected:
    static void OrderBatchHandler(erpc::ReqHandle *req_handle, void *context);       // called from middle man
    static void ReplicateBatchHandler(erpc::ReqHandle *req_handle, void *context);   // called from shard primary
    static void AppendEntryHandler(erpc::ReqHandle *req_handle, void *context);      // called from clients
    static void UpdateGlobalIdxHandler(erpc::ReqHandle *req_handle, void *context);  // called from middle man
    static void ReadEntryHandler(erpc::ReqHandle *req_handle, void *context);        // called from clients
    static void GetReadMetadata(erpc::ReqHandle *req_handle, void *context);
    static size_t collectBatchEntries(std::pair<uint64_t, uint64_t> base_file_no, std::pair<uint64_t, uint64_t> offsets,
                                      uint8_t *buf);
    // called from clients
    static void ReadBatchHandler(erpc::ReqHandle *req_handle, void *context);  // called from clients

    static void writeToDisk(std::vector<LogEntry *> &entries);
    static void writeToDisk(std::vector<int> &gsn_added);
    static size_t readFromDisk(uint64_t base_idx, uint64_t offset, uint64_t size, uint8_t *buf);
    static size_t readFromDisk(uint64_t base_idx, uint64_t from_offset, uint8_t *buf);
    static int createNewDataFile();
    static int createNewGsnFile();
    static std::string getDataFilePath(uint64_t base_idx);
    static bool allRPCCompleted(std::vector<RPCToken> &tokens);
    static void mm_server_func(const Properties &p);
    static void append_entry_server_func(const Properties &p, int thread_id);
    static bool isMyRequest(LogEntry &e);
    static std::string getGsnListFilePath(uint64_t base_gsn);
    static uint64_t getBucket(uint64_t client_id);

   protected:
    static std::unordered_map<std::string, std::shared_ptr<DataLogClient>> backups_;
    static struct circular_buffer *per_client_log_[MAX_NUM_CLIENTS];
    static std::unordered_map<uint64_t, uint64_t> client_id_mapping_;
    static std::shared_mutex client_id_mapping_lock_;
    static uint64_t next_client_id_;
    static std::unordered_map<uint64_t, LogEntry *> entries_cache_;
    static std::unordered_map<uint64_t, std::pair<uint64_t, std::pair<uint64_t, uint64_t>>> gsn_offset_map_;
    static std::shared_mutex gsn_map_lock_;
    static std::shared_mutex gsn_list_lock_;
    static std::condition_variable_any read_cv_;
    static uint64_t replicated_index_;
    static uint64_t global_index_;
    static int shard_num_;
    static int shard_id_;
    static std::string folder_path_;
    static uint64_t active_file_index_;
    static uint64_t active_file_offset_;
    static uint64_t active_file_entries_;
    static uint64_t stripe_unit_size_;
    static int active_fd_;
    // large scratch buffer used for serializing and deserializing stuff
    static uint8_t *scratch_buf_;
    std::vector<std::thread> server_threads_;
    static std::vector<int> gsn_list_;
    static uint64_t gsn_file_index_;
    static uint64_t gsn_file_entries_;
    static uint64_t gsn_file_offset_;
    static int gsn_fd_;
    bool is_primary_;
};
}  // namespace lazylog
