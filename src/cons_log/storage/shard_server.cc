#include "shard_server.h"

#include "../../rpc/common.h"
#include "sys/mman.h"

namespace lazylog {

std::unordered_map<std::string, std::shared_ptr<ShardClient>> ShardServer::backups_;
std::unordered_map<uint64_t, std::vector<LogEntry>> ShardServer::entries_cache_set_;
std::map<uint64_t, int> ShardServer::entries_fd_set_;
std::unordered_map<uint64_t, size_t> ShardServer::cache_size_;
std::shared_mutex ShardServer::cache_rw_lock_;
std::condition_variable_any ShardServer::cache_write_cv_;
ShardServerMetrics ShardServer::metrics_ = {};
size_t ShardServer::stripe_unit_size_ = 0;
std::string ShardServer::folder_path_ = "";
uint64_t ShardServer::global_index_ = 0;
uint64_t ShardServer::replicated_index_ = 0;
int ShardServer::shard_num_ = 0;
int ShardServer::shard_id_ = 0;
bool ShardServer::terminate_ = false;
#ifdef CORFU
uint64_t ShardServer::entry_size_ = 0;
int ShardServer::entry_fd_ = -1;
std::unordered_map<uint64_t, std::atomic<int>> ShardServer::cache_size_atomic_;
#endif

void svr_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

ShardServer::ShardServer() : is_primary_(false) {}

std::ostream &operator<<(std::ostream &out, ShardServerMetrics const &metrics) {
    out << "metrics: " << std::endl
        << "\tnum_slow_path_reads: " << metrics.num_slow_path_reads << std::endl
        << "\tnum_fast_path_reads: " << metrics.num_fast_path_reads << std::endl;
    return out;
}

ShardServer::~ShardServer() {
    if (is_primary_) {
        std::cout << metrics_;
    }
}

void ShardServer::backgroundFsync() {
    uint64_t latest_base_idx = 0;
    uint64_t last_synced_base_idx = 0;
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;

    while (run_) {
        int latest_fd;
        {
            std::unique_lock<std::shared_mutex> lock(cache_rw_lock_);
            if (entries_fd_set_.empty()) continue;
            latest_fd = entries_fd_set_.rbegin()->second;
        }
        fsync(latest_fd);
        usleep(1000);
    }
}

void ShardServer::Initialize(const Properties &p) {
    const std::string server_uri = p.GetProperty(PROP_SHD_SVR_URI, PROP_SHD_SVR_URI_DEFAULT);
    nexus_ = new erpc::Nexus(server_uri, 0, 0);

    stripe_unit_size_ = std::stoi(p.GetProperty(PROP_SHD_STRIPE_SIZE, PROP_SHD_STRIPE_SIZE_DEFAULT));
    shard_num_ = std::stoi(p.GetProperty("shard.num", "1"));
    shard_id_ = std::stoi(p.GetProperty("shard.id", "0"));
    folder_path_ = p.GetProperty(PROP_SHD_FOLDER_PATH, PROP_SHD_FOLDER_PATH_DEFAULT);
    replicated_index_ = std::stoull(p.GetProperty("shard.replicated_index", "0"));
    global_index_ = std::stoull(p.GetProperty("shard.global_index", "0"));

    struct stat info;
    if (::stat(folder_path_.c_str(), &info) != 0 || !S_ISDIR(info.st_mode)) {
        if (::mkdir(folder_path_.c_str(), 0777) != 0) {
            LOG(ERROR) << "Can't make directory, error: " << strerror(errno);
            throw Exception("Can't make directory");
        }
    }

    nexus_->register_req_func(APPEND_BATCH, AppendBatchHandler);
    nexus_->register_req_func(REP_BATCH, ReplicateBatchHandler);
    nexus_->register_req_func(READ_ENTRY_BE, ReadEntryHandler);
    nexus_->register_req_func(UPDATE_GLBL_IDX, UpdateGlobalIdxHandler);
#ifdef CORFU
    nexus_->register_req_func(APPEND_ENTRY_BE, AppendEntryHandler);
#endif

#ifndef CORFU
    if (p.GetProperty("leader", "false") != "true") {
        LOG(INFO) << "Not a shard server leader";
        // fsync_thread_ = std::move(std::thread(ShardServer::backgroundFsync));
        ShardServer::server_func(p);
        return;
    }
    is_primary_ = true;
    LOG(INFO) << "This is shard server leader";

    const std::vector<std::string> backup_uri =
        SeparateValue(p.GetProperty(PROP_SHD_BACKUP_URI, PROP_SHD_BACKUP_URI_DEFAULT), ',');
    for (auto &b : backup_uri) {
        backups_[b] = std::make_shared<ShardClient>();
        backups_[b]->InitializeConn(p, b, nullptr);
    }
#endif

    // start n threads to handle client reads
    const int n_th = std::stoi(p.GetProperty("threadcount", "1"));
#ifdef CORFU
    const int offset = std::stoi(p.GetProperty("offset", "0"));
#endif
    for (int i = 0; i < n_th; i++) {
#ifdef CORFU
        server_threads_.emplace_back(std::move(std::thread(ShardServer::read_server_func, p, i + offset)));
#else
        server_threads_.emplace_back(std::move(std::thread(ShardServer::read_server_func, p, i)));
#endif
    }

#ifdef CORFU
    fsync_thread_ = std::move(std::thread(ShardServer::backgroundFsync));
#else
    ShardServer::server_func(p);
#endif
}

void ShardServer::Finalize() {
    terminate_ = true;
#ifdef CORFU
    fsync_thread_.join();
#endif
    for (auto &t : server_threads_) t.join();
    for (auto &b : backups_) b.second->Finalize();
    delete nexus_;
}

void ShardServer::server_func(const Properties &p) {
    const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));

    if (!rpc_) rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, nullptr, 0, svr_sm_handler, phy_port);
    rpc_use_cnt_.fetch_add(1);

    while (run_) {
        rpc_->run_event_loop(1000);
    }

    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
}

void ShardServer::read_server_func(const Properties &p, int th_id) {
    const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));

    if (!rpc_)
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, nullptr, CL_CLI_RPCID_OFFSET + th_id, svr_sm_handler,
                                               phy_port);  // start from 64
    rpc_use_cnt_.fetch_add(1);

    const size_t msg_size = std::stoull(p.GetProperty(PROP_DL_MSG_SIZE, PROP_DL_MSG_SIZE_DEFAULT));
    rpc_->set_pre_resp_msgbuf_size(msg_size);

    while (run_) {
        rpc_->run_event_loop(1000);
    }

    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
}

void ShardServer::AppendBatchHandler(erpc::ReqHandle *req_handle, void *_context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    auto num = *reinterpret_cast<uint32_t *>(req->buf_);
    LogEntry first_e_in_batch;
    Deserializer(first_e_in_batch, req->buf_ + sizeof(uint32_t));
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;
    uint64_t base_idx = first_e_in_batch.log_idx / big_stripe_unit_size * big_stripe_unit_size;

    std::vector<RPCToken> tokens;
    tokens.reserve(backups_.size());
    for (auto &b : backups_) {
        tokens.emplace_back();
        b.second->ReplicateBatchAsync(req->buf_, req->get_data_size(), tokens.back());
        RunERPCOnce();
    }

    {
        std::unique_lock<std::shared_mutex> write_lock(cache_rw_lock_);
        addToEntryCache(base_idx, req->buf_);
        if (entries_cache_set_[base_idx].size() >= stripe_unit_size_) writeFromCacheToDisk(base_idx);
    }

    while (!allRPCCompleted(tokens)) {
        RunERPCOnce();
    }

    {
        std::unique_lock<std::shared_mutex> write_lock(cache_rw_lock_);
        replicated_index_ = entries_cache_set_[base_idx].back().log_idx;
    }
    // wake up all threads that are waiting for a cache write to update metadata
    cache_write_cv_.notify_all();

    rpc_->resize_msg_buffer(&resp, sizeof(int));
    *reinterpret_cast<int *>(resp.buf_) = 0;
    rpc_->enqueue_response(req_handle, &resp);
}

#ifdef CORFU
void ShardServer::AppendEntryHandler(erpc::ReqHandle *req_handle, void *_context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    LogEntry e;
    Deserializer(e, req->buf_);
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;
    uint64_t base_idx = e.log_idx / big_stripe_unit_size * big_stripe_unit_size;

    addToEntryCacheAsync(base_idx, req->buf_);
    if (cache_size_atomic_[base_idx].load() >= stripe_unit_size_) writeFromCacheToDisk(base_idx);

    rpc_->resize_msg_buffer(&resp, sizeof(int));
    *reinterpret_cast<int *>(resp.buf_) = 0;
    rpc_->enqueue_response(req_handle, &resp);
}
#endif

void ShardServer::ReplicateBatchHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    auto num = *reinterpret_cast<uint32_t *>(req->buf_);
    LogEntry first_e_in_batch;
    Deserializer(first_e_in_batch, req->buf_ + sizeof(uint32_t));
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;
    uint64_t base_idx = first_e_in_batch.log_idx / big_stripe_unit_size * big_stripe_unit_size;

    addToEntryCache(base_idx, req->buf_);

    *reinterpret_cast<Status *>(resp.buf_) = Status::OK;
    if (entries_cache_set_[base_idx].size() >= stripe_unit_size_) {
        if (writeFromCacheToDisk(base_idx) < 0) *reinterpret_cast<Status *>(resp.buf_) = Status::ERROR;
    }

    rpc_->resize_msg_buffer(&resp, sizeof(Status));
    rpc_->enqueue_response(req_handle, &resp);
}

void ShardServer::ReadEntryHandler(erpc::ReqHandle *req_handle, void *context) {
#ifdef CORFU
    const auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    auto idx = *reinterpret_cast<uint64_t *>(req->buf_);
    bool wait = *reinterpret_cast<uint64_t *>(req->buf_ + sizeof(uint64_t));
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;
    uint64_t base_idx = idx / big_stripe_unit_size * big_stripe_unit_size;
    uint64_t local_cache_idx = (idx - base_idx) / shard_num_;
    size_t len = 0;
    int fd = 0;
    uint8_t *buf;

    {
        std::shared_lock<std::shared_mutex> read_lock(cache_rw_lock_);
        if (entries_fd_set_.find(base_idx) == entries_fd_set_.end()) {
            // entries in closed files or non-existent files
            read_lock.unlock();

            fd = open(getDataFilePath(base_idx).c_str(), O_RDONLY);
            if (fd < 0) {
                *reinterpret_cast<Status *>(resp.buf_) = Status::NOENT;
                rpc_->resize_msg_buffer(&resp, sizeof(Status));
                rpc_->enqueue_response(req_handle, &resp);
                return;
            }

            if (pread(fd, resp.buf_, entry_size_, entry_size_ * local_cache_idx) != entry_size_) {
                LOG(WARNING) << "Reading less bytes than expected";
            }

            close(fd);
        } else {
            // entry might be in an unfinished file
            fd = entries_fd_set_[base_idx];
            if (pread(fd, resp.buf_, entry_size_, entry_size_ * local_cache_idx) != entry_size_) {
                LOG(WARNING) << "Reading less bytes than expected";
            }
            read_lock.unlock();
        }
    }

    LogEntry e;
    len = Deserializer(e, resp.buf_);
    if (e.flags != 1) {
        *reinterpret_cast<Status *>(resp.buf_) = Status::NOENT;
        rpc_->resize_msg_buffer(&resp, sizeof(Status));
        rpc_->enqueue_response(req_handle, &resp);
        return;
    }

    // safe to read
    rpc_->resize_msg_buffer(&resp, len);
    rpc_->enqueue_response(req_handle, &resp);
#else
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    auto idx = *reinterpret_cast<uint64_t *>(req->buf_);
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;
    uint64_t base_idx = idx / big_stripe_unit_size * big_stripe_unit_size;
    uint64_t local_cache_idx = (idx - base_idx) / shard_num_;
    size_t len = 0;
    bool slow_path_exercized = false;
    {
        std::shared_lock<std::shared_mutex> read_lock(cache_rw_lock_);
        while (1) {
            if (entries_cache_set_.find(base_idx) == entries_cache_set_.end()) {
                read_lock.unlock();
                bool loaded_by_me = false;
                bool loaded = false;
                {
                    // upgrade to a write lock.
                    // try loading the cache
                    std::unique_lock<std::shared_mutex> write_lock(cache_rw_lock_);
                    // need to check again as another thread might have loaded the cache
                    if (entries_cache_set_.find(base_idx) == entries_cache_set_.end()) {
                        loaded_by_me = loaded = loadFromDiskToCache(base_idx) == 0;
                    } else {
                        loaded = true;
                        loaded_by_me = false;
                    }
                }
                if (loaded_by_me) {
                    // notify others that the cache is loaded
                    cache_write_cv_.notify_all();
                }
                read_lock.lock();
                // check again as condition might have changed yet again from another thread
                if (!loaded && entries_cache_set_.find(base_idx) == entries_cache_set_.end()) {
                    cache_write_cv_.wait(read_lock);
                    slow_path_exercized = true;
                    continue;
                }
            }

            // at this point the cache must be loaded
            if (local_cache_idx >= entries_cache_set_[base_idx].size() || idx > replicated_index_ ||
                idx > global_index_) {
                slow_path_exercized = true;
                cache_write_cv_.wait(read_lock);
                continue;
            }

            // safe to read
            len = Serializer(entries_cache_set_[base_idx][local_cache_idx], resp.buf_);
            break;
        }
    }
    if (slow_path_exercized) {
        metrics_.num_slow_path_reads++;
    } else {
        metrics_.num_fast_path_reads++;
    }
    rpc_->resize_msg_buffer(&resp, len);
    rpc_->enqueue_response(req_handle, &resp);
#endif
}

#ifdef CORFU
size_t ShardServer::collectBatchEntries(const uint64_t start_idx, const uint64_t end_idx, uint8_t *buf) {
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;

    uint64_t end_base_idx = end_idx / big_stripe_unit_size * big_stripe_unit_size;
    uint64_t start_base_idx = start_idx / big_stripe_unit_size * big_stripe_unit_size;

    uint64_t offset = sizeof(uint32_t);
    uint64_t num_entries = 0;
    for (uint64_t i = start_base_idx; i <= end_base_idx; i += big_stripe_unit_size) {
        uint64_t from = i;
        uint64_t to = i + big_stripe_unit_size - 1;
        if (from < start_idx) from = start_idx;
        if (to > end_idx) to = end_idx;

        auto from_local_idx = (from - i) / shard_num_;
        auto to_local_idx = (to - i) / shard_num_;
        auto num = to_local_idx - from_local_idx + 1;
        num_entries += num;

        offset += MultiSerializer(entries_cache_set_[i], from_local_idx, num, buf + offset, true);
    }

    // populate total number of entries in the beginning
    *reinterpret_cast<uint32_t *>(buf) = num_entries;
    return offset;
}

void ShardServer::ReadBatchHandler(erpc::ReqHandle *req_handle, void *context) {
    const auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    // wait for the last index to complete
    auto start_idx = *reinterpret_cast<uint64_t *>(req->buf_);
    auto end_idx = *reinterpret_cast<uint64_t *>(req->buf_ + sizeof(uint64_t));
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;
    uint64_t base_idx = end_idx / big_stripe_unit_size * big_stripe_unit_size;
    uint64_t local_cache_idx = (end_idx - base_idx) / shard_num_;
    size_t len = 0;
    bool slow_path_exercized = false;
    {
        std::shared_lock<std::shared_mutex> read_lock(cache_rw_lock_);
        while (1) {
            if (entries_cache_set_.find(base_idx) == entries_cache_set_.end()) {
                read_lock.unlock();
                bool loaded_by_me = false;
                bool loaded = false;
                {
                    // upgrade to a write lock.
                    // try loading the cache
                    std::unique_lock<std::shared_mutex> write_lock(cache_rw_lock_);
                    // need to check again as another thread might have loaded the cache
                    if (entries_cache_set_.find(base_idx) == entries_cache_set_.end()) {
                        loaded_by_me = loaded = loadFromDiskToCache(base_idx) == 0;
                    } else {
                        loaded = true;
                        loaded_by_me = false;
                    }
                }
                if (loaded_by_me) {
                    // notify others that the cache is loaded
                    cache_write_cv_.notify_all();
                }
                read_lock.lock();
                // check again as condition might have changed yet again from another thread
                if (!loaded && entries_cache_set_.find(base_idx) == entries_cache_set_.end()) {
                    cache_write_cv_.wait(read_lock);
                    slow_path_exercized = true;
                    continue;
                }
            }

            // at this point the cache must be loaded
            if (local_cache_idx >= entries_cache_set_[base_idx].size() || end_idx > replicated_index_) {
                slow_path_exercized = true;
                cache_write_cv_.wait(read_lock);
                continue;
            }

            len = collectBatchEntries(start_idx, end_idx, resp.buf_);
            break;
        }
    }
    if (slow_path_exercized) {
        metrics_.num_slow_path_reads++;
    } else {
        metrics_.num_fast_path_reads++;
    }
    // safe to read
    rpc_->resize_msg_buffer(&resp, len);
    rpc_->enqueue_response(req_handle, &resp);
}
#endif

void ShardServer::UpdateGlobalIdxHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    uint64_t global_idx = *reinterpret_cast<uint64_t *>(req->buf_);

    {
        std::unique_lock<std::shared_mutex> lock(cache_rw_lock_);
        global_index_ = global_idx;
    }
    // wake up all threads that are waiting for a cache write to update metadata
    cache_write_cv_.notify_all();

    *reinterpret_cast<int *>(resp.buf_) = 0;
    rpc_->resize_msg_buffer(&resp, sizeof(int));
    rpc_->enqueue_response(req_handle, &resp);
}

void ShardServer::addToEntryCache(uint64_t base_idx, const uint8_t *buf) {
    // here we assume the rpc only sends entries for the same stripe unit, which is enforced on CL
    int fd = 0;
    if (entries_cache_set_.find(base_idx) == entries_cache_set_.end()) {
        // not in cache but could be on disk
        if (loadFromDiskToCache(base_idx) < 0) {
            // not even on disk, have to create file and data structures
            entries_cache_set_[base_idx] = {};
            cache_size_[base_idx] = 0;
            fd = open(getDataFilePath(base_idx).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
            if (fd < 0) {
                LOG(ERROR) << "Can't open file for write of base idx " << base_idx;
                return;
            }
            entries_fd_set_[base_idx] = fd;
        }
    } else {
        // entries in cache, get fd for file
        fd = entries_fd_set_[base_idx];
    }

    size_t batch_len =
        MultiDeserializer(entries_cache_set_[base_idx], buf) - sizeof(uint32_t);  // does not contain the leading size
    cache_size_[base_idx] += batch_len;
    LOG(INFO) << "cache " << base_idx << ": " << entries_cache_set_[base_idx].size() << " entries, "
              << cache_size_[base_idx] << "B in total";

    if (write(fd, buf + sizeof(uint32_t), batch_len) != batch_len) {
        LOG(WARNING) << "Writing less bytes than expected";
    }
}

#ifdef CORFU
void ShardServer::addToEntryCacheAsync(uint64_t base_idx, const uint8_t *buf) {
    int fd = 0;
    int len = 0;

    // lock upgrade
    std::shared_lock<std::shared_mutex> read_lock(cache_rw_lock_);
    if (entries_fd_set_.find(base_idx) == entries_fd_set_.end()) {
        // file not created (will never close a file before it is full)
        // entries_cache_set_ onyl contains entries that are read back from a closed file
        read_lock.unlock();

        {
            std::unique_lock<std::shared_mutex> write_lock(cache_rw_lock_);
            if (entries_fd_set_.find(base_idx) == entries_fd_set_.end()) {
                // need to actually create the file
                cache_size_atomic_[base_idx] = 0;
                fd = open(getDataFilePath(base_idx).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
                if (fd < 0) {
                    LOG(ERROR) << "Can't open file for write of base idx " << base_idx << ": " << strerror(errno);
                    return;
                }
                entries_fd_set_[base_idx] = fd;
            }
        }
    } else {
        // entries in cache, get fd for file
        fd = entries_fd_set_[base_idx];
        read_lock.unlock();
    }

    LogEntry e;
    size_t entry_len = Deserializer(e, buf);
    uint64_t local_idx = (e.log_idx - base_idx) / shard_num_;

    if (len = pwrite(fd, buf, entry_len, entry_len * local_idx) != entry_len) {
        LOG(WARNING) << "Writing less bytes than expected " << len;
    }

    if (entry_size_ == 0) {
        entry_size_ = entry_len;
        LOG(WARNING) << "setting entry size " << entry_size_;
    }

    cache_size_atomic_[base_idx].fetch_add(1);
}

#endif

std::string ShardServer::getDataFilePath(uint64_t base_idx) {
    return folder_path_ + "/entries_" + std::to_string(base_idx) + "_r_" + std::to_string(shard_id_) + ".dat";
}

int ShardServer::writeFromCacheToDisk(uint64_t base_idx) {
#ifdef CORFU
    std::unique_lock<std::shared_mutex> write_lock(cache_rw_lock_);
    auto it = entries_fd_set_.find(base_idx);
    if (it == entries_fd_set_.end()) {
        LOG(ERROR) << "File not open for base idx " << base_idx;
    } else {
        close(it->second);
        entries_fd_set_.erase(base_idx);
        cache_size_atomic_.erase(base_idx);
    }
#else
    auto it = entries_fd_set_.find(base_idx);
    if (it == entries_fd_set_.end()) {
        LOG(ERROR) << "File not open for base idx " << base_idx;
    }
    close(it->second);
#endif

    return 0;
}

int ShardServer::loadFromDiskToCache(uint64_t base_idx) {
    int fd = open(getDataFilePath(base_idx).c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(INFO) << "Can't open data file of base idx " << base_idx;
        return fd;
    }
    struct stat info;
    if (fstat(fd, &info) < 0) {
        LOG(ERROR) << "Can't stat data file of base idx " << base_idx;
        close(fd);
        return errno;
    }
    uint8_t *buf = static_cast<uint8_t *>(mmap(0, info.st_size, PROT_READ, MAP_FILE | MAP_PRIVATE, fd, 0));
    if (buf == MAP_FAILED) {
        LOG(ERROR) << "Can't mmap data file of base idx " << base_idx;
        return -1;
    }
    entries_cache_set_[base_idx].clear();
    MultiDeserializer(entries_cache_set_[base_idx], buf, info.st_size);
    // if entry is not fully filled, keep fd open and populate size
    if (info.st_size < stripe_unit_size_) {
        cache_size_[base_idx] = info.st_size;
        entries_fd_set_[base_idx] = fd;
    } else {
        close(fd);
    }

    munmap(buf, info.st_size);

    return 0;
}

bool ShardServer::allRPCCompleted(std::vector<RPCToken> &tokens) {
    for (auto &t : tokens) {
        if (!t.Complete()) return false;
    }
    return true;
}

}  // namespace lazylog
