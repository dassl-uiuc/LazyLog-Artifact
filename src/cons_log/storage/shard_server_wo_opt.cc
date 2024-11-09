#include "shard_server_wo_opt.h"

#include <unistd.h>

#include <cerrno>

#include "../../rpc/common.h"
#include "sys/mman.h"
namespace lazylog {

std::unordered_map<std::string, std::shared_ptr<ShardClient>> ShardServerUnoptimized::backups_;
std::unordered_map<uint64_t, int> ShardServerUnoptimized::entries_fd_set_;
std::unordered_map<uint64_t, size_t> ShardServerUnoptimized::cache_size_;
std::unordered_map<uint64_t, size_t> ShardServerUnoptimized::num_entries_;
std::unordered_map<uint64_t, std::map<uint64_t, uint64_t>> ShardServerUnoptimized::gsn_to_file_offset_map_;
std::shared_mutex ShardServerUnoptimized::cache_rw_lock_;
std::condition_variable_any ShardServerUnoptimized::cache_write_cv_;
uint64_t ShardServerUnoptimized::replicated_index_ = 0;
ShardServerMetrics ShardServerUnoptimized::metrics_ = {};
size_t ShardServerUnoptimized::stripe_unit_size_ = 0;
std::string ShardServerUnoptimized::folder_path_ = "";
int ShardServerUnoptimized::shard_num_ = 0;
int ShardServerUnoptimized::shard_id_ = 0;

void svr_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

ShardServerUnoptimized::ShardServerUnoptimized() : is_primary_(false) {}

std::ostream &operator<<(std::ostream &out, ShardServerMetrics const &metrics) {
    out << "metrics: " << std::endl
        << "\tnum_slow_path_reads: " << metrics.num_slow_path_reads << std::endl
        << "\tnum_fast_path_reads: " << metrics.num_fast_path_reads << std::endl;
    return out;
}

ShardServerUnoptimized::~ShardServerUnoptimized() {
    if (is_primary_) {
        std::cout << metrics_;
    }
}

void ShardServerUnoptimized::Initialize(const Properties &p) {
    const std::string server_uri = p.GetProperty(PROP_SHD_SVR_URI, PROP_SHD_SVR_URI_DEFAULT);
    nexus_ = new erpc::Nexus(server_uri, 0, 0);

    stripe_unit_size_ = std::stoi(p.GetProperty(PROP_SHD_STRIPE_SIZE, PROP_SHD_STRIPE_SIZE_DEFAULT));
    shard_num_ = std::stoi(p.GetProperty("shard.num", "1"));
    shard_id_ = std::stoi(p.GetProperty("shard.id", "0"));
    folder_path_ = p.GetProperty(PROP_SHD_FOLDER_PATH, PROP_SHD_FOLDER_PATH_DEFAULT);
    replicated_index_ = std::stoull(p.GetProperty("shard.replicated_index", "0"));

    struct stat info;
    if (::stat(folder_path_.c_str(), &info) != 0 || !S_ISDIR(info.st_mode)) {
        if (::mkdir(folder_path_.c_str(), 0777) != 0) {
            LOG(ERROR) << "Can't make directory, error: " << errno;
            throw Exception("Can't make directory");
        }
    }

    nexus_->register_req_func(APPEND_BATCH, AppendBatchHandler);
    nexus_->register_req_func(REP_BATCH, ReplicateBatchHandler);
    nexus_->register_req_func(READ_ENTRY_BE, ReadEntryHandler);

    if (p.GetProperty("leader", "false") != "true") {
        LOG(INFO) << "Not a shard server leader";
        ShardServerUnoptimized::server_func(p);
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

    // start n threads to handle client reads
    const int n_th = std::stoi(p.GetProperty("threadcount", "1"));
    for (int i = 0; i < n_th; i++) {
        server_threads_.emplace_back(std::move(std::thread(ShardServerUnoptimized::read_server_func, p, i)));
    }
    ShardServerUnoptimized::server_func(p);
}

void ShardServerUnoptimized::Finalize() {
    for (auto &t : server_threads_) t.join();
    for (auto &b : backups_) b.second->Finalize();
    delete nexus_;
}

void ShardServerUnoptimized::server_func(const Properties &p) {
    const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));

    if (!rpc_) rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, nullptr, 0, svr_sm_handler, phy_port);
    rpc_use_cnt_.fetch_add(1);

    while (run_) {
        rpc_->run_event_loop(1000);
    }

    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
}

void ShardServerUnoptimized::read_server_func(const Properties &p, int th_id) {
    ServerContext c;
    const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));

    if (!rpc_)
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, static_cast<void *>(&c), CL_CLI_RPCID_OFFSET + th_id,
                                               svr_sm_handler,
                                               phy_port);  // start from 64
    rpc_use_cnt_.fetch_add(1);

    c.thread_id_ = th_id;
    const size_t msg_size = std::stoull(p.GetProperty(PROP_DL_MSG_SIZE, PROP_DL_MSG_SIZE_DEFAULT));
    c.resp_buf_ = rpc_->alloc_msg_buffer_or_die(msg_size);

    while (run_) {
        rpc_->run_event_loop(1000);
    }

    rpc_->free_msg_buffer(c.resp_buf_);

    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
}

void ShardServerUnoptimized::AppendBatchHandler(erpc::ReqHandle *req_handle, void *_context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    auto num = *reinterpret_cast<uint32_t *>(req->buf_);
    LogEntry first_e_in_batch;
    Deserializer(first_e_in_batch, req->buf_ + sizeof(uint32_t));
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;
    uint64_t base_idx = first_e_in_batch.log_idx / big_stripe_unit_size * big_stripe_unit_size;

    std::vector<RPCToken> tokens;
    for (auto &b : backups_) {
        tokens.emplace_back();
        b.second->ReplicateBatchAsync(req->buf_, req->get_data_size(), tokens.back());
        RunERPCOnce();
    }

    {
        std::unique_lock<std::shared_mutex> write_lock(cache_rw_lock_);
        processEntriesAndBuildMap(base_idx, req->buf_);
        if (num_entries_[base_idx] >= stripe_unit_size_) writeFromCacheToDisk(base_idx);
    }

    while (!allRPCCompleted(tokens)) {
        RunERPCOnce();
    }

    {
        std::unique_lock<std::shared_mutex> write_lock(cache_rw_lock_);
        replicated_index_ = std::prev(gsn_to_file_offset_map_[base_idx].end())->first;
    }
    cache_write_cv_.notify_all();

    rpc_->resize_msg_buffer(&resp, sizeof(int));
    *reinterpret_cast<int *>(resp.buf_) = 0;
    rpc_->enqueue_response(req_handle, &resp);
}

void ShardServerUnoptimized::ReplicateBatchHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    auto num = *reinterpret_cast<uint32_t *>(req->buf_);
    LogEntry first_e_in_batch;
    Deserializer(first_e_in_batch, req->buf_ + sizeof(uint32_t));
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;
    uint64_t base_idx = first_e_in_batch.log_idx / big_stripe_unit_size * big_stripe_unit_size;

    processEntriesAndBuildMap(base_idx, req->buf_);

    *reinterpret_cast<Status *>(resp.buf_) = Status::OK;
    if (num_entries_[base_idx] >= stripe_unit_size_) {
        if (writeFromCacheToDisk(base_idx) < 0) *reinterpret_cast<Status *>(resp.buf_) = Status::ERROR;
    }

    rpc_->resize_msg_buffer(&resp, sizeof(Status));
    rpc_->enqueue_response(req_handle, &resp);
}

void ShardServerUnoptimized::ReadEntryHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = static_cast<ServerContext *>(context)->resp_buf_;

    auto idx = *reinterpret_cast<uint64_t *>(req->buf_);
    uint64_t big_stripe_unit_size = stripe_unit_size_ * shard_num_;
    uint64_t base_idx = idx / big_stripe_unit_size * big_stripe_unit_size;
    uint64_t local_cache_idx = (idx - base_idx) / shard_num_;
    size_t len = 0;
    bool slow_path_exercized = false;
    {
        std::shared_lock<std::shared_mutex> read_lock(cache_rw_lock_);
        while (1) {
            if (gsn_to_file_offset_map_.find(base_idx) == gsn_to_file_offset_map_.end()) {
                read_lock.unlock();
                bool loaded_by_me = false;
                bool loaded = false;
                {
                    // upgrade to a write lock.
                    // try loading the cache
                    std::unique_lock<std::shared_mutex> write_lock(cache_rw_lock_);
                    // need to check again as another thread might have loaded the cache
                    if (gsn_to_file_offset_map_.find(base_idx) == gsn_to_file_offset_map_.end()) {
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
                if (!loaded && gsn_to_file_offset_map_.find(base_idx) == gsn_to_file_offset_map_.end()) {
                    cache_write_cv_.wait(read_lock);
                    slow_path_exercized = true;
                    continue;
                }
            }

            // at this point the cache must be loaded
            if (local_cache_idx >= num_entries_[base_idx] || idx > replicated_index_) {
                slow_path_exercized = true;
                cache_write_cv_.wait(read_lock);
                continue;
            }

            auto &offset_map = gsn_to_file_offset_map_[base_idx];
            // if you find next entry, length is difference, else subtract from total size
            len = offset_map.find(idx + shard_num_) != offset_map.end() ? offset_map[idx + shard_num_] - offset_map[idx]
                                                                        : cache_size_[base_idx] - offset_map[idx];
            int got = readEntryFromDisk(base_idx, offset_map[idx], resp.buf_, len);
            if (len != got) {
                LOG(ERROR) << "not able to read length, expected " << len << ", got " << got;
                if (got == -1) {
                    LOG(ERROR) << strerror(errno);
                }
            };
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
}

void ShardServerUnoptimized::processEntriesAndBuildMap(uint64_t base_idx, const uint8_t *buf) {
    // here we assume the rpc only sends entries for the same stripe unit, which is enforced on CL
    int fd = 0;
    if (gsn_to_file_offset_map_.find(base_idx) == gsn_to_file_offset_map_.end()) {
        // not in cache but could be on disk
        if (loadFromDiskToCache(base_idx) < 0) {
            // not even on disk, have to create file and data structures
            gsn_to_file_offset_map_[base_idx] = std::map<uint64_t, uint64_t>();
            cache_size_[base_idx] = 0;
            num_entries_[base_idx] = 0;
            fd = open(getDataFilePath(base_idx).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
            if (fd < 0) {
                LOG(ERROR) << "Can't open file for write of base idx " << base_idx;
                return;
            }
            entries_fd_set_[base_idx] = fd;
        }
    } else {
        fd = entries_fd_set_[base_idx];
    }

    size_t batch_len = ProcessAndBuildMap(gsn_to_file_offset_map_[base_idx], buf,
                                          cache_size_[base_idx]) -
                       sizeof(uint32_t);  // does not contain the leading size
    cache_size_[base_idx] += batch_len;
    num_entries_[base_idx] += (*reinterpret_cast<const uint32_t *>(buf));
    LOG(INFO) << "cache " << base_idx << ": " << num_entries_[base_idx] << " entries, " << cache_size_[base_idx]
              << "B in total";

    if (write(fd, buf + sizeof(uint32_t), batch_len) != batch_len) {
        LOG(WARNING) << "Writing less bytes than expected";
    }
}

std::string ShardServerUnoptimized::getDataFilePath(uint64_t base_idx) {
    return folder_path_ + "/entries_" + std::to_string(base_idx) + "_r_" + std::to_string(shard_id_) + ".dat";
}

int ShardServerUnoptimized::writeFromCacheToDisk(uint64_t base_idx) {
    auto it = entries_fd_set_.find(base_idx);
    if (it == entries_fd_set_.end()) {
        LOG(ERROR) << "File not open for base idx " << base_idx;
    }
    close(it->second);

    return 0;
}

int ShardServerUnoptimized::readEntryFromDisk(uint64_t base_idx, uint64_t file_offset, uint8_t *buf, size_t len) {
    int fd = open(getDataFilePath(base_idx).c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Can't open data file of base idx " << base_idx;
        return fd;
    }
    int ret = pread(fd, buf, len, file_offset);
    close(fd);
    return ret;
}

int ShardServerUnoptimized::loadFromDiskToCache(uint64_t base_idx) {
    int fd = open(getDataFilePath(base_idx).c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Can't open data file of base idx " << base_idx;
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
    gsn_to_file_offset_map_[base_idx].clear();
    cache_size_[base_idx] = 0;
    auto p = ProcessAndBuildMap(gsn_to_file_offset_map_[base_idx], buf, info.st_size, cache_size_[base_idx]);
    cache_size_[base_idx] += p.first;
    num_entries_[base_idx] += p.second;
    if (info.st_size >= stripe_unit_size_) {
        close(fd);
    }

    munmap(buf, info.st_size);
    return 0;
}

bool ShardServerUnoptimized::allRPCCompleted(std::vector<RPCToken> &tokens) {
    for (auto &t : tokens) {
        if (!t.Complete()) return false;
    }
    return true;
}

}  // namespace lazylog
