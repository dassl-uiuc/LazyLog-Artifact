#include "datalog.h"

#include "sys/mman.h"

namespace lazylog {

std::unordered_map<std::string, std::shared_ptr<DataLogClient>> DataLog::backups_;
std::unordered_map<uint64_t, LogEntry *> DataLog::entries_cache_;
std::unordered_map<uint64_t, std::pair<uint64_t, std::pair<uint64_t, uint64_t>>> DataLog::gsn_offset_map_;
std::shared_mutex DataLog::gsn_map_lock_;
std::shared_mutex DataLog::gsn_list_lock_;
std::condition_variable_any DataLog::read_cv_;
uint64_t DataLog::replicated_index_ = 0;
uint64_t DataLog::global_index_ = 0;
struct circular_buffer *DataLog::per_client_log_[MAX_NUM_CLIENTS];
std::unordered_map<uint64_t, uint64_t> DataLog::client_id_mapping_;
std::shared_mutex DataLog::client_id_mapping_lock_;
uint64_t DataLog::next_client_id_ = 0;
std::string DataLog::folder_path_ = "";
uint8_t *DataLog::scratch_buf_ = NULL;
uint64_t DataLog::active_file_entries_ = 0;
uint64_t DataLog::active_file_index_ = 0;
uint64_t DataLog::gsn_file_index_ = 0;
uint64_t DataLog::gsn_file_entries_ = 0;
uint64_t DataLog::gsn_file_offset_ = 0;
uint64_t DataLog::active_file_offset_ = 0;
uint64_t DataLog::stripe_unit_size_;
std::vector<int> DataLog::gsn_list_;
int DataLog::active_fd_ = -1;
int DataLog::gsn_fd_ = -1;
int DataLog::shard_num_ = 0;
int DataLog::shard_id_ = 0;

void svr_sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

DataLog::DataLog() : is_primary_(false) {}
DataLog::~DataLog() {}

uint64_t DataLog::getBucket(uint64_t client_id) {
    {
        std::shared_lock<std::shared_mutex> read_lock(client_id_mapping_lock_);
        if (client_id_mapping_.find(client_id) != client_id_mapping_.end()) {
            return client_id_mapping_[client_id];
        }
    }
    {
        std::unique_lock<std::shared_mutex> write_lock(client_id_mapping_lock_);
        if (client_id_mapping_.find(client_id) != client_id_mapping_.end()) {
            return client_id_mapping_[client_id];
        }
        if (next_client_id_ >= MAX_NUM_CLIENTS) {
            LOG(ERROR) << "No more client ids to assign";
            return std::numeric_limits<uint64_t>::max();
        }
        client_id_mapping_[client_id] = next_client_id_;
        next_client_id_++;
        return client_id_mapping_[client_id];
    }
}

bool DataLog::isMyRequest(LogEntry &e) { return shard_id_ == std::stoll(e.data); }

void DataLog::Initialize(const Properties &p) {
    const std::string server_uri = p.GetProperty(PROP_SHD_SVR_URI, PROP_SHD_SVR_URI_DEFAULT);
    nexus_ = new erpc::Nexus(server_uri, 0, 0);

    shard_num_ = std::stoi(p.GetProperty("shard.num", "1"));
    shard_id_ = std::stoi(p.GetProperty("shard.id", "0"));
    folder_path_ = p.GetProperty(PROP_SHD_FOLDER_PATH, PROP_SHD_FOLDER_PATH_DEFAULT);
    scratch_buf_ = new uint8_t[MAX_BE_FILE_SIZE];
    stripe_unit_size_ = std::stoi(p.GetProperty(PROP_SHD_STRIPE_SIZE, PROP_SHD_STRIPE_SIZE_DEFAULT));

    struct stat info;
    if (::stat(folder_path_.c_str(), &info) != 0 || !S_ISDIR(info.st_mode)) {
        if (::mkdir(folder_path_.c_str(), 0777) != 0) {
            LOG(ERROR) << "Can't make directory, error: " << strerror(errno);
            throw Exception("Can't make directory");
        }
    }

    createNewDataFile();
    createNewGsnFile();

    // initialize per_client_log
    for (uint64_t i = 0; i < MAX_NUM_CLIENTS; i++) {
        per_client_log_[i] = init(CLIENT_LOG_LENGTH);
    }

    nexus_->register_req_func(ORDER_BATCH, OrderBatchHandler);
    nexus_->register_req_func(REP_BATCH, ReplicateBatchHandler);
    nexus_->register_req_func(APPEND_ENTRY_SHD, AppendEntryHandler);
    nexus_->register_req_func(UPDATE_GLBL_IDX, UpdateGlobalIdxHandler);
    nexus_->register_req_func(READ_ENTRY_BE, ReadEntryHandler);
    nexus_->register_req_func(GET_READ_METADATA, GetReadMetadata);
    nexus_->register_req_func(READ_BATCH, ReadBatchHandler);

    if (p.GetProperty("leader", "false") != "true") {
        LOG(INFO) << "Not a shard server leader";
        // start n threads to handle append entry RPCs
        const int n_th = std::stoi(p.GetProperty("threadcount", "1"));
        for (int i = 0; i < n_th; i++) {
            server_threads_.emplace_back(std::move(std::thread(DataLog::append_entry_server_func, p, i)));
        }
        DataLog::mm_server_func(p);
        return;
    }

    is_primary_ = true;
    LOG(INFO) << "This is shard server leader";

    const std::vector<std::string> backup_uri =
        SeparateValue(p.GetProperty(PROP_SHD_BACKUP_URI, PROP_SHD_BACKUP_URI_DEFAULT), ',');
    for (auto &b : backup_uri) {
        backups_[b] = std::make_shared<DataLogClient>();
        backups_[b]->InitializeConn(p, b, (void *)(-1));
    }

    // start n threads to handle append entry RPCs
    const int n_th = std::stoi(p.GetProperty("threadcount", "1"));
    for (int i = 0; i < n_th; i++) {
        server_threads_.emplace_back(std::move(std::thread(DataLog::append_entry_server_func, p, i)));
    }
    DataLog::mm_server_func(p);
}

void DataLog::Finalize() {
    for (auto &t : server_threads_) t.join();
    for (auto &b : backups_) b.second->Finalize();
    delete nexus_;
}

void DataLog::mm_server_func(const Properties &p) {
    const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));

    if (!rpc_) rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, nullptr, 0, svr_sm_handler, phy_port);
    rpc_use_cnt_.fetch_add(1);
    const size_t msg_size = std::stoull(p.GetProperty(PROP_SHD_MSG_SIZE, PROP_SHD_MSG_SIZE_DEFAULT));
    rpc_->set_pre_resp_msgbuf_size(msg_size);

    while (run_) {
        rpc_->run_event_loop(1000);
    }

    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
}

void DataLog::append_entry_server_func(const Properties &p, int th_id) {
    const uint8_t phy_port = std::stoi(p.GetProperty("erpc.phy_port", "0"));

    if (!rpc_)
        rpc_ = new erpc::Rpc<erpc::CTransport>(nexus_, nullptr, SHD_SVR_RPCID_OFFSET + th_id, svr_sm_handler,
                                               phy_port);  // start from 64
    rpc_use_cnt_.fetch_add(1);
    const size_t msg_size = std::stoull(p.GetProperty(PROP_SHD_MSG_SIZE, PROP_SHD_MSG_SIZE_DEFAULT));
    rpc_->set_pre_resp_msgbuf_size(msg_size);

    while (run_) {
        rpc_->run_event_loop(1000);
    }

    if (rpc_use_cnt_.fetch_sub(1) == 1) delete rpc_;
}

void DataLog::UpdateGlobalIdxHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    uint64_t global_idx = *reinterpret_cast<uint64_t *>(req->buf_);
    {
        std::unique_lock<std::shared_mutex> lock(gsn_map_lock_);
        global_index_ = global_idx;
    }
    read_cv_.notify_all();

    *reinterpret_cast<int *>(resp.buf_) = 0;
    rpc_->resize_msg_buffer(&resp, sizeof(int));
    rpc_->enqueue_response(req_handle, &resp);
}

void DataLog::OrderBatchHandler(erpc::ReqHandle *req_handle, void *_context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    auto num = *reinterpret_cast<uint32_t *>(req->buf_);
    std::vector<LogEntry> entries;
    std::vector<LogEntry *> filled_entries;
    std::vector<int> gsn_added;
    MultiDeserializer(entries, req->buf_);

    // replicate to backups
    std::vector<RPCToken> tokens;
    for (auto &b : backups_) {
        tokens.emplace_back();
        b.second->ReplicateBatchAsync(req->buf_, req->get_data_size(), tokens.back());
        RunERPCOnce();
    }

    LogEntry *entry = nullptr;
    for (auto &e : entries) {
        uint64_t bucket_num;
        if (!isMyRequest(e)) {
            goto end;
        }
        bucket_num = getBucket(e.client_id);
        while (!pop(per_client_log_[bucket_num], entry))
            ;
        if (e.client_id != entry->client_id || e.client_seq != entry->client_seq) {
            LOG(ERROR) << "impossible";
        }
        entry->log_idx = e.log_idx;
        filled_entries.push_back(entry);
    end:
        gsn_added.push_back(std::stoll(e.data));
    }

    {
        std::unique_lock<std::shared_mutex> lock(gsn_map_lock_);
        writeToDisk(filled_entries);
    }
    writeToDisk(gsn_added);
    {
        std::unique_lock<std::shared_mutex> lock(gsn_list_lock_);
        gsn_list_.insert(gsn_list_.end(), gsn_added.begin(), gsn_added.end());
    }

    for (auto &e : filled_entries) {
        delete e;
    }

    while (!allRPCCompleted(tokens)) {
        RunERPCOnce();
    }

    {
        std::unique_lock<std::shared_mutex> lock(gsn_map_lock_);
        replicated_index_ = entries.back().log_idx;
    }
    read_cv_.notify_all();

    rpc_->resize_msg_buffer(&resp, sizeof(int));
    *reinterpret_cast<int *>(resp.buf_) = 0;
    rpc_->enqueue_response(req_handle, &resp);
}

void DataLog::ReplicateBatchHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    auto num = *reinterpret_cast<uint32_t *>(req->buf_);
    std::vector<LogEntry> entries;
    std::vector<LogEntry *> filled_entries;
    MultiDeserializer(entries, req->buf_);

    LogEntry *entry = nullptr;

    for (auto &e : entries) {
        if (!isMyRequest(e)) continue;
        uint64_t bucket_num = getBucket(e.client_id);
        while (!pop(per_client_log_[bucket_num], entry))
            ;
        assert(e.client_id == entry->client_id && e.client_seq == entry->client_seq);
        entry->log_idx = e.log_idx;
        filled_entries.push_back(entry);
    }

    {
        std::unique_lock<std::shared_mutex> lock(gsn_map_lock_);
        writeToDisk(filled_entries);
    }
    for (auto &e : filled_entries) {
        delete e;
    }
    *reinterpret_cast<Status *>(resp.buf_) = Status::OK;
    rpc_->resize_msg_buffer(&resp, sizeof(Status));
    rpc_->enqueue_response(req_handle, &resp);
}

void DataLog::AppendEntryHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    LogEntry *entry = new LogEntry();
    Deserializer(*entry, req->buf_);
    uint64_t bucket_num = getBucket(entry->client_id);
    assert(bucket_num >= 0 && bucket_num < MAX_NUM_CLIENTS);

    while (!push(per_client_log_[bucket_num], entry))
        ;

    *reinterpret_cast<Status *>(resp.buf_) = Status::OK;
    rpc_->resize_msg_buffer(&resp, sizeof(Status));
    rpc_->enqueue_response(req_handle, &resp);
}

size_t DataLog::readFromDisk(uint64_t base_idx, uint64_t offset, uint64_t size, uint8_t *buf) {
    int fd = open(getDataFilePath(base_idx).c_str(), O_RDONLY);
    if (fd == -1) {
        LOG(ERROR) << "Failed to open data file";
        return 0;
    }

    size_t bytesRead = pread(fd, buf, size, offset);
    close(fd);
    return bytesRead;
}

size_t DataLog::readFromDisk(uint64_t base_idx, uint64_t from_offset, uint8_t *buf) {
    int fd = open(getDataFilePath(base_idx).c_str(), O_RDONLY);
    if (fd == -1) {
        LOG(ERROR) << "Failed to open data file";
        return 0;
    }
    struct stat info;
    if (fstat(fd, &info) < 0) {
        LOG(ERROR) << "Can't stat data file of base idx " << base_idx;
        close(fd);
        return errno;
    }
    size_t bytesRead = pread(fd, buf, info.st_size - from_offset, from_offset);
    close(fd);
    return bytesRead;
}

void DataLog::ReadEntryHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    uint64_t idx = *reinterpret_cast<uint64_t *>(req->buf_);
    uint64_t base_file_no;
    std::pair<uint64_t, uint64_t> offset_size;
    LOG(INFO) << "Read entry idx: " << idx;
    {
        std::shared_lock<std::shared_mutex> read_lock(gsn_map_lock_);
        while (true) {
            if (idx > global_index_ || idx > replicated_index_ || replicated_index_ == 0 || global_index_ == 0) {
                read_cv_.wait(read_lock);
                continue;
            }
            break;
        }
        base_file_no = gsn_offset_map_[idx].first;
        offset_size = gsn_offset_map_[idx].second;
    }

    size_t len = readFromDisk(base_file_no, offset_size.first, offset_size.second, resp.buf_);
    if (len != offset_size.second) {
        LOG(ERROR) << "Failed to read entry from disk";
        *reinterpret_cast<Status *>(resp.buf_) = Status::ERROR;
        rpc_->resize_msg_buffer(&resp, sizeof(Status));
        rpc_->enqueue_response(req_handle, &resp);
        return;
    }

    rpc_->resize_msg_buffer(&resp, len);
    rpc_->enqueue_response(req_handle, &resp);
}

void DataLog::GetReadMetadata(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    uint64_t start_idx = *reinterpret_cast<uint64_t *>(req->buf_);
    uint64_t end_idx = *reinterpret_cast<uint64_t *>(req->buf_ + sizeof(uint64_t));

    {
        std::shared_lock<std::shared_mutex> read_lock(gsn_map_lock_);
        while (true) {
            if (end_idx > global_index_ || end_idx > replicated_index_ || replicated_index_ == 0 ||
                global_index_ == 0) {
                read_cv_.wait(read_lock);
                continue;
            }
            break;
        }
    }
    {
        std::shared_lock<std::shared_mutex> read_lock(gsn_list_lock_);
        size_t len = MultiIntSerializer(gsn_list_, start_idx, end_idx - start_idx + 1, resp.buf_, true);
        rpc_->resize_msg_buffer(&resp, len);
    }

    rpc_->enqueue_response(req_handle, &resp);
}

size_t DataLog::collectBatchEntries(std::pair<uint64_t, uint64_t> base_file_no, std::pair<uint64_t, uint64_t> offsets,
                                    uint8_t *buf) {
    uint64_t offset = 0;
    offset += sizeof(uint32_t);

    if (base_file_no.first == base_file_no.second) {
        offset += readFromDisk(base_file_no.first, offsets.first, offsets.second - offsets.first, buf + offset);
        *reinterpret_cast<uint32_t *>(buf) = GetNumEntries(buf + sizeof(uint32_t), offset - sizeof(uint32_t));
        LOG(INFO) << "read " << *reinterpret_cast<uint32_t *>(buf) << " entries";
        return offset;
    }

    for (uint64_t i = base_file_no.first; i <= base_file_no.second; i++) {
        if (i == base_file_no.first) {
            offset += readFromDisk(i, offsets.first, buf + offset);  // read from offsets.first to EOF
        } else if (i == base_file_no.second) {
            offset += readFromDisk(i, 0, offsets.second, buf + offset);  // read from offsets.second bytes from 0
        } else {
            offset += readFromDisk(i, 0, buf + offset);  // collect entire file
        }
    }

    *reinterpret_cast<uint32_t *>(buf) = GetNumEntries(buf + sizeof(uint32_t), offset - sizeof(uint32_t));
    LOG(INFO) << "read " << *reinterpret_cast<uint32_t *>(buf) << " entries";

    return offset;
}

void DataLog::ReadBatchHandler(erpc::ReqHandle *req_handle, void *context) {
    auto *req = req_handle->get_req_msgbuf();
    auto &resp = req_handle->pre_resp_msgbuf_;

    uint64_t start_idx = *reinterpret_cast<uint64_t *>(req->buf_);
    uint64_t end_idx = *reinterpret_cast<uint64_t *>(req->buf_ + sizeof(uint64_t));
    std::pair<uint64_t, uint64_t> base_file_no;
    std::pair<uint64_t, uint64_t> offsets;
    {
        std::shared_lock<std::shared_mutex> read_lock(gsn_map_lock_);
        while (true) {
            if (end_idx > global_index_ || end_idx > replicated_index_ || replicated_index_ == 0 ||
                global_index_ == 0) {
                read_cv_.wait(read_lock);
                continue;
            }
            break;
        }
        base_file_no = {gsn_offset_map_[start_idx].first, gsn_offset_map_[end_idx].first};
        offsets = {gsn_offset_map_[start_idx].second.first,
                   gsn_offset_map_[end_idx].second.first + gsn_offset_map_[end_idx].second.second};
    }

    size_t len = collectBatchEntries(base_file_no, offsets, resp.buf_);

    rpc_->resize_msg_buffer(&resp, len);
    rpc_->enqueue_response(req_handle, &resp);
}

void DataLog::writeToDisk(std::vector<LogEntry *> &entries) {
    uint64_t num_entries = entries.size();
    uint64_t from = 0;
    uint64_t curr_batch_entries;
    size_t batch_len;
    while (num_entries != 0) {
        if (active_file_entries_ >= stripe_unit_size_) createNewDataFile();
        curr_batch_entries = std::min(num_entries, stripe_unit_size_ - active_file_entries_);
        batch_len = MultiSerializer(entries, from, curr_batch_entries, scratch_buf_, gsn_offset_map_,
                                    active_file_index_, active_file_offset_);
        if (write(active_fd_, scratch_buf_, batch_len) != batch_len) {
            LOG(WARNING) << "Writing less bytes than expected";
        }
        LOG(INFO) << "data file " << active_file_index_ << ": " << curr_batch_entries << " entries, " << batch_len
                  << "B in total";
        active_file_offset_ += batch_len;
        active_file_entries_ += curr_batch_entries;
        from += curr_batch_entries;
        num_entries -= curr_batch_entries;
    }
}

void DataLog::writeToDisk(std::vector<int> &gsn_added) {
    uint64_t num_entries = gsn_added.size();
    uint64_t from = 0;
    uint64_t curr_batch_entries;
    size_t batch_len;
    while (num_entries != 0) {
        if (gsn_file_entries_ >= stripe_unit_size_) createNewGsnFile();
        curr_batch_entries = std::min(num_entries, stripe_unit_size_ - gsn_file_entries_);
        batch_len = MultiIntSerializer(gsn_added, from, curr_batch_entries, scratch_buf_);
        if (write(gsn_fd_, scratch_buf_, batch_len) != batch_len) {
            LOG(WARNING) << "Writing less bytes than expected";
        }
        LOG(INFO) << "gsn file " << gsn_file_index_ << ": " << curr_batch_entries << " entries, " << batch_len
                  << "B in total";
        gsn_file_offset_ += batch_len;
        gsn_file_entries_ += curr_batch_entries;
        from += curr_batch_entries;
        num_entries -= curr_batch_entries;
    }
}

std::string DataLog::getDataFilePath(uint64_t base_idx) {
    return folder_path_ + "/entries_" + std::to_string(base_idx) + "_r_" + std::to_string(shard_id_) + ".dat";
}

std::string DataLog::getGsnListFilePath(uint64_t base_idx) {
    return folder_path_ + "/gsn_list_" + std::to_string(base_idx) + "_r_" + std::to_string(shard_id_) + ".dat";
}

int DataLog::createNewGsnFile() {
    if (gsn_fd_ != -1) close(gsn_fd_);
    int fd = open(getGsnListFilePath(gsn_file_index_ + 1).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        LOG(ERROR) << "unable to create new gsn file for index " << gsn_file_index_ + 1;
        return -1;
    }
    gsn_file_index_++;
    gsn_fd_ = fd;
    gsn_file_entries_ = 0;
    gsn_file_offset_ = 0;
    return 0;
}

int DataLog::createNewDataFile() {
    if (active_fd_ != -1) close(active_fd_);
    int fd = open(getDataFilePath(active_file_index_ + 1).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        LOG(ERROR) << "unable to create new data file for index " << active_file_index_ + 1;
        return -1;
    }
    active_file_index_++;
    active_fd_ = fd;
    active_file_entries_ = 0;
    active_file_offset_ = 0;
    return 0;
}

bool DataLog::allRPCCompleted(std::vector<RPCToken> &tokens) {
    for (auto &t : tokens) {
        if (!t.Complete()) return false;
    }
    return true;
}

}  // namespace lazylog
