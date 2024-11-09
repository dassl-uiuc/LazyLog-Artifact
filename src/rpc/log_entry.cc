#include "log_entry.h"

#include <cassert>
#include <cstring>

#define SERIALIZE_FIELD(BUF, OFFSET, FIELD)                         \
    do {                                                            \
        *reinterpret_cast<decltype(FIELD) *>(BUF + OFFSET) = FIELD; \
        OFFSET += sizeof(FIELD);                                    \
    } while (0)

#define DESERIALIZE_FIELD(BUF, OFFSET, FIELD)                             \
    do {                                                                  \
        FIELD = *reinterpret_cast<const decltype(FIELD) *>(BUF + OFFSET); \
        OFFSET += sizeof(FIELD);                                          \
    } while (0)

namespace lazylog {

LogEntry::LogEntry() : client_id(0), client_seq(0), log_idx(0), size(0), flags(0), view(0), data() {}

LogEntry::LogEntry(const char *data) : client_id(0), client_seq(0), log_idx(0), size(0), flags(0), view(0), data(data) {
    size = this->data.size();
}

bool LogEntry::operator==(const LogEntry &e) const {
    return (e.client_id == client_id) && (e.client_seq == client_seq) && (e.size == size) && (e.flags == flags) &&
           (e.view == view) && (e.data == data);
}

std::ostream &operator<<(std::ostream &os, const LogEntry &e) {
    os << "{cli id: " << e.client_id << ", cli seq: " << e.client_seq << ", log index: " << e.log_idx
       << ", data size: " << e.size << ", flags: " << std::hex << e.flags << std::dec << ", view: " << e.view
       << ", data: " << e.data << "}";

    return os;
}

size_t GetSize(const LogEntry &e) { return MetaDataSize() + (e.size + 3) & (~3); }

size_t GetSizeFromBuf(const uint8_t *buf) {
    size_t sz_ofst = sizeof(LogEntry::client_id) + sizeof(LogEntry::client_seq) + sizeof(LogEntry::log_idx);
    return MetaDataSize() + (*reinterpret_cast<const decltype(LogEntry::size) *>(buf + sz_ofst) + 3) & (~3);
}

uint64_t GetClientId(const uint8_t *buf) { return *reinterpret_cast<const decltype(LogEntry::client_id) *>(buf); }

uint64_t GetClientSeq(const uint8_t *buf) {
    return *reinterpret_cast<const decltype(LogEntry::client_seq) *>(buf + sizeof(LogEntry::client_id));
}

uint64_t GetIdx(const uint8_t *buf) {
    return *reinterpret_cast<const decltype(LogEntry::client_seq) *>(buf + sizeof(LogEntry::client_id) +
                                                                     sizeof(LogEntry::client_seq));
}

size_t Serializer(const LogEntry &e, uint8_t *buf) {
    size_t offset = 0;
    SERIALIZE_FIELD(buf, offset, e.client_id);
    SERIALIZE_FIELD(buf, offset, e.client_seq);
    SERIALIZE_FIELD(buf, offset, e.log_idx);
    assert(e.size == e.data.size());
    SERIALIZE_FIELD(buf, offset, e.size);
    SERIALIZE_FIELD(buf, offset, e.flags);
    SERIALIZE_FIELD(buf, offset, e.view);
    std::memcpy(buf + offset, e.data.data(), e.data.size());
    offset += (e.data.size() + 3) & (~3);  // ensure 4-bytes aligned
    return offset;
}

size_t Deserializer(LogEntry &e, const uint8_t *buf) {
    size_t offset = 0;
    DESERIALIZE_FIELD(buf, offset, e.client_id);
    DESERIALIZE_FIELD(buf, offset, e.client_seq);
    DESERIALIZE_FIELD(buf, offset, e.log_idx);
    DESERIALIZE_FIELD(buf, offset, e.size);
    DESERIALIZE_FIELD(buf, offset, e.flags);
    DESERIALIZE_FIELD(buf, offset, e.view);
    if (e.flags == 0) return 0;
    e.data = std::string(reinterpret_cast<const char *>(buf) + offset, e.size);
    offset += (e.data.size() + 3) & (~3);

    return offset;
}

size_t MultiSerializer(const std::vector<LogEntry> &es, uint8_t *buf) {
    size_t offset = 0;
    uint32_t num = es.size();
    *reinterpret_cast<uint32_t *>(buf) = num;
    offset += sizeof(num);

    for (auto &e : es) {
        offset += Serializer(e, buf + offset);
    }

    return offset;
}

size_t MultiSerializer(const std::vector<LogEntry> &es, uint64_t from, uint32_t num, uint8_t *buf, bool wo_num) {
    size_t offset = 0;
    if (!wo_num) {
        *reinterpret_cast<uint32_t *>(buf) = num;
        offset += sizeof(num);
    }

    for (uint32_t i = 0; i < num; ++i) {
        offset += Serializer(es[from + i], buf + offset);
    }

    return offset;
}

size_t MultiSerializer(const std::vector<LogEntry> &es, uint64_t from, uint64_t &to, uint32_t itrv, uint8_t *buf) {
    size_t offset = sizeof(uint32_t);
    uint32_t num = 0;
    uint64_t i;
    for (i = from; i <= to; i += itrv) {
        offset += Serializer(es[i], buf + offset);
        num++;
    }

    *reinterpret_cast<uint32_t *>(buf) = num;
    to = i - itrv;
    return offset;
}

size_t MultiDeserializer(std::vector<LogEntry> &es, const uint8_t *buf) {
    size_t offset = 0;
    uint32_t num = *reinterpret_cast<const uint32_t *>(buf);
    offset += sizeof(num);

    for (uint32_t i = 0; i < num; i++) {
        es.emplace_back();
        size_t sz = Deserializer(es.back(), buf + offset);
        if (sz == 0) {
            es.pop_back();
            break;
        }
        offset += sz;
    }

    return offset;
}

size_t ProcessAndBuildMap(std::map<uint64_t, uint64_t> &map, const uint8_t *buf, const uint64_t curr_file_offset) {
    size_t offset = 0;
    LogEntry dummy;
    uint32_t num = *reinterpret_cast<const uint32_t *>(buf);
    offset += sizeof(num);

    for (uint32_t i = 0; i < num; i++) {
        size_t sz = Deserializer(dummy, buf + offset);
        if (sz == 0) {
            break;
        }
        map[dummy.log_idx] = curr_file_offset + offset - sizeof(num);
        offset += sz;
    }

    return offset;
}

std::pair<size_t, uint32_t> ProcessAndBuildMap(std::map<uint64_t, uint64_t> &map, const uint8_t *buf, const size_t size,
                                               const uint64_t curr_file_offset) {
    size_t offset = 0;
    LogEntry dummy;
    uint32_t num = 0;

    while (offset < size) {
        size_t sz = Deserializer(dummy, buf + offset);
        if (sz == 0) {
            break;
        }
        num++;
        map[dummy.log_idx] = curr_file_offset + offset;
        offset += sz;
    }

    return {offset, num};
}

size_t MultiDeserializer(std::vector<LogEntry> &es, const uint8_t *buf, const size_t size) {
    size_t offset = 0;
    while (offset < size) {
        es.emplace_back();
        size_t sz = Deserializer(es.back(), buf + offset);
        if (sz == 0) {
            es.pop_back();
            break;
        }
        offset += sz;
    }

    return offset;
}

size_t MultiDeserializerMaxSize(std::vector<LogEntry> &es, const uint8_t *buf, const size_t max_size) {
    size_t offset = 0;
    while (offset < max_size) {
        if (max_size - offset < MetaDataSize()) break;                // no complete entry remains
        if (max_size - offset < GetSizeFromBuf(buf + offset)) break;  // no complete entry remains
        es.emplace_back();
        size_t sz = Deserializer(es.back(), buf + offset);
        if (sz == 0) {
            es.pop_back();
            break;
        }
        offset += sz;
    }

    return offset;
}

size_t ReqIdSerializer(const std::vector<LogEntry::ReqID> &ids, uint8_t *buf) {
    size_t offset = 0;
    uint32_t num = ids.size();
    *reinterpret_cast<uint32_t *>(buf) = num;
    offset += sizeof(num);

    for (auto &rid : ids) {
        SERIALIZE_FIELD(buf, offset, rid.first);
        SERIALIZE_FIELD(buf, offset, rid.second);
    }
    return offset;
}

size_t ReqIdDeserializer(std::vector<LogEntry::ReqID> &ids, const uint8_t *buf) {
    size_t offset = 0;
    uint32_t num = *reinterpret_cast<const uint32_t *>(buf);
    offset += sizeof(num);

    for (uint32_t i = 0; i < num; i++) {
        ids.emplace_back();
        DESERIALIZE_FIELD(buf, offset, ids.back().first);
        DESERIALIZE_FIELD(buf, offset, ids.back().second);
    }

    return offset;
}

}  // namespace lazylog
