#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace lazylog {

#pragma pack(4)
struct LogEntry {
    uint64_t client_id;
    uint64_t client_seq;
    uint64_t log_idx;
    uint32_t size;
    uint16_t flags;
    uint16_t view;
    std::string data;

    LogEntry();
    LogEntry(const char *data);

    using ReqID = std::pair<decltype(client_id), decltype(client_seq)>;
    friend std::ostream &operator<<(std::ostream &os, const LogEntry &e);
    bool operator==(const LogEntry &e) const;
};
#pragma pack()

inline size_t FlagPos() {
    return sizeof(LogEntry::client_id) + sizeof(LogEntry::client_seq) + sizeof(LogEntry::log_idx) +
           sizeof(LogEntry::size);
}

inline size_t MetaDataSize() { return FlagPos() + sizeof(LogEntry::flags) + sizeof(LogEntry::view); }

inline decltype(LogEntry::flags) GetFlagFromBuf(const uint8_t *buf) {
    return *reinterpret_cast<const decltype(LogEntry::flags) *>(buf + FlagPos());
}

inline void SetFlag(uint8_t *buf, decltype(LogEntry::flags) flag) {
    *reinterpret_cast<decltype(LogEntry::flags) *>(buf + FlagPos()) = flag;
}

size_t GetSize(const LogEntry &e);

size_t GetSizeFromBuf(const uint8_t *buf);

uint64_t GetClientId(const uint8_t *buf);

uint64_t GetClientSeq(const uint8_t *buf);

uint64_t GetIdx(const uint8_t *buf);

size_t Serializer(const LogEntry &e, uint8_t *buf);

size_t Deserializer(LogEntry &e, const uint8_t *buf);

size_t MultiSerializer(const std::vector<LogEntry> &es, uint8_t *buf);

/**
 * Serialize a subsequence of the entries
 * @param from first entry
 * @param num total number of entries to serialize
 * @param wo_num do not add num in the beginning
 */
size_t MultiSerializer(const std::vector<LogEntry> &es, uint64_t from, uint32_t num, uint8_t *buf, bool wo_num = false);

/**
 * Process the buffer and build the map of sequence numbers to file offsets
 * @param map map to build
 * @param buf buffer containing serialized entries
 * @param curr_file_offset current offset into file
 * @return bytes of data processed
 */

size_t ProcessAndBuildMap(std::map<uint64_t, uint64_t> &map, const uint8_t *buf, const uint64_t curr_file_offset);

/**
 * Process the buffer and build the map of sequence numbers to file offsets, without number of entries at the beginning
 * @param map map to build
 * @param buf buffer containing serialized entries
 * @param size total size of the buf (exact)
 * @param curr_file_offset current offset into file
 * @return bytes of data processed
 */
std::pair<size_t, uint32_t> ProcessAndBuildMap(std::map<uint64_t, uint64_t> &map, const uint8_t *buf, const size_t size,
                                               const uint64_t curr_file_offset);
/**
 * Serialize a subsequence of the entries, with a fixed interval between every two entries
 * @param from idx of the first entry to serialize
 * @param to upper-bound of the last entry to serialize, will be updated with the read idx of the last serialized
 * entry
 * @param itrv interval of entries in the original sequence
 */
size_t MultiSerializer(const std::vector<LogEntry> &es, uint64_t from, uint64_t &to, uint32_t itrv, uint8_t *buf);

/**
 * Deserializer for buf with num of entries at the beginning
 */
size_t MultiDeserializer(std::vector<LogEntry> &es, const uint8_t *buf);

/**
 * Deserializer for buf without number of entries at the beginning
 * @param size total size of the buf (exact)
 * @return actual deserialized size
 */
size_t MultiDeserializer(std::vector<LogEntry> &es, const uint8_t *buf, const size_t size);

size_t MultiDeserializerMaxSize(std::vector<LogEntry> &es, const uint8_t *buf, const size_t max_size);

uint32_t MultiDeserializer(uint32_t num, std::vector<LogEntry> &es, const uint8_t *buf);

size_t ReqIdSerializer(const std::vector<LogEntry::ReqID> &ids, uint8_t *buf);

size_t ReqIdDeserializer(std::vector<LogEntry::ReqID> &ids, const uint8_t *buf);

const int min_entry_size = MetaDataSize();  // 8+8+8+8 round up to power of 2

}  // namespace lazylog
