#include "../src/rpc/log_entry.h"

#include <vector>
#include <cstdlib>
#include <ctime>

#include "gtest/gtest.h"

namespace lazylog {

TEST(LogEntryTest, TestSerialization) {
    LogEntry e1, e2;
    size_t expected_size = 0;
    e1.client_id = 42;
    expected_size += sizeof(e1.client_id);
    e1.client_seq = 42;
    expected_size += sizeof(e1.client_seq);
    e1.log_idx = 42;
    expected_size += sizeof(e1.log_idx);
    e1.flags = 1;
    expected_size += sizeof(e1.flags);
    e1.view = 0;
    expected_size += sizeof(e1.view);
    e1.data = "This is a test log entry";
    expected_size += (e1.data.size() + 3) & (~3);
    e1.size = e1.data.size();
    expected_size += sizeof(e1.size);

    uint8_t *buf = new uint8_t[512];

    EXPECT_EQ(Serializer(e1, buf), expected_size);
    EXPECT_EQ(Deserializer(e2, buf), expected_size);

    EXPECT_EQ(e1, e2);

    delete buf;
}

TEST(LogEntryTest, TestMultiSerialization) {
    std::vector<LogEntry> es1, es2;
    size_t expected_size = 0;

    expected_size += sizeof(uint32_t);

    for (int i = 0; i < 10; i++) {
        es1.emplace_back();
        LogEntry &e = es1.back();
        e.client_id = 42 + i;
        expected_size += sizeof(e.client_id);
        e.client_seq = 42 + i;
        expected_size += sizeof(e.client_seq);
        e.log_idx = 42;
        expected_size += sizeof(e.log_idx);
        e.flags = 1;
        expected_size += sizeof(e.flags);
        e.view = 0;
        expected_size += sizeof(e.view);
        e.data = "This is a test log entry";
        expected_size += (e.data.size() + 3) & (~3);
        e.size = e.data.size();
        expected_size += sizeof(e.size);
    }

    uint8_t *buf = new uint8_t[2048];

    EXPECT_EQ(MultiSerializer(es1, buf), expected_size);
    EXPECT_EQ(MultiDeserializer(es2, buf), expected_size);

    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(es1[i], es2[i]);
    }

    delete buf;
}

TEST(LogEntryTest, TestMultiSerializationRange) {
    std::vector<LogEntry> es1, es2;
    size_t expected_size = 0;

    expected_size += sizeof(uint32_t);

    for (int i = 0; i < 10; i++) {
        es1.emplace_back();
        LogEntry &e = es1.back();
        e.client_id = 42 + i;
        e.client_seq = 42 + i;
        e.log_idx = 42;
        e.flags = 1;
        e.data = "This is a test log entry";
        e.size = e.data.size();
        if ((i < 5) && (i % 2 == 0))
            expected_size += GetSize(e);
    }

    uint8_t *buf = new uint8_t[2048];

    uint64_t to = 5;
    EXPECT_EQ(MultiSerializer(es1, 0, to, 2, buf), expected_size);
    EXPECT_EQ(to, 4);
    EXPECT_EQ(MultiDeserializer(es2, buf), expected_size);
}

TEST(LogEntryTest, TestMultiDeserializerSize) {
    std::vector<LogEntry> es1, es2;
    size_t deserialize_size = 0;

    for (int i = 0; i < 10; i++) {
        es1.emplace_back();
        LogEntry &e = es1.back();
        e.client_id = 42 + i;
        e.client_seq = 42 + i;
        e.log_idx = i;
        e.flags = 1;
        e.data = "This is a test log entry";
        e.size = e.data.size();
        if (i < 5) deserialize_size += GetSize(e);  // only deserialize first 5 entries
    }

    uint8_t *buf = new uint8_t[2048];

    MultiSerializer(es1, buf);
    EXPECT_EQ(MultiDeserializer(es2, buf + sizeof(uint32_t), deserialize_size), deserialize_size);

    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(es1[i], es2[i]);
    }

    delete buf;
}

TEST(LogEntryTest, TestDeserializeInvalid) {
    std::vector<LogEntry> es1, es2, es3;
    size_t valid_size = 0, serialized_size;

    for (int i = 0; i < 10; i++) {
        es1.emplace_back();
        LogEntry &e = es1.back();
        e.client_id = 42 + i;
        e.client_seq = 42 + i;
        e.log_idx = i;
        e.flags = 0;
        e.data = "This is a test log entry";
        e.size = e.data.size();
        if (i < 5) {
            e.flags = 1;
            valid_size += GetSize(e);  // only deserialize first 5 entries
        }
    }

    uint8_t *buf = new uint8_t[2048];
    serialized_size = MultiSerializer(es1, buf);
    EXPECT_EQ(MultiDeserializer(es2, buf), valid_size + sizeof(uint32_t));
    EXPECT_EQ(es2.size(), 5);
    EXPECT_EQ(MultiDeserializer(es3, buf + sizeof(uint32_t), serialized_size), valid_size);
    EXPECT_EQ(es3.size(), 5);

    delete buf;
}

TEST(LogEntryTest, TestMultiDeserializerMaxSize) {
    std::vector<LogEntry> es1, es2, es3;
    size_t deserialize_size = 0;

    for (int i = 0; i < 10; i++) {
        es1.emplace_back();
        LogEntry &e = es1.back();
        e.client_id = 42 + i;
        e.client_seq = 42 + i;
        e.log_idx = i;
        e.flags = 1;
        e.data = "This is a test log entry";
        e.size = e.data.size();
        deserialize_size += GetSize(e);
    }

    uint8_t *buf = new uint8_t[2048];
    MultiSerializer(es1, buf);
    EXPECT_EQ(MultiDeserializerMaxSize(es2, buf + sizeof(uint32_t), deserialize_size - deserialize_size / 10 / 2), deserialize_size / 10 * 9);
    EXPECT_EQ(MultiDeserializerMaxSize(es3, buf + sizeof(uint32_t), deserialize_size / 10 * 9 + MetaDataSize() / 2), deserialize_size / 10 * 9);

    for (int i = 0; i < 9; i++) {
        EXPECT_EQ(es1[i], es2[i]);
        EXPECT_EQ(es1[i], es3[i]);
    }

    delete buf;
}

TEST(LogEntryTest, TestSerializationPerf) {
    using namespace std::chrono;
    std::vector<LogEntry> es(1000000, "This is a test log entry This is a test log entry This is a test log entry This is a test log entry.");
    uint8_t *buf = new uint8_t[200000000];

    auto start = high_resolution_clock::now();

    MultiSerializer(es, buf);

    auto end = high_resolution_clock::now();

    std::cout << "Total time " << duration_cast<microseconds>(end - start).count() << "us" << std::endl;
}

TEST(ReqIdTest, TestReqIdSerialization) {
    std::vector<LogEntry::ReqID> ids1, ids2;
    std::srand(std::time(nullptr));
    size_t expected_size = 0;

    expected_size += sizeof(uint32_t);

    for (int i = 0; i < 10; i++) {
        ids1.emplace_back(std::rand(), std::rand());
        expected_size += sizeof(LogEntry::ReqID::first) + sizeof(LogEntry::ReqID::second);
    }

    uint8_t *buf = new uint8_t[2048];

    EXPECT_EQ(ReqIdSerializer(ids1, buf), expected_size);
    EXPECT_EQ(ReqIdDeserializer(ids2, buf), expected_size);

    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(ids1[i], ids2[i]);
    }

    delete buf;
}

}  // namespace lazylog
