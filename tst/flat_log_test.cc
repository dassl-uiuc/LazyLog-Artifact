#include "gtest/gtest.h"

#include "../src/dur_log/dur_log_flat.h"
#include "../src/dur_log/dur_log_flat_cli.h"

namespace lazylog {

class FlatLogTest : public testing::Test {
    protected:
    void SetUp() override {
        
        p.SetProperty("dur_log.mr_size_m", "1");
        p.SetProperty("test", "true");
        p.SetProperty("wait", "false");

        dlog.Initialize(p, nullptr);
    }

    void TearDown() override {
        dlog.Finalize();
    }

    Properties p;
    DurabilityLogFlat dlog;
    DurabilityLogFlatCli dcli;
};

TEST_F(FlatLogTest, TestAppend) {
    LogEntry e1, e2;
    e1.client_id = 42;
    e1.client_seq = 1;
    e1.data = "This is a test entry";
    e1.size = e1.data.size();

    dlog.AppendEntry(e1);

    Deserializer(e2, dlog.GetMR());
    EXPECT_EQ(e1.client_id, e2.client_id);
    EXPECT_EQ(e1.client_seq, e2.client_seq);
    EXPECT_EQ(e1.size, e2.size);
    EXPECT_EQ(e1.data, e2.data);
}

TEST_F(FlatLogTest, TestAppendRewind) {
    LogEntry e1, e2;
    e1.client_id = 42;
    e1.client_seq = 1;
    e1.data = "This is a test entry";
    e1.size = e1.data.size();

    size_t sz_mr = 1024 * 1024;

    size_t sz_e1 = GetSize(e1);
    int i;
    for (i = 0; i < sz_mr / sz_e1; i++) {
        dlog.AppendEntry(e1);
    }

    size_t offset = i * sz_e1, sz_p1, sz_p2;
    sz_p1 = sz_mr - offset;
    sz_p2 = offset + sz_e1 - sz_mr;
    dlog.AppendEntry(e1);

    uint8_t buf[1024];
    memcpy(buf, dlog.GetMR() + offset, sz_p1);
    memcpy(buf + sz_p1, dlog.GetMR(), sz_p2);
    Deserializer(e2, buf);
    EXPECT_EQ(e1.client_id, e2.client_id);
    EXPECT_EQ(e1.client_seq, e2.client_seq);
    EXPECT_EQ(e1.size, e2.size);
    EXPECT_EQ(e1.data, e2.data);
}

TEST_F(FlatLogTest, TestFetchEntries) {
    LogEntry e;
    e.client_id = 42;
    e.data = "This is a test entry";
    e.size = e.data.size();

    for (int i = 0; i < 100; i++) {
        e.client_seq = i;
        dlog.AppendEntry(e);
    }

    std::vector<LogEntry> es;

    EXPECT_EQ(dlog.FetchUnorderedEntries(es, 100), 100);
    es.clear();
    EXPECT_EQ(dlog.FetchUnorderedEntries(es, 500), 100);

    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(es[i].client_id, e.client_id);
        ASSERT_EQ(es[i].client_seq, i);
        ASSERT_EQ(es[i].size, e.size);
        ASSERT_EQ(es[i].data, e.data);
    }
}

TEST_F(FlatLogTest, TestDelEntries) {
    LogEntry e;
    e.data = "This is a test entry";
    e.size = e.data.size();

    for (int i = 0; i < 100; i++) {
        e.client_id = 42 + i % 3;
        e.client_seq = i;
        dlog.AppendEntry(e);
    }

    std::vector<LogEntry> es;
    std::vector<LogEntry::ReqID> max_seq_req_ids;

    EXPECT_EQ(dlog.FetchUnorderedEntries(es, 50), 50);
    dcli.ProcessFetchedEntries(es, max_seq_req_ids);

    EXPECT_EQ(dlog.DelOrderedEntries(max_seq_req_ids), 50);

    es.clear();
    EXPECT_EQ(dlog.FetchUnorderedEntries(es, 100), 50);
    for (int i = 0; i < 50; i++) {
        ASSERT_EQ(es[i].client_id, 42 + (50 + i) % 3);
        ASSERT_EQ(es[i].client_seq, 50 + i);
        ASSERT_EQ(es[i].size, e.size);
        ASSERT_EQ(es[i].data, e.data);
    }
}

TEST_F(FlatLogTest, TestDelEntriesReOrderedCase1) {
    LogEntry e;
    e.data = "This is a test entry";
    e.size = e.data.size();
    DurabilityLogFlat dlog_p;
    dlog_p.Initialize(p, nullptr);

    int i;
    for (i = 0; i < 15; i++) {
        e.client_id = 42 + i % 3;
        e.client_seq = i;
        dlog_p.AppendEntry(e);
        if (i < 9)
            dlog.AppendEntry(e);
    }

    // append reordered entries
    for (i = 9; i < 12; i++) {
        e.client_id = 42 + i % 3;
        e.client_seq = i + 3;
        dlog.AppendEntry(e);
    }

    for (i = 12; i < 15; i++) {
        e.client_id = 42 + i % 3;
        e.client_seq = i - 3;
        dlog.AppendEntry(e);
    }
    /**
     * P: 1 2 3 4 5
     * B: 1 2 3 5 4
     */

    std::vector<LogEntry> es;
    std::vector<LogEntry::ReqID> max_seq_req_ids;

    EXPECT_EQ(dlog_p.FetchUnorderedEntries(es, 15), 15);
    dcli.ProcessFetchedEntries(es, max_seq_req_ids);
    EXPECT_EQ(dlog_p.DelOrderedEntries(max_seq_req_ids), 15);
    EXPECT_EQ(dlog.DelOrderedEntries(max_seq_req_ids), 12);

    for (i = 15; i < 18; i++) {
        e.client_id = 42 + i % 3;
        e.client_seq = i;
        dlog_p.AppendEntry(e);
        dlog.AppendEntry(e);
    }

    es.clear();
    max_seq_req_ids.clear();
    EXPECT_EQ(dlog_p.FetchUnorderedEntries(es, 15), 3);
    dcli.ProcessFetchedEntries(es, max_seq_req_ids);
    EXPECT_EQ(dlog.DelOrderedEntries(max_seq_req_ids), 6);
}

TEST_F(FlatLogTest, TestDelEntriesReOrderedCase2) {
    LogEntry e;
    e.data = "This is a test entry";
    e.size = e.data.size();
    DurabilityLogFlat dlog_p;
    dlog_p.Initialize(p, nullptr);

    int i;
    for (i = 0; i < 12; i++) {
        e.client_id = 42 + i % 3;
        e.client_seq = i;
        if (i < 9)
            dlog.AppendEntry(e);
        dlog_p.AppendEntry(e);
    }

    // append reordered entries
    for (i = 9; i < 12; i++) {
        e.client_id = 42 + i % 3;
        e.client_seq = i + 3;
        dlog.AppendEntry(e);
    }

    for (i = 12; i < 15; i++) {
        e.client_id = 42 + i % 3;
        e.client_seq = i - 3;
        dlog.AppendEntry(e);
    }
    /**
     * P: 1 2 3 4
     * B: 1 2 3 5 4
     */

    std::vector<LogEntry> es;
    std::vector<LogEntry::ReqID> max_seq_req_ids;

    EXPECT_EQ(dlog_p.FetchUnorderedEntries(es, 15), 12);
    dcli.ProcessFetchedEntries(es, max_seq_req_ids);
    EXPECT_EQ(dlog_p.DelOrderedEntries(max_seq_req_ids), 12);
    EXPECT_EQ(dlog.DelOrderedEntries(max_seq_req_ids), 12);

    for (i = 12; i < 15; i++) {
        e.client_id = 42 + i % 3;
        e.client_seq = i;
        dlog_p.AppendEntry(e);
    }

    es.clear();
    max_seq_req_ids.clear();
    EXPECT_EQ(dlog_p.FetchUnorderedEntries(es, 15), 3);
    dcli.ProcessFetchedEntries(es, max_seq_req_ids);
    EXPECT_EQ(dlog.DelOrderedEntries(max_seq_req_ids), 3);
}

TEST_F(FlatLogTest, TestSpecRead) {
    LogEntry e1, e2;
    e1.data = "This is a test entry";
    e1.size = e1.data.size();
    e1.client_id = 42;

    int i;
    for (i = 0; i < 10; i++) {
        e1.client_seq = i;
        dlog.AppendEntry(e1);
    }

    std::vector<LogEntry> es;
    std::vector<LogEntry::ReqID> max_seq_req_ids;

    EXPECT_EQ(dlog.FetchUnorderedEntries(es, 10), 10);
    dcli.ProcessFetchedEntries(es, max_seq_req_ids);
    EXPECT_EQ(dlog.DelOrderedEntries(max_seq_req_ids), 10);

    for (; i < 20; i++) {
        e1.client_seq = i;
        dlog.AppendEntry(e1);
    }

    for (int j = 0; j < 25; j++) {
        if (j < 10 || j >= 20)
            EXPECT_FALSE(dlog.SpecReadEntry(j, e2) > 0);
        else {
            EXPECT_TRUE(dlog.SpecReadEntry(j, e2) > 0);
            EXPECT_EQ(e2.client_id, 42);
            EXPECT_EQ(e2.log_idx, j);
            EXPECT_EQ(e2.data, e1.data);
        }
    }
}

} // namespace lazylog

