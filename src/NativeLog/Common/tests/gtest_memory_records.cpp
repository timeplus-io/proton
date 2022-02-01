#include <NativeLog/Schemas/MemoryRecords.h>

#include <gtest/gtest.h>

namespace
{
std::string HEADER_KEY_TEMPLATE = "key";
std::vector<int8_t> HEADER_VALUE_TEMPLATE = {'v', 'a', 'l', 'u', 'e'};
std::vector<int8_t> KEY_DATA_TEMPLATE = {'a', 'b', 'c', 'd', 'e', 'f'};
std::vector<int8_t> VALUE_DATA_TEMPLATE = {'i', 'j', 'k', 'l', 'm', 'n'};

flatbuffers::Offset<nlog::Record> createRecord(flatbuffers::FlatBufferBuilder & fbb, int8_t batch_delta, int8_t record_delta)
{
    /// Build header
    std::vector<int8_t> head_value{HEADER_VALUE_TEMPLATE};
    std::for_each(head_value.begin(), head_value.end(), [batch_delta, record_delta](auto & k) { k += batch_delta + record_delta; });
    auto header = nlog::CreateRecordHeader(
        fbb,
        fbb.CreateString(HEADER_KEY_TEMPLATE + std::to_string(batch_delta) + std::to_string(record_delta)),
        fbb.CreateVector(head_value));

    /// Build record
    std::vector<int8_t> key_data{KEY_DATA_TEMPLATE};
    std::for_each(key_data.begin(), key_data.end(), [batch_delta, record_delta](auto & k) { k += batch_delta + record_delta; });

    std::vector<int8_t> value_data{VALUE_DATA_TEMPLATE};
    std::for_each(value_data.begin(), value_data.end(), [batch_delta, record_delta](auto & k) { k += batch_delta + record_delta; });

    return nlog::CreateRecord(
        fbb,
        0 + batch_delta + record_delta,
        0 + batch_delta + record_delta,
        fbb.CreateVector(key_data),
        fbb.CreateVector(value_data),
        fbb.CreateVector(std::vector<decltype(header)>{header}));
}

flatbuffers::Offset<nlog::RecordBatch> createBatch(flatbuffers::FlatBufferBuilder & fbb, int8_t batch_delta)
{
    auto record1 = createRecord(fbb, batch_delta, 0);
    auto record2 = createRecord(fbb, batch_delta, 1);

    /// Build record batch
    return nlog::CreateRecordBatch(
        fbb,
        123 + batch_delta,
        nlog::MemoryRecords::FLAGS_MAGIC | 0X8000000000000000 | 0X1,
        1000000 + batch_delta,
        1 + batch_delta,
        2 + batch_delta,
        0 + batch_delta,
        0 + batch_delta,
        0 + batch_delta,
        -1 + batch_delta,
        -1 + batch_delta,
        -1 + batch_delta,
        fbb.CreateVector(std::vector<decltype(record1)>{record1, record2}));
}
}

TEST(MemoryRecords, serde)
{
    for (int8_t i = 0; i < 2; ++i)
    {
        flatbuffers::FlatBufferBuilder fbb;
        fbb.ForceDefaults(true);

        auto batch = createBatch(fbb, i);
        nlog::FinishSizePrefixedRecordBatchBuffer(fbb, batch);
        nlog::MemoryRecords records{fbb.GetBufferSpan()};
        EXPECT_TRUE(records.isMagicValid());
        EXPECT_TRUE(records.removed());
        EXPECT_EQ(records.version(), nlog::RecordVersion::V1);
        EXPECT_EQ(records.baseOffset(), 1000000 + i);
        EXPECT_EQ(records.appendTimestamp(), 0 + i);
        EXPECT_EQ(records.maxTimestamp(), 0 + i);
        records.setAppendTimestamp(123456);
        records.setMaxTimestamp(123457);
        records.setBaseOffset(2000000);
        EXPECT_EQ(records.appendTimestamp(), 123456);
        EXPECT_EQ(records.appendTimestamp(), 123456);
        EXPECT_EQ(records.baseOffset(), 2000000);
        EXPECT_EQ(records.maxTimestamp(), 123457);
    }
}
