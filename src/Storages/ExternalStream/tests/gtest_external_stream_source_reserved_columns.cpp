#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/ExternalStream/ExternalStreamSource.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageValues.h>

#include <Formats/FormatSettings.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

class TestExternalStreamSource final : public ExternalStreamSource
{
public:
    TestExternalStreamSource(const Block & header_, StorageSnapshotPtr storage_snapshot_, ContextPtr query_context_)
        : ExternalStreamSource(
              header_, std::move(storage_snapshot_), query_context_->getSettingsRef().max_block_size.value, std::move(query_context_))
    {
    }

    void init(const String & data_format, const FormatSettings & format_settings)
    {
        getPhysicalHeader();
        format_executor = getInputFormatExecutor(data_format, format_settings).first;
    }

    const Block & getPhysicalHeaderForTest() const { return physical_header; }

    size_t execute(const String & data)
    {
        ReadBufferFromString buf(data);
        return format_executor->execute(buf);
    }

    MutableColumns getResultColumnsForTest() { return format_executor->getResultColumns(); }

    std::shared_ptr<StreamingFormatExecutor> format_executor;
};

ColumnsDescription makeColumns()
{
    return ColumnsDescription{
        {"key", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeInt32>()},
        {ProtonConsts::RESERVED_EVENT_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")},
    };
}

struct StorageSnapshotFixture
{
    StoragePtr storage;
    StorageSnapshotPtr snapshot;
};

StorageSnapshotFixture makeSnapshotFixture(const ColumnsDescription & columns)
{
    Block sample_block;
    for (const auto & name_type : columns.getOrdinary())
        sample_block.insert({name_type.type->createColumn(), name_type.type, name_type.name});

    auto res_block = sample_block.cloneEmpty();
    auto storage = StorageValues::create(StorageID("default", "gtest_values"), columns, res_block);
    auto snapshot = std::make_shared<StorageSnapshot>(*storage, storage->getInMemoryMetadataPtr());

    return {.storage = std::move(storage), .snapshot = std::move(snapshot)};
}

}

TEST(ExternalStreamSource, TreatsTpTimeAsVirtualEvenIfDefinedInSchema)
{
    tryRegisterFormats();
    auto context = Context::createCopy(getContext().context);

    auto columns = makeColumns();
    auto fixture = makeSnapshotFixture(columns);

    /// Query header selects only a subset of columns, but includes `_tp_time` as a reserved column.
    Block query_header;
    auto string_type = std::make_shared<DataTypeString>();
    auto datetime64_type = std::make_shared<DataTypeDateTime64>(3, "UTC");
    query_header.insert({string_type->createColumn(), string_type, "key"});
    query_header.insert({datetime64_type->createColumn(), datetime64_type, ProtonConsts::RESERVED_EVENT_TIME});

    TestExternalStreamSource source(query_header, fixture.snapshot, context);
    source.init("CSV", FormatSettings{});

    const auto & physical_header = source.getPhysicalHeaderForTest();
    ASSERT_EQ(physical_header.columns(), 1U);
    ASSERT_TRUE(physical_header.tryGetPositionByName("key").has_value());
    ASSERT_FALSE(physical_header.tryGetPositionByName(ProtonConsts::RESERVED_EVENT_TIME).has_value());

    /// Input contains only the physical payload fields: key, value.
    /// The `value` field must be skipped via column mapping. `_tp_time` is populated from transport metadata.
    ASSERT_EQ(source.execute("one,1\n"), 1U);

    auto cols = source.getResultColumnsForTest();
    ASSERT_EQ(cols.size(), 1U);
    EXPECT_EQ(cols[0]->size(), 1U);
}

TEST(ExternalStreamSource, CanParsePayloadWithoutTpTimeEvenIfDefinedInSchema)
{
    tryRegisterFormats();
    auto context = Context::createCopy(getContext().context);

    auto columns = makeColumns();
    auto fixture = makeSnapshotFixture(columns);

    /// Stream schema includes `_tp_time`, but it is populated from transport metadata and is not part of payload.
    Block query_header;
    auto string_type = std::make_shared<DataTypeString>();
    auto int32_type = std::make_shared<DataTypeInt32>();
    query_header.insert({string_type->createColumn(), string_type, "key"});
    query_header.insert({int32_type->createColumn(), int32_type, "value"});

    TestExternalStreamSource source(query_header, fixture.snapshot, context);
    source.init("CSV", FormatSettings{});

    const auto & physical_header = source.getPhysicalHeaderForTest();
    ASSERT_EQ(physical_header.columns(), 2U);
    ASSERT_TRUE(physical_header.tryGetPositionByName("key").has_value());
    ASSERT_TRUE(physical_header.tryGetPositionByName("value").has_value());

    /// Input contains only the physical payload fields: key, value.
    ASSERT_EQ(source.execute("one,1\n"), 1U);

    auto cols = source.getResultColumnsForTest();
    ASSERT_EQ(cols.size(), 2U);
    EXPECT_EQ(cols[0]->size(), 1U);
    EXPECT_EQ(cols[1]->size(), 1U);
}

TEST(ExternalStreamSource, SkipsVirtualTpMessageKeyInPhysicalHeaderAndMapping)
{
    tryRegisterFormats();
    auto context = Context::createCopy(getContext().context);

    ColumnsDescription columns{
        {ProtonConsts::RESERVED_MESSAGE_KEY, std::make_shared<DataTypeString>()},
        {"key", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeInt32>()},
    };
    auto fixture = makeSnapshotFixture(columns);

    /// Query selects `_tp_message_key` and a physical payload column. The payload does not contain `_tp_message_key`.
    Block query_header;
    auto string_type = std::make_shared<DataTypeString>();
    query_header.insert({string_type->createColumn(), string_type, ProtonConsts::RESERVED_MESSAGE_KEY});
    query_header.insert({string_type->createColumn(), string_type, "key"});

    TestExternalStreamSource source(query_header, fixture.snapshot, context);
    source.init("CSV", FormatSettings{});

    /// `_tp_message_key` is transport metadata and should never appear in the physical header.
    const auto & physical_header = source.getPhysicalHeaderForTest();
    ASSERT_EQ(physical_header.columns(), 1U);
    ASSERT_TRUE(physical_header.tryGetPositionByName("key").has_value());

    /// Input contains only the physical payload fields: key, value.
    /// The `value` field must be skipped via column mapping.
    ASSERT_EQ(source.execute("one,1\n"), 1U);

    auto cols = source.getResultColumnsForTest();
    ASSERT_EQ(cols.size(), 1U);
    EXPECT_EQ(cols[0]->size(), 1U);
}
