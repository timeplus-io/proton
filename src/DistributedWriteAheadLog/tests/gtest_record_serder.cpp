#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DistributedWriteAheadLog/ByteVector.h>
#include <DistributedWriteAheadLog/OpCodes.h>
#include <DistributedWriteAheadLog/Record.h>

#include <gtest/gtest.h>


using namespace DB;
using namespace DWAL;
using namespace std;

std::unique_ptr<Record> createRecord()
{
    Block block;

    auto uint64_type = make_shared<DataTypeUInt64>();
    auto float64_type = make_shared<DataTypeFloat64>();
    auto datetime64_type = make_shared<DataTypeDateTime64>(3);
    auto string_type = make_shared<DataTypeString>();

    auto id_col = uint64_type->createColumn();
    /// auto id_col = make_shared<ColumnInt64>();
    auto * id_col_inner = typeid_cast<ColumnUInt64 *>(id_col.get());
    id_col_inner->insertValue(102);
    id_col_inner->insertValue(101);
    id_col_inner->insertValue(100);

    ColumnWithTypeAndName id_col_with_type{std::move(id_col), uint64_type, "id"};
    block.insert(id_col_with_type);

    auto cpu_col = float64_type->createColumn();
    /// auto cpu_col = make_shared<ColumnFloat64>();
    auto * cpu_col_inner = typeid_cast<ColumnFloat64 *>(cpu_col.get());
    cpu_col_inner->insertValue(13.338);
    cpu_col_inner->insertValue(17.378);
    cpu_col_inner->insertValue(11.539);

    ColumnWithTypeAndName cpu_col_with_type(std::move(cpu_col), float64_type, "cpu");
    block.insert(cpu_col_with_type);

    auto raw_col = string_type->createColumn();
    raw_col->insertData("hello", 5);
    raw_col->insertData("world", 5);
    raw_col->insertData("你好啊", 6);

    ColumnWithTypeAndName raw_col_with_type(std::move(raw_col), string_type, "raw");
    block.insert(raw_col_with_type);

    auto time_col = datetime64_type->createColumn();
    /// auto time_col = make_shared<ColumnDecimal<DateTime64>>;
    auto * time_col_inner = typeid_cast<ColumnDecimal<DateTime64> *>(time_col.get());
    time_col_inner->insertValue(1612286044.256326);
    time_col_inner->insertValue(1612296044.256326);
    time_col_inner->insertValue(1612276044.256326);

    ColumnWithTypeAndName time_col_with_type(std::move(time_col), datetime64_type, "_time");
    block.insert(time_col_with_type);

    return std::make_unique<Record>(OpCode::ADD_DATA_BLOCK, move(block));
}

void checkRecord(const std::vector<String> & cols, const Record & expect, const Record & actual)
{
    EXPECT_EQ(expect.op_code, actual.op_code);

    SipHash hash_expected;
    expect.block.updateHash(hash_expected);

    SipHash hash_got;
    actual.block.updateHash(hash_got);

    EXPECT_EQ(hash_expected.get64(), hash_got.get64());

    /// Compare columns

    for (const auto & col_name : cols)
    {
        const auto & col_expected = expect.block.getByName(col_name);
        const auto & col_got = actual.block.getByName(col_name);

        EXPECT_EQ(col_expected.name, col_got.name);
        EXPECT_EQ(col_expected.type->getTypeId(), col_got.type->getTypeId());
        EXPECT_EQ(col_expected.column->size(), 3);
        EXPECT_EQ(col_expected.column->size(), col_got.column->size());

        for (size_t i = 0; i < col_got.column->size(); ++i)
        {
            if (col_name == "id")
                EXPECT_EQ(col_expected.column->get64(i), col_got.column->get64(i));
            else if (col_name == "cpu" || col_name == "_time")
                EXPECT_EQ(col_expected.column->getFloat64(i), col_got.column->getFloat64(i));
            else if (col_name == "raw")
                EXPECT_EQ(col_expected.column->getDataAt(i), col_got.column->getDataAt(i));
            else
                ASSERT_TRUE(false);
        }
    }
}

TEST(CheckRecordSerializationDeserialization, Sender)
{
    auto r = createRecord();
    ByteVector data{Record::write(*r, false)};
    auto rr = Record::read(reinterpret_cast<char *>(data.data()), data.size());
    auto cols = std::vector<String>{"id", "cpu", "raw", "_time"};

    checkRecord(cols, *r, *rr);
}

TEST(CheckRecordSerializationDeserialization, Compression)
{
    auto r = createRecord();
    ByteVector data{Record::write(*r, true)};
    auto rr = Record::read(reinterpret_cast<char *>(data.data()), data.size());
    auto cols = std::vector<String>{"id", "cpu", "raw", "_time"};

    checkRecord(cols, *r, *rr);
}

TEST(CheckRecordSerializationDeserialization, WriteBenchmark)
{
    auto r = createRecord();

    int32_t n = 1000000;
    auto write_loop = [&](bool compressed) {
        for (auto i = 0; i < n; i++)
        {
            Record::write(*r, compressed);
        }
    };

    auto t = std::chrono::high_resolution_clock::now();
    write_loop(false);
    std::chrono::duration<double> duration0 = std::chrono::high_resolution_clock::now() - t;

    t = std::chrono::high_resolution_clock::now();
    write_loop(true);
    std::chrono::duration<double> duration1 = std::chrono::high_resolution_clock::now() - t;
    std::cout << "write uncompressed: " << int32_t(n / duration0.count()) << "/s write compressed: " << int32_t(n / duration1.count()) << "/s" << std::endl;
}

TEST(CheckRecordSerializationDeserialization, ReadBenchmark)
{
    auto r = createRecord();

    int32_t n = 1000000;
    auto read_loop = [&](bool compressed) {
    	ByteVector data{Record::write(*r, compressed)};
        for (auto i = 0; i < n; i++)
        {
            Record::read(reinterpret_cast<char *>(data.data()), data.size());
        }
    };

    auto t = std::chrono::high_resolution_clock::now();
    read_loop(false);
    std::chrono::duration<double> duration0 = std::chrono::high_resolution_clock::now() - t;

    t = std::chrono::high_resolution_clock::now();
    read_loop(true);
    std::chrono::duration<double> duration1 = std::chrono::high_resolution_clock::now() - t;
    std::cout << "read uncompressed: " << int32_t(n / duration0.count()) << "/s read compressed: " << int32_t(n / duration1.count()) << "/s" << std::endl;
}
