#include <base/Decimal.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Streaming/ChunkSplitter.h>

#include <gtest/gtest.h>

namespace
{
size_t OFFSET = 100;
size_t ROWS = 12;
size_t CHUNKS = 3;

template <typename ColumnType, typename IntegerType>
void doInsertColumnNumber(DB::Chunk & chunk, size_t rows, size_t chunks, DB::DataTypePtr data_type)
{
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnType *>(col.get());

    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (DB::is_decimal<IntegerType>)
            col_ptr->insert(IntegerType(static_cast<typename IntegerType::NativeType>((i + OFFSET) % chunks)));
        else
            col_ptr->insert(static_cast<IntegerType>((i + OFFSET) % chunks));
    }

    chunk.addColumn(std::move(col));
}

template <typename NumberType, typename ColumnType, typename IntegerType>
void insertColumnNumber(DB::Chunk & chunk, size_t rows, size_t chunks)
{
    doInsertColumnNumber<ColumnType, IntegerType>(chunk, rows, chunks, std::make_shared<NumberType>());
}

template <typename DecimalType, typename ColumnType, typename IntegerType>
void insertColumnDecimal(DB::Chunk & chunk, size_t rows, size_t chunks, int32_t precision, int32_t scale)
{
    doInsertColumnNumber<ColumnType, IntegerType>(chunk, rows, chunks, std::make_shared<DecimalType>(precision, scale));
}

void insertColumnDateTime64(DB::Chunk & chunk, size_t rows, size_t chunks)
{
    doInsertColumnNumber<DB::ColumnDecimal<DB::DateTime64>, DB::Decimal64>(
        chunk, rows, chunks, std::make_shared<DB::DataTypeDateTime64>(3));
}

void insertColumnNumber(DB::Chunk & chunk, size_t rows, size_t chunks)
{
    insertColumnNumber<DB::DataTypeInt8, DB::ColumnInt8, Int8>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeInt16, DB::ColumnInt16, Int16>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeInt32, DB::ColumnInt32, Int32>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeInt64, DB::ColumnInt64, Int64>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeInt128, DB::ColumnInt128, Int128>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeInt256, DB::ColumnInt256, Int256>(chunk, rows, chunks);

    /// insertColumnNumber<DB::DataTypeUInt8, DB::ColumnUInt8, UInt8>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeUInt8, DB::ColumnUInt8, UInt8>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeUInt16, DB::ColumnUInt16, UInt16>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeUInt32, DB::ColumnUInt32, UInt32>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeUInt64, DB::ColumnUInt64, UInt64>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeUInt128, DB::ColumnUInt128, UInt128>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeUInt256, DB::ColumnUInt256, UInt256>(chunk, rows, chunks);

    insertColumnNumber<DB::DataTypeFloat32, DB::ColumnFloat32, float>(chunk, rows, chunks);
    insertColumnNumber<DB::DataTypeFloat64, DB::ColumnFloat64, double>(chunk, rows, chunks);

    insertColumnDecimal<DB::DataTypeDecimal32, DB::ColumnDecimal<DB::Decimal32>, DB::Decimal32>(chunk, rows, chunks, 9, 3);
    insertColumnDecimal<DB::DataTypeDecimal64, DB::ColumnDecimal<DB::Decimal64>, DB::Decimal64>(chunk, rows, chunks, 10, 3);
    insertColumnDecimal<DB::DataTypeDecimal128, DB::ColumnDecimal<DB::Decimal128>, DB::Decimal128>(chunk, rows, chunks, 19, 3);
    insertColumnDecimal<DB::DataTypeDecimal256, DB::ColumnDecimal<DB::Decimal256>, DB::Decimal256>(chunk, rows, chunks, 39, 3);

    insertColumnDateTime64(chunk, rows, chunks);
}

template <typename ColumnType>
void doInsertColumnString(DB::Chunk & chunk, size_t rows, size_t chunks, DB::DataTypePtr data_type)
{
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnType *>(col.get());

    for (size_t i = 0; i < rows; ++i)
        col_ptr->insert(std::to_string((i + OFFSET) % chunks));

    chunk.addColumn(std::move(col));
}

void insertColumnString(DB::Chunk & chunk, size_t rows, size_t chunks)
{
    doInsertColumnString<DB::ColumnString>(chunk, rows, chunks, std::make_shared<DB::DataTypeString>());
    doInsertColumnString<DB::ColumnFixedString>(chunk, rows, chunks, std::make_shared<DB::DataTypeFixedString>(16));
}

DB::Chunk getChunk(size_t rows, size_t chunks)
{
    DB::Chunk chunk;
    insertColumnString(chunk, rows, chunks);
    insertColumnNumber(chunk, rows, chunks);

    return chunk;
}
}

TEST(ChunkSplitter, Split)
{
    auto test = [](size_t num_rows, size_t num_chunks, std::function<std::vector<size_t>(size_t)> get_key_positions) {
        auto num_columns = getChunk(num_rows, num_chunks).getNumColumns();
        for (size_t i = 0; i < num_columns; ++i)
        {
            auto key_positions = get_key_positions(i);
            DB::Streaming::ChunkSplitter splitter(key_positions);
            auto chunk = getChunk(num_rows, num_chunks);
            auto chunks = splitter.split(chunk);
            ASSERT_EQ(chunks.size(), num_chunks);

            for (const auto & chunk_with_id : chunks)
            {
                const auto & columns = chunk_with_id.chunk.getColumns();

                for (auto key_position : key_positions)
                    for (size_t row = 0, rows = chunk_with_id.chunk.getNumRows(); row < rows - 1; ++row)
                        ASSERT_EQ(columns[key_position]->compareAt(row, row + 1, *columns[key_position], -1), 0);

                /// Check hash
                for (size_t row = 0, rows = chunk_with_id.chunk.getNumRows(); row < rows - 1; ++row)
                {
                    UInt128 key{};
                    SipHash hash;

                    for (auto key_position : key_positions)
                        columns[key_position]->updateHashWithValue(row, hash);

                    hash.get128(key);
                    ASSERT_EQ(key, chunk_with_id.id);
                }
            }

            for (size_t j = 0, chunk_size = chunks.size(); chunk_size > 0 && j < chunk_size - 1; ++j)
                ASSERT_NE(chunks[j].id, chunks[j + 1].id);
        }
    };

    /// Single key
    {
        test(ROWS, CHUNKS, [](size_t i) { return std::vector<size_t>(1, i); });
    }

    /// Incremental keys
    {
        test(ROWS, CHUNKS, [](size_t i) {
            std::vector<size_t> keys;
            keys.reserve(i + 1);

            for (size_t j = 0; j <= i; ++j)
                keys.push_back(j);

            return keys;
        });
    }

    /// Single row chunk
    {
        test(1, 1, [](size_t i) { return std::vector<size_t>(1, i); });
    }

    /// Empty chunk
    {
        test(0, 0, [](size_t i) { return std::vector<size_t>(1, i); });
    }
}
