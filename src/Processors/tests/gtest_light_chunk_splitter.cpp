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
#include <Processors/LightChunkSplitter.h>

#include <gtest/gtest.h>

namespace
{
size_t OFFSET = 100;
size_t ROWS = 120;
size_t SHARDS = 4;

template <typename ColumnType, typename IntegerType>
void doInsertColumnNumber(DB::Chunk & chunk, size_t rows, size_t shards, DB::DataTypePtr data_type)
{
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnType *>(col.get());

    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (DB::is_decimal<IntegerType>)
            col_ptr->insert(IntegerType(static_cast<typename IntegerType::NativeType>((i + OFFSET) % shards)));
        else
            col_ptr->insert(static_cast<IntegerType>((i + OFFSET) % shards));
    }

    chunk.addColumn(std::move(col));
}

template <typename NumberType, typename ColumnType, typename IntegerType>
void insertColumnNumber(DB::Chunk & chunk, size_t rows, size_t shards)
{
    doInsertColumnNumber<ColumnType, IntegerType>(chunk, rows, shards, std::make_shared<NumberType>());
}

template <typename DecimalType, typename ColumnType, typename IntegerType>
void insertColumnDecimal(DB::Chunk & chunk, size_t rows, size_t shards, int32_t precision, int32_t scale)
{
    doInsertColumnNumber<ColumnType, IntegerType>(chunk, rows, shards, std::make_shared<DecimalType>(precision, scale));
}

void insertColumnDateTime64(DB::Chunk & chunk, size_t rows, size_t shards)
{
    doInsertColumnNumber<DB::ColumnDecimal<DB::DateTime64>, DB::Decimal64>(
        chunk, rows, shards, std::make_shared<DB::DataTypeDateTime64>(3));
}

void insertColumnNumber(DB::Chunk & chunk, size_t rows, size_t shards)
{
    insertColumnNumber<DB::DataTypeInt8, DB::ColumnInt8, Int8>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeInt16, DB::ColumnInt16, Int16>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeInt32, DB::ColumnInt32, Int32>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeInt64, DB::ColumnInt64, Int64>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeInt128, DB::ColumnInt128, Int128>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeInt256, DB::ColumnInt256, Int256>(chunk, rows, shards);

    /// insertColumnNumber<DB::DataTypeUInt8, DB::ColumnUInt8, UInt8>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeUInt8, DB::ColumnUInt8, UInt8>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeUInt16, DB::ColumnUInt16, UInt16>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeUInt32, DB::ColumnUInt32, UInt32>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeUInt64, DB::ColumnUInt64, UInt64>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeUInt128, DB::ColumnUInt128, UInt128>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeUInt256, DB::ColumnUInt256, UInt256>(chunk, rows, shards);

    insertColumnNumber<DB::DataTypeFloat32, DB::ColumnFloat32, float>(chunk, rows, shards);
    insertColumnNumber<DB::DataTypeFloat64, DB::ColumnFloat64, double>(chunk, rows, shards);

    insertColumnDecimal<DB::DataTypeDecimal32, DB::ColumnDecimal<DB::Decimal32>, DB::Decimal32>(chunk, rows, shards, 9, 3);
    insertColumnDecimal<DB::DataTypeDecimal64, DB::ColumnDecimal<DB::Decimal64>, DB::Decimal64>(chunk, rows, shards, 10, 3);
    insertColumnDecimal<DB::DataTypeDecimal128, DB::ColumnDecimal<DB::Decimal128>, DB::Decimal128>(chunk, rows, shards, 19, 3);
    insertColumnDecimal<DB::DataTypeDecimal256, DB::ColumnDecimal<DB::Decimal256>, DB::Decimal256>(chunk, rows, shards, 39, 3);

    insertColumnDateTime64(chunk, rows, shards);
}

template <typename ColumnType>
void doInsertColumnString(DB::Chunk & chunk, size_t rows, size_t shards, DB::DataTypePtr data_type)
{
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnType *>(col.get());

    for (size_t i = 0; i < rows; ++i)
        col_ptr->insert(std::to_string((i + OFFSET) % shards));

    chunk.addColumn(std::move(col));
}

void insertColumnString(DB::Chunk & chunk, size_t rows, size_t shards)
{
    doInsertColumnString<DB::ColumnString>(chunk, rows, shards, std::make_shared<DB::DataTypeString>());
    doInsertColumnString<DB::ColumnFixedString>(chunk, rows, shards, std::make_shared<DB::DataTypeFixedString>(16));
}

DB::Chunk getChunk(size_t rows, size_t shards)
{
    DB::Chunk chunk;
    insertColumnString(chunk, rows, shards);
    insertColumnNumber(chunk, rows, shards);

    return chunk;
}

bool findDuplicateKey(const DB::ShardChunk & src_chunk, const DB::ShardChunks & chunks, const std::vector<size_t> & key_positions)
{
    auto rows = src_chunk.chunk.getNumRows();
    const auto & src_columns = src_chunk.chunk.getColumns();

    for (const auto & dst_chunk : chunks)
    {
        if (dst_chunk.shard == src_chunk.shard)
            continue;

        const auto & dst_columns = dst_chunk.chunk.getColumns();
        auto dst_rows = dst_columns[0]->size();

        for (size_t row = 0; row < rows; ++row)
        {
            for (size_t dst_row = 0; dst_row < dst_rows; ++dst_row)
            {
                for (auto key_col : key_positions)
                {
                    if (src_columns[key_col]->compareAt(row, dst_row, *dst_columns[key_col], -1) == 0)
                        return true;
                }
            }
        }
    }
    return false;
}
}

TEST(LightChunkSplitter, Split)
{
    auto test = [](size_t num_rows, size_t num_shards, std::function<std::vector<size_t>(size_t)> get_key_positions) {
        auto num_columns = getChunk(num_rows, num_shards).getNumColumns();
        for (size_t i = 0; i < num_columns; ++i)
        {
            auto key_positions = get_key_positions(i);
            DB::LightChunkSplitter splitter(key_positions, num_shards);
            auto chunk = getChunk(num_rows, num_shards);
            auto shard_chunks = splitter.split(chunk);
            ASSERT_TRUE(shard_chunks.size() <= num_shards);

            /// No duplicate shard_id
            std::unordered_map<uint16_t, size_t> shard_ids;
            shard_ids.reserve(shard_chunks.size());
            for (const auto & shard_chunk : shard_chunks)
                ++shard_ids[shard_chunk.shard];

            for (auto [shard, cnt] : shard_ids)
                ASSERT_EQ(cnt, 1);

            /// Same row shall never cross shards (no overlap)
            for (const auto & shard_chunk : shard_chunks)
            {
                auto found = findDuplicateKey(shard_chunk, shard_chunks, key_positions);
                ASSERT_FALSE(found);
            }
        }
    };

    /// Single key
    {
        test(ROWS, SHARDS, [](size_t i) { return std::vector<size_t>(1, i); });
    }

    /// Incremental keys
    {
        test(ROWS, SHARDS, [](size_t i) {
            std::vector<size_t> keys;
            keys.reserve(i + 1);

            for (size_t j = 0; j <= i; ++j)
                keys.push_back(j);

            return keys;
        });
    }

    /// Single row chunk
    {
        test(1, SHARDS, [](size_t i) { return std::vector<size_t>(1, i); });
    }
}
