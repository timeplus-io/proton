#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/LightChunk.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Streaming/AsofRowRefs.h>

#include <gtest/gtest.h>

namespace
{
DB::LightChunk prepareChunk(size_t rows, size_t start_value)
{
    DB::Columns columns;
    {
        auto type = std::make_shared<DB::DataTypeUInt64>();
        auto mutable_col = type->createColumn();
        auto * col = typeid_cast<DB::ColumnUInt64 *>(mutable_col.get());

        for (size_t i = 0; i < rows; ++i)
            col->insertValue(start_value + i);

        columns.push_back(std::move(mutable_col));
    }

    {
        auto type = std::make_shared<DB::DataTypeString>();
        auto mutable_col = type->createColumn();
        auto * col = typeid_cast<DB::ColumnString *>(mutable_col.get());

        for (size_t i = 0; i < rows; ++i)
        {
            auto s = std::to_string(start_value + i);
            col->insertData(s.data(), s.size());
        }
        columns.push_back(std::move(mutable_col));
    }

    return DB::LightChunk(std::move(columns));
}

void commonTest(size_t keys, size_t page_size, size_t total_pages, size_t keep_versions)
{
    using BlockPages = DB::Streaming::RefCountDataBlockPages<DB::LightChunk>;
    DB::Streaming::CachedBlockMetrics metrics;

    /// Block pages holds the source blocks
    BlockPages block_pages(page_size, metrics);

    size_t chunk_rows = (keys / total_pages / page_size);

    DB::Streaming::PagedAsofRowRefs<DB::LightChunk> paged_rows_refs(DB::TypeIndex::UInt64);

    /// [1, keys]
    /// Then we can have 0 as lower bound, keys + 1 as upper bound
    for (size_t k = 1; k <= keys; k += chunk_rows)
    {
        auto chunk = prepareChunk(chunk_rows, k);

        /// First add to source block pages
        block_pages.add(std::move(chunk));
        const auto & asof_column = block_pages.lastDataBlock().getColumns()[0];

        /// Index it in row refs
        for (size_t r = 0; r < chunk_rows; ++r)
            paged_rows_refs.insert(DB::TypeIndex::UInt64, *asof_column, &block_pages, r, DB::ASOFJoinInequality::Less, keep_versions);
    }

    auto pages_kept_around = keep_versions / page_size / chunk_rows;
    if ((keep_versions <= keys) && (keep_versions % (page_size * chunk_rows)))
        ++pages_kept_around;

    ASSERT_EQ(block_pages.size(), pages_kept_around);

    auto blocks_kept_around = keep_versions / chunk_rows;
    if ((keep_versions <= keys) && (keep_versions % chunk_rows))
        ++blocks_kept_around;

    ASSERT_EQ(metrics.current_total_blocks, blocks_kept_around);

    /// We have kept the last `keep_versions` rows
    /// So the keys we kept around is [keys - keep_versions + 1, keys] if keep_versions <= keys
    /// otherwise, the keys we kept around is [1, keys] if keep_versions > keys
    ///
    /// [0, keys + 1]
    auto probe_chunk = prepareChunk(keys + 2, /*start_value=*/0); /// One extra lower bound key `0`, one extra upper bound key `keys + 1`
    const auto & asof_column = probe_chunk.getColumns()[0];
    for (size_t row = 0; row < keys + 2; ++row)
    {
        /// Less => source column < target column
        const auto * result = paged_rows_refs.findAsof(DB::TypeIndex::UInt64, DB::ASOFJoinInequality::Less, *asof_column, row);

        if (keep_versions <= keys)
        {
            /// [keys - keep_versions + 1, keys] are kept around
            /// For less, source keys in range [0, keys) can find a match
            if (row < keys)
            {
                ASSERT_NE(result, nullptr);
                const auto & found_columns = result->block().getColumns();
                if (row < keys - keep_versions)
                    ASSERT_EQ(found_columns[0]->getUInt(result->row_num), keys - keep_versions + 1);
                else
                    ASSERT_EQ(asof_column->getUInt(row) + 1, found_columns[0]->getUInt(result->row_num));
            }
            else
            {
                if (result)
                {
                    const auto & found_columns = result->block().getColumns();
                    std::cout << found_columns[0]->getUInt(result->row_num) << " " << row << "\n";
                }
                ASSERT_EQ(result, nullptr);
            }
        }
        else
        {
            /// All keys [1, keys] are kept around, so except the last 2 key (key = keys, keys + 1) can't be found (Less),
            /// other keys shall find a perfect match
            if (row < keys)
            {
                ASSERT_NE(result, nullptr);
                const auto & found_columns = result->block().getColumns();
                ASSERT_EQ(asof_column->getUInt(row) + 1, found_columns[0]->getUInt(result->row_num));
            }
            else
            {
                ASSERT_EQ(result, nullptr);
            }
        }
    }
#if 0
        if (keep_versions <= keys)
        {
            /// [keys - keep_version + 1, keys] are kept around
            /// For less, source keys in range [keys - keep_version + 2, keys] can find a perfect match
            if (row < keys - keep_versions + 2)
            {
               ASSERT_EQ(result, nullptr);
            }
            else
            {
                ASSERT_NE(result, nullptr);
                const auto & found_columns = result->block().getColumns();
                ASSERT_EQ(asof_column->getUInt(row), found_columns[0]->getUInt(result->row_num) + 1);
            }
        }
        else
        {
            /// All keys [1, keys] are kept around, so except the first 2 key (key = 0, 1) can't be found (Less),
            /// other keys shall find a perfect match
            if (row <= 1)
            {
                ASSERT_NE(result, nullptr);
                const auto & found_columns = result->block().getColumns();
                ASSERT_EQ(asof_column->getUInt(row), found_columns[0]->getUInt(result->row_num) + 1);
            }
            else
            {
                ASSERT_EQ(result, nullptr);
            }
        }
    }
#endif
}
}

TEST(PagedAsofRowRefs, InsertAndFind)
{
    commonTest(/*keys=*/1024, /*page_size=*/16, /*total_pages=*/8, /*keep_versions=*/3);
    //    commonTest(/*keys=*/1024, /*page_size=*/16, /*total_pages=*/8, /*keep_versions=*/8);
    //    commonTest(/*keys=*/1024, /*page_size=*/16, /*total_pages=*/8, /*keep_versions=*/9);
    //    commonTest(/*keys=*/1024, /*page_size=*/16, /*total_pages=*/8, /*keep_versions=*/16);
    //    commonTest(/*keys=*/1024, /*page_size=*/16, /*total_pages=*/8, /*keep_versions=*/17);
    //    commonTest(/*keys=*/1024, /*page_size=*/16, /*total_pages=*/8, /*keep_versions=*/1000);
    //    commonTest(/*keys=*/1024, /*page_size=*/16, /*total_pages=*/8, /*keep_versions=*/1024);
    //    commonTest(/*keys=*/1024, /*page_size=*/16, /*total_pages=*/8, /*keep_versions=*/1025);
}