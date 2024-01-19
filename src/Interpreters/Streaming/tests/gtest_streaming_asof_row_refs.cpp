#include <Columns/ColumnDecimal.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/Streaming/CachedBlockMetrics.h>
#include <Interpreters/Streaming/RefCountDataBlockList.h>
#include <Interpreters/Streaming/RowRefs.h>

#include <gtest/gtest.h>

namespace
{

DB::Block prepareDateTime64Block(std::vector<int64_t> values)
{
    auto type = std::make_shared<DB::DataTypeDateTime64>(3);
    auto mutable_col = type->createColumn();
    auto * col = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(mutable_col.get());

    col->reserve(values.size());
    for (auto value : values)
        col->insertValue(value);

    return DB::Block{DB::ColumnWithTypeAndName{std::move(mutable_col), type, "_tp_time"}};
}

void fillDataBlockList(
    std::shared_ptr<DB::Streaming::RefCountDataBlockList<DB::Block>> blocks,
    std::vector<int64_t> values,
    std::function<void(DB::Streaming::RefCountDataBlockList<DB::Block> *, size_t)> callback)
{
    auto start_row = blocks->pushBackOrConcat(prepareDateTime64Block(std::move(values)));
    if (callback)
        callback(blocks.get(), start_row);
}

/// TODO, other types
std::shared_ptr<DB::Streaming::RefCountDataBlockList<DB::Block>> forEachRightBlock(
    size_t data_block_size,
    DB::Streaming::CachedBlockMetrics & join_metrics,
    std::function<void(DB::Streaming::RefCountDataBlockList<DB::Block> *, size_t)> callback = {})
{
    auto blocks = std::make_shared<DB::Streaming::RefCountDataBlockList<DB::Block>>(data_block_size, join_metrics);

    if (data_block_size > 0)
    {
        fillDataBlockList(blocks, {1'612'286'044'256}, callback);
        fillDataBlockList(blocks, {1'612'286'045'256}, callback);
        fillDataBlockList(blocks, {1'612'286'054'256}, callback);
        fillDataBlockList(blocks, {1'612'286'059'256}, callback);
        fillDataBlockList(blocks, {1'612'286'060'256}, callback);
        fillDataBlockList(blocks, {1'612'286'069'256}, callback);
    }
    else
    {
        fillDataBlockList(blocks, {1'612'286'044'256, 1'612'286'045'256}, callback);
        fillDataBlockList(blocks, {1'612'286'054'256, 1'612'286'059'256}, callback);
        fillDataBlockList(blocks, {1'612'286'060'256, 1'612'286'069'256}, callback);
    }

    return blocks;
}

DB::Block prepareLeftBlock(DB::Streaming::CachedBlockMetrics & join_metrics)
{
    auto blocks{forEachRightBlock(/*data_block_size=*/0, join_metrics)};
    DB::Block block;
    for (size_t i = 0; auto & b : *blocks)
    {
        if (i++ == 0)
        {
            block.swap(b.block);
        }
        else
        {
            auto & col = b.block.getByPosition(0).column;
            block.getByPosition(0).column->assumeMutable()->insertRangeFrom(*col, 0, col->size());
        }
    }

    auto * col = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(block.getByPosition(0).column->assumeMutable().get());
    col->insertValue(1'612'286'033'256);
    col->insertValue(1'612'286'080'256);

    return block;
}

struct Case
{
    size_t row_num;
    size_t keep_versions;
    DB::ASOFJoinInequality inequality;

    size_t expected_block_count;
    std::optional<size_t> expected_block_idx;
    std::optional<uint32_t> expected_matching_row;
};

void commonTest(size_t data_block_size, const std::vector<Case> & cases)
{
    for (const auto & test_case : cases)
    {
        DB::Streaming::CachedBlockMetrics join_metrics;
        {
            std::shared_ptr<DB::Streaming::RefCountDataBlockList<DB::Block>> ret_right_blocks;
            auto left_block{prepareLeftBlock(join_metrics)};

            auto & asof_col = left_block.getByPosition(0);
            DB::Streaming::AsofRowRefs<DB::Block> row_refs(asof_col.type->getTypeId());

            ret_right_blocks = forEachRightBlock(data_block_size, join_metrics, [&](auto * right_blocks, size_t start_row) {
                auto & last_block = right_blocks->lastDataBlock();
                auto & right_asof_col = last_block.getByPosition(0);
                for (size_t i = start_row, rows = last_block.rows(); i < rows; ++i)
                    row_refs.insert(
                        asof_col.type->getTypeId(), *right_asof_col.column, right_blocks, i, i, test_case.inequality, test_case.keep_versions);
            });

            auto result{row_refs.findAsof(asof_col.type->getTypeId(), test_case.inequality, *asof_col.column, test_case.row_num)};

            /// std::cout << "keep_versions=" << test_case.keep_versions << "\n";

            ASSERT_EQ(ret_right_blocks->size(), test_case.expected_block_count);

            if (test_case.expected_matching_row)
            {
                ASSERT_TRUE(result != nullptr);
                ASSERT_EQ(result->row_num, test_case.expected_matching_row.value());

                size_t block_idx = 0;
                for (auto iter = ret_right_blocks->begin(); iter != ret_right_blocks->end(); ++iter)
                {
                    if (iter == result->block_iter)
                        break;

                    ++block_idx;
                }

                ASSERT_EQ(block_idx, test_case.expected_block_idx.value());
            }
            else
                ASSERT_TRUE(result == nullptr);
        }
        ASSERT_EQ(join_metrics.total_blocks, 0);
    }
}
}

TEST(StreamingRowRefs, FindAsof)
{
    /// 0, col->insertValue(1'612'286'044'256); <- 0
    /// 1, col->insertValue(1'612'286'045'256); <- 1
    /// ---
    /// 0, col->insertValue(1'612'286'054'256); <- 2
    /// 1, col->insertValue(1'612'286'059'256); <- 3
    /// ---
    /// 0, col->insertValue(1'612'286'060'256); <- 4
    /// 1, col->insertValue(1'612'286'069'256); <- 5

    /// col->insertValue(1'612'286'033'256); <- 6
    /// col->insertValue(1'612'286'080'256); <- 7

    std::vector<Case> cases = {
        {.row_num = 0,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::Greater,
         .expected_block_count = 3,
         .expected_block_idx = {},
         .expected_matching_row = {}},
        {.row_num = 0,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::GreaterOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {0}},
        {.row_num = 0,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::Less,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {1}},
        {.row_num = 0,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::LessOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {0}},

        {.row_num = 0,
         .keep_versions = 5,
         .inequality = DB::ASOFJoinInequality::Greater,
         .expected_block_count = 3,
         .expected_block_idx = {},
         .expected_matching_row = {}},
        {.row_num = 0,
         .keep_versions = 5,
         .inequality = DB::ASOFJoinInequality::GreaterOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {},
         .expected_matching_row = {}},
        {.row_num = 0,
         .keep_versions = 5,
         .inequality = DB::ASOFJoinInequality::Less,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {1}},
        {.row_num = 0,
         .keep_versions = 5,
         .inequality = DB::ASOFJoinInequality::LessOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {1}},

        {.row_num = 0,
         .keep_versions = 4,
         .inequality = DB::ASOFJoinInequality::Greater,
         .expected_block_count = 2,
         .expected_block_idx = {},
         .expected_matching_row = {}},
        {.row_num = 0,
         .keep_versions = 4,
         .inequality = DB::ASOFJoinInequality::GreaterOrEquals,
         .expected_block_count = 2,
         .expected_block_idx = {},
         .expected_matching_row = {}},
        {.row_num = 0,
         .keep_versions = 4,
         .inequality = DB::ASOFJoinInequality::Less,
         .expected_block_count = 2,
         .expected_block_idx = {0}, /// Still the first block in the list since the initial first blog gets pruned.
         .expected_matching_row = {0}},
        {.row_num = 0,
         .keep_versions = 4,
         .inequality = DB::ASOFJoinInequality::LessOrEquals,
         .expected_block_count = 2,
         .expected_block_idx = {0},
         .expected_matching_row = {0}},

        {.row_num = 1,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::Greater,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {0}},
        {.row_num = 1,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::GreaterOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {1}},
        {.row_num = 1,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::Less,
         .expected_block_count = 3,
         .expected_block_idx = {1},
         .expected_matching_row = {0}},
        {.row_num = 1,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::LessOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {1}},

        {.row_num = 6,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::Greater,
         .expected_block_count = 3,
         .expected_block_idx = {},
         .expected_matching_row = {}},
        {.row_num = 6,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::GreaterOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {},
         .expected_matching_row = {}},
        {.row_num = 6,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::Less,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {0}},
        {.row_num = 6,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::LessOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {0},
         .expected_matching_row = {0}},
        {.row_num = 7,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::Greater,
         .expected_block_count = 3,
         .expected_block_idx = {2},
         .expected_matching_row = {1}},
        {.row_num = 7,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::GreaterOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {2},
         .expected_matching_row = {1}},
        {.row_num = 7,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::Less,
         .expected_block_count = 3,
         .expected_block_idx = {},
         .expected_matching_row = {}},
        {.row_num = 7,
         .keep_versions = 6,
         .inequality = DB::ASOFJoinInequality::LessOrEquals,
         .expected_block_count = 3,
         .expected_block_idx = {},
         .expected_matching_row = {}},
    };

    commonTest(/*data_block_size=*/0, cases);
    commonTest(/*data_block_size=*/2, cases);
}
