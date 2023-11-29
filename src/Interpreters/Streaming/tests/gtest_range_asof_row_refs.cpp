#include <Columns/ColumnDecimal.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/Streaming/RangeAsofJoinContext.h>
#include <Interpreters/Streaming/RowRefs.h>

#include <gtest/gtest.h>

namespace
{
/// TODO, other types
DB::Block prepareRightBlock()
{
    auto type = std::make_shared<DB::DataTypeDateTime64>(3);
    auto mutable_col = type->createColumn();
    auto * col = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(mutable_col.get());

    col->insertValue(1'612'286'044'256);
    col->insertValue(1'612'286'045'256);
    col->insertValue(1'612'286'054'256);
    col->insertValue(1'612'286'059'256);
    col->insertValue(1'612'286'060'256);
    col->insertValue(1'612'286'069'256);

    DB::ColumnWithTypeAndName column_with_type{std::move(mutable_col), type, "_tp_time"};

    return DB::Block{DB::ColumnsWithTypeAndName{{column_with_type}}};
}

DB::Block prepareLeftBlock()
{
    auto block{prepareRightBlock()};

    auto * col = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(block.getByPosition(0).column->assumeMutable().get());
    col->insertValue(1'612'286'033'256);
    col->insertValue(1'612'286'080'256);

    return block;
}

struct Case
{
    size_t row_num;
    std::vector<uint32_t> expected_matching_rows;
};

void commonTest(const std::vector<Case> & cases, const DB::Streaming::RangeAsofJoinContext & range_ctx)
{
    DB::LightChunkWithTimestamp right_block{prepareRightBlock()};
    auto left_block{prepareLeftBlock()};

    auto & asof_col = left_block.getByPosition(0);

    DB::Streaming::RangeAsofRowRefs<DB::LightChunkWithTimestamp> row_refs(asof_col.type->getTypeId());

    for (size_t i = 0; i < right_block.rows(); ++i)
        row_refs.insert(asof_col.type->getTypeId(), *asof_col.column, &right_block, i, i);

    for (const auto & test_case : cases)
    {
        auto results{row_refs.findRange(asof_col.type->getTypeId(), range_ctx, *asof_col.column, test_case.row_num, true)};

        /// expected matching rows: 0, 1, 2
        ASSERT_EQ(results.size(), test_case.expected_matching_rows.size());
        for (size_t i = 0; const auto & row_ref : results)
            ASSERT_EQ(test_case.expected_matching_rows[i++], row_ref.row_num);
    }
}
}

/// -10 <= x - y <= 10
TEST(RowRefs, FindRangeGreaterOrEqualsAndLessOrEquals)
{
    DB::Streaming::RangeAsofJoinContext range_ctx;
    range_ctx.left_inequality = DB::ASOFJoinInequality::GreaterOrEquals;
    range_ctx.right_inequality = DB::ASOFJoinInequality::LessOrEquals;

    range_ctx.lower_bound = -10 * 1000;
    range_ctx.upper_bound = 10 * 1000;
    range_ctx.type = DB::Streaming::RangeType::Interval;

    std::vector<Case> cases
        = {{.row_num = 0, .expected_matching_rows = {0, 1, 2}},
           {.row_num = 1, .expected_matching_rows = {0, 1, 2}},
           {.row_num = 2, .expected_matching_rows = {0, 1, 2, 3, 4}},
           {.row_num = 3, .expected_matching_rows = {2, 3, 4, 5}},
           {.row_num = 4, .expected_matching_rows = {2, 3, 4, 5}},
           {.row_num = 5, .expected_matching_rows = {3, 4, 5}},
           {.row_num = 6, .expected_matching_rows = {}},
           {.row_num = 7, .expected_matching_rows = {}}};

    commonTest(cases, range_ctx);
}

/// -10 < x - y < 10
TEST(RowRefs, FindRangeGreateAndLess)
{
    DB::Streaming::RangeAsofJoinContext range_ctx;
    range_ctx.left_inequality = DB::ASOFJoinInequality::Greater;
    range_ctx.right_inequality = DB::ASOFJoinInequality::Less;

    range_ctx.lower_bound = -10 * 1000;
    range_ctx.upper_bound = 10 * 1000;
    range_ctx.type = DB::Streaming::RangeType::Interval;

    std::vector<Case> cases
        = {{.row_num = 0, .expected_matching_rows = {0, 1}},
           {.row_num = 1, .expected_matching_rows = {0, 1, 2}},
           {.row_num = 2, .expected_matching_rows = {1, 2, 3, 4}},
           {.row_num = 3, .expected_matching_rows = {2, 3, 4}},
           {.row_num = 4, .expected_matching_rows = {2, 3, 4, 5}},
           {.row_num = 5, .expected_matching_rows = {4, 5}},
           {.row_num = 6, .expected_matching_rows = {}},
           {.row_num = 7, .expected_matching_rows = {}}};

    commonTest(cases, range_ctx);
}

/// -10 <= x - y < 10
TEST(RowRefs, FindRangeGreaterOrEqualsAndLess)
{
    DB::Streaming::RangeAsofJoinContext range_ctx;
    range_ctx.left_inequality = DB::ASOFJoinInequality::GreaterOrEquals;
    range_ctx.right_inequality = DB::ASOFJoinInequality::Less;

    range_ctx.lower_bound = -10 * 1000;
    range_ctx.upper_bound = 10 * 1000;
    range_ctx.type = DB::Streaming::RangeType::Interval;

    std::vector<Case> cases
        = {{.row_num = 0, .expected_matching_rows = {0, 1, 2}},
           {.row_num = 1, .expected_matching_rows = {0, 1, 2}},
           {.row_num = 2, .expected_matching_rows = {1, 2, 3, 4}},
           {.row_num = 3, .expected_matching_rows = {2, 3, 4, 5}},
           {.row_num = 4, .expected_matching_rows = {2, 3, 4, 5}},
           {.row_num = 5, .expected_matching_rows = {4, 5}},
           {.row_num = 6, .expected_matching_rows = {}},
           {.row_num = 7, .expected_matching_rows = {}}};

    commonTest(cases, range_ctx);
}

/// -10 < x - y <= 10
TEST(RowRefs, FindRangeGreaterAndLessOrEquals)
{
    DB::Streaming::RangeAsofJoinContext range_ctx;
    range_ctx.left_inequality = DB::ASOFJoinInequality::Greater;
    range_ctx.right_inequality = DB::ASOFJoinInequality::LessOrEquals;

    range_ctx.lower_bound = -10 * 1000;
    range_ctx.upper_bound = 10 * 1000;
    range_ctx.type = DB::Streaming::RangeType::Interval;

    std::vector<Case> cases
        = {{.row_num = 0, .expected_matching_rows = {0, 1}},
           {.row_num = 1, .expected_matching_rows = {0, 1, 2}},
           {.row_num = 2, .expected_matching_rows = {0, 1, 2, 3, 4}},
           {.row_num = 3, .expected_matching_rows = {2, 3, 4}},
           {.row_num = 4, .expected_matching_rows = {2, 3, 4, 5}},
           {.row_num = 5, .expected_matching_rows = {3, 4, 5}},
           {.row_num = 6, .expected_matching_rows = {}},
           {.row_num = 7, .expected_matching_rows = {}}};

    commonTest(cases, range_ctx);
}
