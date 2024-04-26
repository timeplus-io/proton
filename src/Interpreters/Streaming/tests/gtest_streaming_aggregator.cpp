#include <cstddef>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/formatBlock.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinedTables.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Interpreters/Streaming/HashJoin.h>
#include <Interpreters/Streaming/SyntaxAnalyzeUtils.h>
#include <Interpreters/Streaming/TrackingUpdatesData.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Transforms/Streaming/JoinTransform.h>
#include <base/constexpr_helpers.h>
#include <gtest/gtest.h>
#include <Common/logger_useful.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include "Core/ColumnNumbers.h"
#include "Core/NamesAndTypes.h"
#include "Core/iostream_debug_helpers.h"
#include "DataTypes/DataTypeDateTime.h"
#include "DataTypes/DataTypeDateTime64.h"
#include "DataTypes/DataTypeFixedString.h"
#include "DataTypes/DataTypeInterval.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"
#include "Interpreters/Streaming/TableFunctionDescription.h"
#include "Interpreters/Streaming/TableFunctionDescription_fwd.h"

namespace DB
{
using namespace DB;

void prepareHeader(Block & header)
{
    std::vector<ColumnWithTypeAndName> columns;
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int8"), fmt::format("int8", 0)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int16"), fmt::format("num16", 1)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int32"), fmt::format("num32", 2)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int64"), fmt::format("num64", 3)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int128"), fmt::format("num128", 4)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int256"), fmt::format("num256", 5)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int8)"), fmt::format("low_int8", 6)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int16)"), fmt::format("low_int16", 7)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int32)"), fmt::format("low_int32", 8)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int64)"), fmt::format("low_int64", 9)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int128)"), fmt::format("low_int128", 10)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int256)"), fmt::format("low_int256", 11)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(string)"), fmt::format("low_str", 12)));
    columns.push_back(
        ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(fixed_string(3))"), fmt::format("low_fixed_str", 13)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("string"), fmt::format("str", 14)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("fixed_string(3)"), fmt::format("fixed_str", 15)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("nullable(int8)"), fmt::format("nullable", 16)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("datetime"), fmt::format("window_start", 17)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("datetime"), fmt::format("window_end", 18)));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("datetime"), fmt::format("_tp_time", 19)));
    for (auto & column : columns)
    {
        header.insert(column);
    }
}

void prepareAggregates(AggregateDescriptions & aggregates)
{
    tryRegisterAggregateFunctions();
    DataTypes insert_data_types = {std::make_shared<DataTypeInt32>()};
    Array param;
    DataTypes result_data_types = {std::make_shared<DataTypeUInt64>()};
    ColumnNumbers arguments = {};
    AggregateFunctionProperties properties = {false, false};
    AggregateDescription aggregate{
        AggregateFunctionFactory::instance().get("count", result_data_types, param, properties, false), param, arguments, {}, "count()"};
    aggregates.push_back(aggregate);
}

std::shared_ptr<Streaming::Aggregator::Params> prepareParams(size_t max_threads, [[maybe_unused]] std::vector<size_t> & key_sites)
{
    Block src_header;
    prepareHeader(src_header);
    ColumnNumbers keys = key_sites;
    AggregateDescriptions aggregates;
    prepareAggregates(aggregates);
    bool overflow_row{false};
    size_t max_rows_to_group_by{0};
    OverflowMode group_by_overflow_mode{OverflowMode::THROW};
    size_t group_by_two_level_threshold{0};
    size_t group_by_two_level_threshold_bytes{0};
    size_t max_bytes_before_external_group_by{0};
    bool empty_result_for_aggregation_by_empty_set{false};
    VolumePtr tmp_volume = nullptr;
    size_t min_free_disk_space{0};
    bool compile_aggregate_expressions{true};
    size_t min_count_to_compile_aggregate_expression{3};
    size_t max_block_size{65409};
    Block intermediate_header;
    bool keep_state{true};
    size_t streaming_window_count{0};
    Streaming::Aggregator::Params::GroupBy streaming_group_by{Streaming::Aggregator::Params::GroupBy::OTHER};
    ssize_t delta_col_pos{-1};
    size_t window_keys_num{0};
    Streaming::WindowParamsPtr window_params{nullptr};
    Streaming::TrackingUpdatesType tracking_updates_type{Streaming::TrackingUpdatesType::None};
    return make_shared<Streaming::Aggregator::Params>(
        src_header,
        keys,
        aggregates,
        overflow_row,
        max_rows_to_group_by,
        group_by_overflow_mode,
        group_by_two_level_threshold,
        group_by_two_level_threshold_bytes,
        max_bytes_before_external_group_by,
        empty_result_for_aggregation_by_empty_set,
        tmp_volume,
        max_threads,
        min_free_disk_space,
        compile_aggregate_expressions,
        min_count_to_compile_aggregate_expression,
        max_block_size,
        intermediate_header,
        keep_state,
        streaming_window_count,
        streaming_group_by,
        delta_col_pos,
        window_keys_num,
        window_params,
        tracking_updates_type);
}

template <typename DataType, typename... Args>
auto createLowColumn(Args &&... args)
{
    auto int_data_type = std::make_shared<DataType>(std::forward(args)...);
    auto low_cardinality_data_type = std::make_shared<DataTypeLowCardinality>(int_data_type);
    return low_cardinality_data_type->createColumn();
};

void setColumnsData(Columns & columns, size_t & num_rows, [[maybe_unused]] std::vector<const DB::IColumn *> & aggr_columns)
{
    auto c_int_8 = ColumnInt8::create();
    auto c_int_16 = ColumnInt16::create();
    auto c_int_32 = ColumnInt32::create();
    auto c_int_64 = ColumnInt64::create();
    auto c_int_128 = ColumnInt128::create();
    auto c_int_256 = ColumnInt256::create();
    auto c_low_int8 = createLowColumn<DataTypeInt8>();
    auto c_low_int16 = createLowColumn<DataTypeInt16>();
    auto c_low_int32 = createLowColumn<DataTypeInt32>();
    auto c_low_int64 = createLowColumn<DataTypeInt64>();
    auto c_low_int128 = createLowColumn<DataTypeInt128>();
    auto c_low_int256 = createLowColumn<DataTypeInt256>();
    auto c_low_str = createLowColumn<DataTypeString>();
    auto low_fixed_date_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeFixedString>(3));
    auto c_low_fixed_str = low_fixed_date_type->createColumn();
    auto c_str = ColumnString::create();
    auto c_fixed_str = ColumnFixedString::create(3);
    auto c_nullable = ColumnNullable::create(ColumnInt8::create(), ColumnUInt8::create());
    auto time_date_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeDateTime>());
    auto s_time_date_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeDateTime>());
    auto e_time_date_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeDateTime>());
    auto c_datetime64 = time_date_type->createColumn();
    auto c_time_start = s_time_date_type->createColumn();
    auto c_time_end = e_time_date_type->createColumn();
    for (size_t i = 0; i < 10; ++i)
    {
        c_int_8->insertValue(8);
        c_int_16->insertValue(8);
        c_int_32->insertValue(8);
        c_int_64->insertValue(8);
        c_int_128->insertValue(8);
        c_int_256->insertValue(8);
        c_low_int8->insert(8);
        c_low_int16->insert(8);
        c_low_int32->insert(8);
        c_low_int64->insert(8);
        c_low_int128->insert(8);
        c_low_int256->insert(8);
        c_low_str->insertData("str", 3);
        c_low_fixed_str->insertData("str", 3);
        c_str->insertData("str", 3);
        c_fixed_str->insertData("str", 3);
        c_nullable->insert(8);
        c_datetime64->insert(3);
        c_time_start->insert(0);
        c_time_end->insert(5);
    }
    num_rows = 10;
    columns.push_back(std::move(c_int_8));
    columns.push_back(std::move(c_int_16));
    columns.push_back(std::move(c_int_32));
    columns.push_back(std::move(c_int_64));
    columns.push_back(std::move(c_int_128));
    columns.push_back(std::move(c_int_256));
    columns.push_back(std::move(c_low_int8));
    columns.push_back(std::move(c_low_int16));
    columns.push_back(std::move(c_low_int32));
    columns.push_back(std::move(c_low_int64));
    columns.push_back(std::move(c_low_int128));
    columns.push_back(std::move(c_low_int256));
    columns.push_back(std::move(c_low_str));
    columns.push_back(std::move(c_low_fixed_str));
    columns.push_back(std::move(c_str));
    columns.push_back(std::move(c_fixed_str));
    columns.push_back(std::move(c_nullable));
    columns.push_back(std::move(c_time_start));
    columns.push_back(std::move(c_time_end));
    columns.push_back(std::move(c_datetime64));
}

TEST(StreamingAggregation, globalcount)
{
    auto context = getContext().context;
    tryRegisterFormats();
    tryRegisterAggregateFunctions();
    size_t max_threads = 10;

    size_t num_rows{0};
    Columns columns;
    std::vector<const DB::IColumn *> aggr_columns;
    setColumnsData(columns, num_rows, aggr_columns);

    for (size_t i = 0; i < 18; ++i)
    {
        std::vector<size_t> key_sites;
        if (i == 17)
        {
            key_sites.push_back(4);
            key_sites.push_back(16);
        }
        else
        {
            key_sites.push_back(i);
        }
        // global aggr
        std::shared_ptr<Streaming::Aggregator::Params> params;
        params = prepareParams(max_threads, key_sites);
        Streaming::Aggregator aggregator(*params);

        ColumnRawPtrs key_columns;

        for (auto & key_site : key_sites)
        {
            key_columns.push_back(columns[key_site].get());
        }

        Aggregator::AggregateColumns aggregate_columns{aggr_columns};

        size_t row_begin = 0;
        size_t row_end = num_rows;
        Streaming::AggregatedDataVariants hash_map;
        aggregator.executeOnBlock(columns, row_begin, row_end, hash_map, key_columns, aggregate_columns);

        auto blocks = aggregator.convertToBlocks(hash_map, true, max_threads);
        // auto
        if (i >= 12 && i <= 15)
            EXPECT_EQ(blocks.begin()->getByPosition(0).column->getDataAt(0), "str");
        else
            EXPECT_EQ(blocks.begin()->getByPosition(0).column->get64(0), 8);
        if (i == 17)
        {
            EXPECT_EQ(blocks.begin()->getByPosition(1).column->get64(0), 8);
            EXPECT_EQ(blocks.begin()->getByPosition(2).column->get64(0), 10);
        }
        else
            EXPECT_EQ(blocks.begin()->getByPosition(1).column->get64(0), 10);
    }
}

TEST(StreamingAggregation, windowcount)
{
    auto context = getContext().context;
    tryRegisterFormats();
    tryRegisterAggregateFunctions();
    size_t max_threads = 10;

    size_t num_rows{0};
    Columns columns;
    std::vector<const DB::IColumn *> aggr_columns;
    setColumnsData(columns, num_rows, aggr_columns);

    for (size_t i = 0; i < 18; ++i)
    {
        std::vector<size_t> key_sites;
        key_sites.push_back(17);
        if (i != 0)
            key_sites.push_back(18);

        if (i == 17)
        {
            key_sites.push_back(4);
            key_sites.push_back(16);
        }
        else if (i == 3 || i == 4 || i == 9 || i == 10 || i == 16)
        {
            key_sites.push_back(i);
        }
        else if (i != 0 && i != 1)
            continue;
        // window aggr
        std::shared_ptr<Streaming::Aggregator::Params> params;
        params = prepareParams(max_threads, key_sites);
        params->group_by = {Streaming::Aggregator::Params::GroupBy::WINDOW_END};
        params->window_keys_num = 2;
        if (i == 0)
            params->window_keys_num = 1;
        ParserFunction func_parser;
        auto ast = parseQuery(func_parser, "tumble(stream, 5s)", 0, 10000);
        NamesAndTypesList columns_list;
        if (i == 0)
            columns_list = {{"window_start", std::make_shared<DataTypeDateTime64>(3, "UTC")}};
        columns_list.push_back({"window_end", std::make_shared<DataTypeDateTime64>(3, "UTC")});
        Streaming::TableFunctionDescriptionPtr table_function_description = std::make_shared<Streaming::TableFunctionDescription>(
            ast,
            Streaming::WindowType::Tumble,
            Names{"_tp_time", "5s"},
            DataTypes{std::make_shared<DataTypeDateTime64>(3, "UTC"), std::make_shared<DataTypeInterval>(IntervalKind::Second)},
            std::shared_ptr<ExpressionActions>(),
            Names{"_tp_time"},
            columns_list);
        Streaming::WindowParamsPtr window_params = Streaming::WindowParams::create(table_function_description);

        params->window_params = window_params;
        Streaming::Aggregator aggregator(*params);

        ColumnRawPtrs key_columns;

        for (auto & key_site : key_sites)
        {
            key_columns.push_back(columns[key_site].get());
        }

        Aggregator::AggregateColumns aggregate_columns{aggr_columns};

        size_t row_begin = 0;
        size_t row_end = num_rows;
        Streaming::AggregatedDataVariants hash_map;
        aggregator.executeOnBlock(columns, row_begin, row_end, hash_map, key_columns, aggregate_columns);

        auto blocks = aggregator.convertToBlocks(hash_map, true, max_threads);


        if (i == 0)
        {
            EXPECT_EQ(blocks.begin()->getByPosition(1).column->get64(0), 10);
            continue;
        }
        if (i == 1)
        {
            EXPECT_EQ(blocks.begin()->getByPosition(2).column->get64(0), 10);
            continue;
        }
        EXPECT_EQ(blocks.begin()->getByPosition(2).column->get64(0), 8);
        if (i == 3 || i == 4 || i == 9 || i == 10 || i == 16)
        {
            EXPECT_EQ(blocks.begin()->getByPosition(3).column->get64(0), 10);
        }
        if (i == 17)
        {
            EXPECT_EQ(blocks.begin()->getByPosition(3).column->get64(0), 8);
            EXPECT_EQ(blocks.begin()->getByPosition(4).column->get64(0), 10);
        }
    }
}

};