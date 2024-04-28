#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Streaming/Aggregator.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include "DataTypes/DataTypeInterval.h"
#include "Interpreters/Streaming/TableFunctionDescription.h"
namespace DB
{
Block prepareHeader()
{
    std::vector<ColumnWithTypeAndName> columns;
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int8"), "int8"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int16"), "int16"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int32"), "int32"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int64"), "int64"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int128"), "int128"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int256"), "int256"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int8)"), "low_int8"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int16)"), "low_int16"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int32)"), "low_int32"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int64)"), "low_int64"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int128)"), "low_int128"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(int256)"), "low_int256"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(string)"), "low_str"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("low_cardinality(fixed_string(3))"), "low_fixed_str"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("string"), "str"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("fixed_string(3)"), "fixed_str"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("nullable(int8)"), "nullable"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("datetime"), "window_start"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("datetime"), "window_end"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("datetime"), "_tp_time"));
    return Block(std::move(columns));
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

std::shared_ptr<Streaming::Aggregator::Params> prepareParams(size_t max_threads, [[maybe_unused]] std::vector<size_t> & key_site)
{
    Block src_header = prepareHeader();
    ColumnNumbers keys = key_site;
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

void setColumnsData(Columns & columns, [[maybe_unused]] std::vector<const DB::IColumn *> & aggr_columns)
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

void initAggregation()
{
    tryRegisterFormats();
    tryRegisterAggregateFunctions();
}

std::vector<std::vector<size_t>> prepareGlobalCountKeys()
{
    std::vector<std::vector<size_t>> key_sites{
        {0},
        {1},
        {2},
        {3},
        {4},
        {5},
        {6},
        {7},
        {8},
        {9},
        {10},
        {11},
        {12},
        {13},
        {14},
        {15},
        {16},
        {4, 16},
    };
    return key_sites;
}

std::vector<std::vector<size_t>> prepareWindowCountKeys()
{
    std::vector<std::vector<size_t>> key_sites{
        {17},
        {17, 18},
        {17, 18, 3},
        {17, 18, 4},
        {17, 18, 9},
        {17, 18, 10},
        {17, 18, 16},
        {17, 18, 4, 16},
    };
    return key_sites;
}

using ResultType = std::variant<size_t, std::string>;

std::vector<std::vector<std::pair<size_t, ResultType>>> prepareGlobalResult()
{
    std::vector<std::vector<std::pair<size_t, ResultType>>> result{
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, std::string("str")}, {1, size_t(10)}},
        {{0, std::string("str")}, {1, size_t(10)}},
        {{0, std::string("str")}, {1, size_t(10)}},
        {{0, std::string("str")}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(10)}},
        {{0, size_t(8)}, {1, size_t(8)}, {2, size_t(10)}},
    };
    return result;
};

std::vector<std::vector<std::pair<size_t, ResultType>>> prepareWindowResult()
{
    std::vector<std::vector<std::pair<size_t, ResultType>>> result{
        {{1, size_t(10)}},
        {{2, size_t(10)}},
        {{2, size_t(8)}, {3, size_t(10)}},
        {{2, size_t(8)}, {3, size_t(10)}},
        {{2, size_t(8)}, {3, size_t(10)}},
        {{2, size_t(8)}, {3, size_t(10)}},
        {{2, size_t(8)}, {3, size_t(10)}},
        {{2, size_t(8)}, {3, size_t(8)}, {4, size_t(10)}},
    };
    return result;
};

void checkGlobalAggregationResult(BlocksList & blocks, std::vector<std::vector<std::pair<size_t, ResultType>>> & results, size_t position)
{
    EXPECT_EQ(blocks.size(), 1);
    auto & result = results[position];
    for (auto & [site, value] : result)
    {
        if (std::holds_alternative<size_t>(value))
        {
            EXPECT_EQ(blocks.begin()->getByPosition(site).column->get64(0), std::get<size_t>(value));
        }
        else
        {
            EXPECT_EQ(blocks.begin()->getByPosition(site).column->getDataAt(0), std::get<std::string>(value));
        }
    }
}

void checkWindowAggregationResult(BlocksList & blocks, std::vector<std::vector<std::pair<size_t, ResultType>>> & results, size_t position)
{
    EXPECT_EQ(blocks.size(), 1);
    auto & result = results[position];

    for (auto & [site, value] : result)
    {
        if (std::holds_alternative<size_t>(value))
        {
            EXPECT_EQ(blocks.begin()->getByPosition(site).column->get64(0), std::get<size_t>(value));
        }
        else
        {
            EXPECT_EQ(blocks.begin()->getByPosition(site).column->getDataAt(0), std::get<std::string>(value));
        }
    }
}

std::shared_ptr<Streaming::Aggregator::Params> prepareGlobalAggregator(
    size_t max_threads,
    std::vector<size_t> & key_site,
    ColumnRawPtrs & key_columns,
    [[maybe_unused]] ColumnRawPtrs & aggr_columns,
    Columns & columns)
{
    setColumnsData(columns, aggr_columns);
    std::shared_ptr<Streaming::Aggregator::Params> params = prepareParams(max_threads, key_site);
    Streaming::Aggregator aggregator(*params);
    for (auto & site : key_site)
    {
        key_columns.push_back(columns[site].get());
    }
    return params;
}

std::shared_ptr<Streaming::Aggregator::Params> prepareWindowAggregator(
    size_t max_threads,
    std::vector<size_t> & key_site,
    ColumnRawPtrs & key_columns,
    [[maybe_unused]] ColumnRawPtrs & aggr_columns,
    Columns & columns)
{
    setColumnsData(columns, aggr_columns);
    std::shared_ptr<Streaming::Aggregator::Params> params = prepareParams(max_threads, key_site);
    params->group_by = {Streaming::Aggregator::Params::GroupBy::WINDOW_END};
    params->window_keys_num = 2;
    if (key_site.size() == 1)
        params->window_keys_num = 1;
    ParserFunction func_parser;
    auto ast = parseQuery(func_parser, "tumble(stream, 5s)", 0, 10000);
    NamesAndTypesList columns_list;
    if (key_site.size() == 1)
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
    for (auto & site : key_site)
    {
        key_columns.push_back(columns[site].get());
    }
    return params;
}

TEST(StreamingAggregation, globalcount)
{
    initAggregation();
    auto key_sites = prepareGlobalCountKeys();
    auto results = prepareGlobalResult();
    size_t position = 0;
    for (auto & key_site : key_sites)
    {
        Columns columns;
        ColumnRawPtrs key_columns, aggregate_columns;
        Streaming::AggregatedDataVariants hash_map;
        std::shared_ptr<Streaming::Aggregator::Params> params
            = prepareGlobalAggregator(10, key_site, key_columns, aggregate_columns, columns);
        Streaming::Aggregator aggregator(*params);
        Aggregator::AggregateColumns aggregate_column{aggregate_columns};
        aggregator.executeOnBlock(columns, 0, 10, hash_map, key_columns, aggregate_column);
        auto blocks = aggregator.convertToBlocks(hash_map, true, 10);
        checkGlobalAggregationResult(blocks, results, position++);
    }
}

TEST(StreamingAggregation, windowcount)
{
    initAggregation();
    auto key_sites = prepareWindowCountKeys();
    auto results = prepareWindowResult();
    size_t position = 0;
    for (auto & key_site : key_sites)
    {
        Columns columns;
        ColumnRawPtrs key_columns, aggregate_columns;
        Streaming::AggregatedDataVariants hash_map;
        std::shared_ptr<Streaming::Aggregator::Params> params
            = prepareWindowAggregator(10, key_site, key_columns, aggregate_columns, columns);
        Streaming::Aggregator aggregator(*params);
        Aggregator::AggregateColumns aggregate_column{aggregate_columns};
        aggregator.executeOnBlock(columns, 0, 10, hash_map, key_columns, aggregate_column);
        auto blocks = aggregator.convertToBlocks(hash_map, true, 10);
        checkWindowAggregationResult(blocks, results, position++);
    }
}

};