#include <memory>
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

class InputData
{
public:
    size_t int_data;
    std::string str_data;
    size_t time_data;
    int delta_data;
};

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
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("datetime64(3)"), "window_start"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("datetime64(3)"), "window_end"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("datetime64(3)"), "_tp_time"));
    columns.push_back(ColumnWithTypeAndName(DataTypeFactory::instance().get("int8"), "_tp_delta"));
    return Block(std::move(columns));
}

void prepareAggregates(AggregateDescriptions & aggregates, bool is_changelog_input)
{
    tryRegisterAggregateFunctions();
    DataTypes insert_data_types = {std::make_shared<DataTypeInt32>()};
    Array param;
    DataTypes result_data_types = {std::make_shared<DataTypeInt8>()};
    ColumnNumbers arguments{};
    AggregateFunctionProperties properties = {false, false};
    std::string name;
    if (is_changelog_input)
    {
        name = "_tp_delta";
        arguments.push_back(20);
    }
    std::string function_name = is_changelog_input ? "__count_retract" : "count";
    AggregateDescription aggregate{
        AggregateFunctionFactory::instance().get(function_name, result_data_types, param, properties, is_changelog_input),
        param,
        arguments,
        {name},
        "count()"};
    aggregates.push_back(aggregate);
}

Streaming::Aggregator::Params prepareParams(size_t max_threads, bool is_changelog_input, [[maybe_unused]] std::vector<size_t> & key_site)
{
    Block src_header = prepareHeader();
    ColumnNumbers & keys = key_site;
    AggregateDescriptions aggregates;
    prepareAggregates(aggregates, is_changelog_input);
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
    int delta_col_pos{is_changelog_input ? 20 : -1};
    size_t window_keys_num{0};
    Streaming::WindowParamsPtr window_params{nullptr};
    Streaming::TrackingUpdatesType tracking_updates_type{Streaming::TrackingUpdatesType::None};
    return Streaming::Aggregator::Params(
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

void setColumnsData(Columns & columns, InputData input_data, [[maybe_unused]] std::vector<const DB::IColumn *> & aggr_columns)
{
    columns.clear();
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
    auto time_date_type = std::make_shared<DataTypeDateTime64>(3);
    auto s_time_date_type = std::make_shared<DataTypeDateTime64>(3);
    auto e_time_date_type = std::make_shared<DataTypeDateTime64>(3);
    auto c_datetime64 = time_date_type->createColumn();
    auto c_time_start = s_time_date_type->createColumn();
    auto c_time_end = e_time_date_type->createColumn();
    auto * c_datetime64_inner = typeid_cast<ColumnDecimal<DateTime64> *>(c_datetime64.get());
    auto * c_time_start_inner = typeid_cast<ColumnDecimal<DateTime64> *>(c_time_start.get());
    auto * c_time_end_inner = typeid_cast<ColumnDecimal<DateTime64> *>(c_time_end.get());
    auto c_delta = ColumnInt8::create();
    for (size_t i = 0; i < 10; ++i)
    {
        c_int_8->insertValue(input_data.int_data);
        c_int_16->insertValue(input_data.int_data);
        c_int_32->insertValue(input_data.int_data);
        c_int_64->insertValue(input_data.int_data);
        c_int_128->insertValue(Int128(input_data.int_data));
        c_int_256->insertValue(Int256(input_data.int_data));
        c_low_int8->insert(input_data.int_data);
        c_low_int16->insert(input_data.int_data);
        c_low_int32->insert(input_data.int_data);
        c_low_int64->insert(input_data.int_data);
        c_low_int128->insert(Int128(input_data.int_data));
        c_low_int256->insert(Int256(input_data.int_data));
        c_low_str->insertData(input_data.str_data.c_str(), 3);
        c_low_fixed_str->insertData(input_data.str_data.c_str(), 3);
        c_str->insertData(input_data.str_data.c_str(), 3);
        c_fixed_str->insertData(input_data.str_data.c_str(), 3);
        c_nullable->insert(input_data.int_data);
        c_datetime64_inner->insertValue(input_data.time_data);
        c_time_start_inner->insertValue(input_data.time_data / 5 * 5);
        c_time_end_inner->insertValue((input_data.time_data / 5 + 1) * 5);
        c_delta->insert(input_data.delta_data);
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
    columns.push_back(std::move(c_delta));
}

Streaming::Aggregator::Params prepareGlobalAggregator(
    size_t max_threads,
    std::vector<size_t> & key_site,
    ColumnRawPtrs & key_columns,
    InputData & input_data,
    [[maybe_unused]] ColumnRawPtrs & aggr_columns,
    Columns & columns,
    bool is_changelog_input = false)
{
    setColumnsData(columns, input_data, aggr_columns);
    auto params = prepareParams(max_threads, is_changelog_input, key_site);
    for (size_t i = 0; i < key_site.size(); ++i)
    {
        key_columns.push_back(nullptr);
    }
    return params;
}

Streaming::Aggregator::Params prepareWindowAggregator(
    size_t max_threads,
    std::vector<size_t> & key_site,
    ColumnRawPtrs & key_columns,
    InputData & input_data,
    [[maybe_unused]] ColumnRawPtrs & aggr_columns,
    Columns & columns,
    bool is_changelog_input = false)
{
    setColumnsData(columns, input_data, aggr_columns);
    auto params = prepareParams(max_threads, is_changelog_input, key_site);
    params.group_by = {Streaming::Aggregator::Params::GroupBy::WINDOW_END};
    params.window_keys_num = 2;
    if (key_site.size() == 1)
        params.window_keys_num = 1;
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
    params.window_params = window_params;
    for (size_t i = 0; i < key_site.size(); ++i)
    {
        key_columns.push_back(nullptr);
    }
    return params;
}

using ResultType = std::variant<Int64, Int8, std::string>;

void checkAggregationResults(
    BlocksList & blocks, std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> & results, size_t position)
{
    auto & result = results[position];
    std::map<ResultType, size_t> result_map, expected_map;
    for (auto & [column, raw, value] : result)
    {
        if (std::holds_alternative<Int8>(value))
        {
            expected_map[value]++;
            result_map[static_cast<Int8>(blocks.begin()->getByPosition(column).column->get64(raw))]++;
        }
        else if (std::holds_alternative<Int64>(value))
        {
            expected_map[value]++;
            result_map[blocks.begin()->getByPosition(column).column->getInt(raw)]++;
        }
        else
        {
            expected_map[value]++;
            result_map[blocks.begin()->getByPosition(column).column->getDataAt(raw).toString()]++;
        }
    }
    EXPECT_EQ(result_map, expected_map);
}

auto convertToResult(
    Streaming::Aggregator & aggregator,
    Streaming::AggregatedDataVariants & hash_map,
    InputData & input_data,
    bool is_changelog_output,
    bool is_update_output,
    bool is_window_aggregator)
{
    if (is_update_output && is_window_aggregator)
        return std::list{aggregator.spliceAndConvertUpdatesToBlock(hash_map, {Int64(input_data.time_data / 5 * 5)})};
    else if (is_window_aggregator)
        return std::list{aggregator.spliceAndConvertToBlock(hash_map, true, {Int64(input_data.time_data / 5 * 5)})};
    else if (is_changelog_output)
        return aggregator.convertRetractToBlocks(hash_map);
    else if (is_update_output)
        return aggregator.convertUpdatesToBlocks(hash_map);
    else
        return aggregator.convertToBlocks(hash_map, true, 10);
}

void initAggregation()
{
    tryRegisterFormats();
    tryRegisterAggregateFunctions();
}

template <typename KeyFunc, typename ResultFunc, typename AggregatorFunc>
void executeAggregatorTest(
    KeyFunc key_func,
    ResultFunc result_func,
    AggregatorFunc aggregator_func,
    InputData first_input_data,
    InputData second_input_data,
    bool is_changelog_input,
    bool is_changelog_output,
    bool is_update_output,
    bool is_window_aggregator)
{
    initAggregation();
    auto key_sites = key_func();
    auto results = result_func();
    size_t position = 0;
    for (auto & key_site : key_sites)
    {
        Columns columns_first, columns_second;
        ColumnRawPtrs key_columns, aggregate_columns;
        Streaming::AggregatedDataVariants hash_map;
        Streaming::Aggregator::Params params
            = aggregator_func(10, key_site, key_columns, first_input_data, aggregate_columns, columns_first, is_changelog_input);
        if (is_changelog_output)
            params.tracking_updates_type = Streaming::TrackingUpdatesType::UpdatesWithRetract;
        if (is_update_output)
            params.tracking_updates_type = Streaming::TrackingUpdatesType::Updates;
        auto aggregator = Streaming::Aggregator(params);
        Aggregator::AggregateColumns aggregate_column{aggregate_columns};
        aggregator.executeOnBlock(columns_first, 0, 10, hash_map, key_columns, aggregate_column);
        auto block_first
            = convertToResult(aggregator, hash_map, first_input_data, is_changelog_output, is_update_output, is_window_aggregator);
        setColumnsData(columns_second, second_input_data, aggregate_columns);
        if (is_changelog_output)
            aggregator.executeAndRetractOnBlock(columns_second, 0, 10, hash_map, key_columns, aggregate_column);
        else
            aggregator.executeOnBlock(columns_second, 0, 10, hash_map, key_columns, aggregate_column);
        auto block_second
            = convertToResult(aggregator, hash_map, second_input_data, is_changelog_output, is_update_output, is_window_aggregator);
        checkAggregationResults(block_second, results, position++);
    }
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

std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> prepareGlobalAppendonlyAppendonlyResult()
{
    std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> result{
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, std::string("str")}, {0, 1, std::string("sts")}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, std::string("str")}, {0, 1, std::string("sts")}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, std::string("str")}, {0, 1, std::string("sts")}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, std::string("str")}, {0, 1, std::string("sts")}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int8(8)}, {0, 1, Int8(9)}, {1, 0, Int64(10)}, {1, 1, Int64(10)}},
        {{0, 0, Int64(8)}, {0, 1, Int64(9)}, {1, 0, Int8(8)}, {1, 1, Int8(9)}, {2, 0, Int64(10)}, {2, 1, Int64(10)}},
    };
    return result;
};

std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> prepareGlobalChangelogAppendonlyResult()
{
    std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> result{
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(0)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(0)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(0)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(0)}},
        {{0, 0, Int8(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int8(8)}, {2, 0, Int64(0)}},
    };
    return result;
};

std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> prepareGlobalAppendonlyUpdateResult()
{
    std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> result{
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int64(10)}},
        {{0, 0, std::string("sts")}, {1, 0, Int64(10)}},
        {{0, 0, std::string("sts")}, {1, 0, Int64(10)}},
        {{0, 0, std::string("sts")}, {1, 0, Int64(10)}},
        {{0, 0, std::string("sts")}, {1, 0, Int64(10)}},
        {{0, 0, Int8(9)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(9)}, {1, 0, Int8(9)}, {2, 0, Int64(10)}},
    };
    return result;
};

std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> prepareGlobalChangelogUpdateResult()
{
    std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> result{
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(0)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(0)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(0)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(0)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(0)}},
        {{0, 0, Int8(8)}, {1, 0, Int64(0)}},
        {{0, 0, Int64(8)}, {1, 0, Int8(8)}, {2, 0, Int64(0)}},
    };
    return result;
};

std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> prepareGlobalAppendonlyChangelogResult()
{
    std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> result{
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(10)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(10)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(10)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(10)}},
        {{0, 0, Int8(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int8(8)}, {2, 0, Int64(10)}},
    };
    return result;
};

std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> prepareGlobalChangelogChangelogResult()
{
    std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> result{
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int64(10)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(10)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(10)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(10)}},
        {{0, 0, std::string("str")}, {1, 0, Int64(10)}},
        {{0, 0, Int8(8)}, {1, 0, Int64(10)}},
        {{0, 0, Int64(8)}, {1, 0, Int8(8)}, {2, 0, Int64(10)}},
    };
    return result;
};

std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> prepareWindowAppendonlyAppendonlyResult()
{
    std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> result{
        {{1, 0, Int64(20)}},
        {{2, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int8(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int8(8)}, {4, 0, Int64(20)}},
    };
    return result;
};

std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> prepareWindowAppendonlyUpdateResult()
{
    std::vector<std::vector<std::tuple<size_t, size_t, ResultType>>> result{
        {{1, 0, Int64(20)}},
        {{2, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int8(8)}, {3, 0, Int64(20)}},
        {{2, 0, Int64(8)}, {3, 0, Int8(8)}, {4, 0, Int64(20)}},
    };
    return result;
};

TEST(StreamingAggregation, GlobalAppendOnlyAppendOnly)
{
    executeAggregatorTest(
        prepareGlobalCountKeys,
        prepareGlobalAppendonlyAppendonlyResult,
        prepareGlobalAggregator,
        {8, "str", 1, 1},
        {9, "sts", 3, 1},
        false,
        false,
        false,
        false);
}

TEST(StreamingAggregation, GlobalChangelogAppendOnly)
{
    executeAggregatorTest(
        prepareGlobalCountKeys,
        prepareGlobalChangelogAppendonlyResult,
        prepareGlobalAggregator,
        {8, "str", 1, 1},
        {8, "str", 1, -1},
        true,
        false,
        false,
        false);
}

TEST(StreamingAggregation, GlobalAppendOnlyUpdate)
{
    executeAggregatorTest(
        prepareGlobalCountKeys,
        prepareGlobalAppendonlyUpdateResult,
        prepareGlobalAggregator,
        {8, "str", 1, 1},
        {9, "sts", 3, 1},
        false,
        false,
        true,
        false);
}

TEST(StreamingAggregation, GlobalChangelogUpdate)
{
    executeAggregatorTest(
        prepareGlobalCountKeys,
        prepareGlobalChangelogUpdateResult,
        prepareGlobalAggregator,
        {8, "str", 1, 1},
        {8, "str", 1, -1},
        true,
        false,
        true,
        false);
}

TEST(StreamingAggregation, GlobalAppendOnlyChangelog)
{
    executeAggregatorTest(
        prepareGlobalCountKeys,
        prepareGlobalAppendonlyChangelogResult,
        prepareGlobalAggregator,
        {8, "str", 1, 1},
        {8, "str", 3, 1},
        false,
        true,
        false,
        false);
}

TEST(StreamingAggregation, GlobalChangelogChangelog)
{
    executeAggregatorTest(
        prepareGlobalCountKeys,
        prepareGlobalChangelogChangelogResult,
        prepareGlobalAggregator,
        {8, "str", 1, 1},
        {8, "str", 3, 1},
        true,
        true,
        false,
        false);
}

TEST(StreamingAggregation, WindowAppendOnlyAppendOnly)
{
    executeAggregatorTest(
        prepareWindowCountKeys,
        prepareWindowAppendonlyAppendonlyResult,
        prepareWindowAggregator,
        {8, "str", 1, 1},
        {8, "str", 3, 1},
        false,
        false,
        false,
        true);
}

TEST(StreamingAggregation, WindowAppendOnlyUpdate)
{
    executeAggregatorTest(
        prepareWindowCountKeys,
        prepareWindowAppendonlyUpdateResult,
        prepareWindowAggregator,
        {8, "str", 1, 1},
        {8, "str", 3, 1},
        false,
        false,
        true,
        true);
}
};