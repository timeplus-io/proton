#include <AggregateFunctions/Streaming/AggregateFunctionMinMaxK.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/typeIndexToTypeName.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Common/FieldVisitorConvertToNumber.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace Streaming
{
namespace
{
template <bool is_min>
AggregateFunctionPtr
createAggregateFunctionMinMaxK(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings * settings)
{
    if (argument_types.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least two argument.", name);

    UInt64 k = 10; /// default values

    if (!params.empty())
    {
        if (params.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires one parameter.", name);

        k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
        if (k > TOP_K_MAX_SIZE)
            throw Exception(
                ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Too large parameter(s) for aggregate function {}. Maximum: {}", name, TOP_K_MAX_SIZE);

        if (k == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter 0 is illegal for aggregate function {}", name);
    }

    /// how many more values to keep around for changelog processing to workaround retract scenarios
    UInt64 max_size = k * settings->retract_k_multiplier.value;
    return std::make_shared<AggregateFunctionMinMaxKTuple<is_min>>(k, max_size, argument_types, params);
}

#define DISPATCH(TYPE, M, ...) \
    do \
    { \
        switch (WhichDataType(TYPE).idx) \
        { \
            case TypeIndex::UInt8: \
                M(UInt8, ##__VA_ARGS__); \
                break; \
            case TypeIndex::UInt16: \
                M(UInt16, ##__VA_ARGS__); \
                break; \
            case TypeIndex::UInt32: \
                M(UInt32, ##__VA_ARGS__); \
                break; \
            case TypeIndex::UInt64: \
                M(UInt64, ##__VA_ARGS__); \
                break; \
            case TypeIndex::UInt128: \
                M(UInt128, ##__VA_ARGS__); \
                break; \
            case TypeIndex::UInt256: \
                M(UInt256, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Enum8: \
                [[fallthrough]]; \
            case TypeIndex::Int8: \
                M(Int8, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Enum16: \
                [[fallthrough]]; \
            case TypeIndex::Int16: \
                M(Int16, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Int32: \
                M(Int32, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Int64: \
                M(Int64, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Int128: \
                M(Int128, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Int256: \
                M(Int256, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Float32: \
                M(Float32, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Float64: \
                M(Float64, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Date: \
                M(DataTypeDate::FieldType, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Date32: \
                M(DataTypeDate32::FieldType, ##__VA_ARGS__); \
                break; \
            case TypeIndex::DateTime: \
                M(DataTypeDateTime::FieldType, ##__VA_ARGS__); \
                break; \
            case TypeIndex::DateTime64: \
                M(DataTypeDateTime64::FieldType, ##__VA_ARGS__); \
                break; \
            case TypeIndex::String: \
                M(StringRef, ##__VA_ARGS__); \
                break; \
            case TypeIndex::FixedString: \
                M(StringRef, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Decimal32: \
                M(Decimal32, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Decimal64: \
                M(Decimal64, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Decimal128: \
                M(Decimal128, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Decimal256: \
                M(Decimal256, ##__VA_ARGS__); \
                break; \
            case TypeIndex::Tuple: \
                M(TupleValue, ##__VA_ARGS__); \
                break; \
            case TypeIndex::IPv4: \
                M(DataTypeIPv4::FieldType, ##__VA_ARGS__); \
                break; \
            case TypeIndex::IPv6: \
                M(DataTypeIPv6::FieldType, ##__VA_ARGS__); \
                break; \
            default: { \
                throw Exception( \
                    ErrorCodes::BAD_ARGUMENTS, \
                    "The min_k/max_k doesn't support argument type '{}'", \
                    typeIndexToTypeName(WhichDataType(TYPE).idx)); \
            } \
        } \
    } while (0)

#define BUILD_VALUE_COMPARER(TYPE, ...) buildValueComparer<TYPE, is_min>(__VA_ARGS__)
#define BUILD_VALUE_GETTER(TYPE, ...) buildValueGetter<TYPE>(__VA_ARGS__)
#define BUILD_VALUE_APPENDER(TYPE, ...) buildValueAppender<TYPE>(__VA_ARGS__)
#define BUILD_VALUE_WRITER(TYPE, ...) buildValueWriter<TYPE>(__VA_ARGS__)
#define BUILD_VALUE_READER(TYPE, ...) buildValueReader<TYPE>(__VA_ARGS__)

template <typename TYPE, bool is_min>
void buildValueComparer(DataTypePtr data_type, std::vector<TupleOperators::Comparer> & comparers)
{
    /// For ColumnTuple, we will build comparers for each elem column
    if constexpr (std::is_same_v<TYPE, TupleValue>)
    {
        std::vector<TupleOperators::Comparer> elem_comparers;

        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
        for (auto elem_type : tuple_type.getElements())
        {
            DISPATCH(elem_type, BUILD_VALUE_COMPARER, elem_type, elem_comparers);
        }

        /// build elem comparers
        comparers.emplace_back([inner_comparers = std::move(elem_comparers)](const std::any & l, const std::any & r) {
            TupleValue tuple_value1 = std::any_cast<const TupleValue &>(l);
            TupleValue tuple_value2 = std::any_cast<const TupleValue &>(r);
            assert(tuple_value1.values.size() == tuple_value2.values.size());
            assert(inner_comparers.size() == tuple_value1.values.size());

            for (size_t index = 0; index < tuple_value1.values.size(); ++index)
            {
                auto res = inner_comparers[index](tuple_value1.values[index], tuple_value2.values[index]);
                if (res != 0)
                    return res;
            }

            return 0;
        });
    }
    /// else if (more nested column)
    // FIXME, support more nested column such as `ColumnSparse`
    else
    {
        comparers.emplace_back([](const std::any & l, const std::any & r) {
            if constexpr (is_min)
                return DB::CompareHelper<TYPE>::compare(
                    std::any_cast<const TYPE &>(l), std::any_cast<const TYPE &>(r), /* nan_direction_hint */ 1);
            else
                return DB::CompareHelper<TYPE>::compare(
                    std::any_cast<const TYPE &>(l), std::any_cast<const TYPE &>(r), /* nan_direction_hint */ -1);
        });
    }
}

template <typename TYPE>
void buildValueGetter(DataTypePtr data_type, size_t col_index, std::vector<TupleOperators::Getter> & getters)
{
    /// For ColumnTuple, we will build getters for each elem column
    if constexpr (std::is_same_v<TYPE, TupleValue>)
    {
        std::vector<TupleOperators::Getter> elem_getters;

        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
        for (size_t i = 0; auto elem_type : tuple_type.getElements())
        {
            DISPATCH(elem_type, BUILD_VALUE_GETTER, elem_type, i, elem_getters);
            ++i;
        }

        getters.emplace_back(
            [inner_getters = std::move(elem_getters),
             col_index](const DB::IColumn ** columns, size_t row_num) -> std::pair<std::any, bool> {
                const auto & tuple_column = assert_cast<const DB::ColumnTuple &>(*columns[col_index]);
                const IColumn * elem_columns[tuple_column.tupleSize()];

                std::vector<std::any> values;
                std::vector<size_t> string_ref_indexs;
                for (size_t index = 0; index < tuple_column.tupleSize(); ++index)
                {
                    elem_columns[index] = tuple_column.getColumnPtr(index).get();
                    auto [elem_value, is_string_ref] = inner_getters[index](elem_columns, row_num);
                    if (is_string_ref)
                        string_ref_indexs.push_back(values.size());

                    values.emplace_back(std::move(elem_value));
                }
                return {TupleValue(std::move(values), std::move(string_ref_indexs)), false};
            });
    }
    /// else if (more nested column)
    else if constexpr (std::is_same_v<TYPE, StringRef>)
    {
        getters.emplace_back([=](const DB::IColumn ** columns, size_t row_num) -> std::pair<std::any, bool> {
            return {columns[col_index]->getDataAt(row_num), true};
        });
    }
    else if constexpr (DB::is_decimal<TYPE>)
    {
        getters.emplace_back([=](const DB::IColumn ** columns, size_t row_num) -> std::pair<std::any, bool> {
            const auto & column = assert_cast<const DB::ColumnDecimal<TYPE> &>(*columns[col_index]);
            return {column.getElement(row_num), false};
        });
    }
    else
    {
        assert(DB::isColumnedAsNumber(data_type) || DB::isEnum(data_type));
        getters.emplace_back([=](const DB::IColumn ** columns, size_t row_num) -> std::pair<std::any, bool> {
            const auto & column = assert_cast<const DB::ColumnVector<TYPE> &>(*columns[col_index]);
            return {column.getElement(row_num), false};
        });
    }
}

template <typename TYPE>
void buildValueAppender(DataTypePtr data_type, size_t tuple_element_index, std::vector<TupleOperators::Appender> & appenders)
{
    if constexpr (std::is_same_v<TYPE, TupleValue>)
    {
        std::vector<TupleOperators::Appender> elem_appenders;
        auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
        for (size_t i = 0; auto elem_type : tuple_type.getElements())
        {
            DISPATCH(elem_type, BUILD_VALUE_APPENDER, elem_type, i, elem_appenders);
            ++i;
        }

        /// build elem appenders
        appenders.emplace_back(
            [inner_appenders = std::move(elem_appenders), tuple_element_index](const std::any & val, DB::ColumnTuple & to_column) {
                auto & tuple_column = assert_cast<DB::ColumnTuple &>(to_column.getColumn(tuple_element_index));
                const auto & tuple_value = std::any_cast<const TupleValue &>(val);
                assert(tuple_value.values.size() == inner_appenders.size());

                for (size_t index = 0; index < tuple_value.values.size(); ++index)
                    inner_appenders[index](tuple_value.values[index], tuple_column);
            });
    }
    else if constexpr (std::is_same_v<TYPE, StringRef>)
    {
        appenders.emplace_back([=](const std::any & val, DB::ColumnTuple & to_column) {
            const auto & string_ref = std::any_cast<const TYPE &>(val);
            to_column.getColumn(tuple_element_index).insertData(string_ref.data, string_ref.size);
        });
    }
    else if constexpr (DB::is_decimal<TYPE>)
    {
        appenders.emplace_back([=](const std::any & val, DB::ColumnTuple & to_column) {
            auto & column = assert_cast<DB::ColumnDecimal<TYPE> &>(to_column.getColumn(tuple_element_index));
            column.insertValue(std::any_cast<const TYPE &>(val));
        });
    }
    else
    {
        assert(DB::isColumnedAsNumber(data_type) || DB::isEnum(data_type));
        appenders.emplace_back([=](const std::any & val, DB::ColumnTuple & to_column) {
            auto & column = assert_cast<DB::ColumnVector<TYPE> &>(to_column.getColumn(tuple_element_index));
            column.insertValue(std::any_cast<const TYPE &>(val));
        });
    }
}

template <typename TYPE>
void buildValueWriter(DataTypePtr data_type, std::vector<TupleOperators::Writer> & writers)
{
    if constexpr (std::is_same_v<TYPE, TupleValue>)
    {
        std::vector<TupleOperators::Writer> elem_writers;

        auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
        for (auto elem_type : tuple_type.getElements())
        {
            DISPATCH(elem_type, BUILD_VALUE_WRITER, elem_type, elem_writers);
        }

        /// build elem appenders
        writers.emplace_back([inner_writers = std::move(elem_writers)](const std::any & val, WriteBuffer & buf) {
            const auto & tuple_value = std::any_cast<const TupleValue &>(val);
            assert(tuple_value.values.size() == inner_writers.size());

            for (size_t index = 0; index < tuple_value.values.size(); ++index)
            {
                inner_writers[index](tuple_value.values[index], buf);
            }
        });
    }
    else
    {
        writers.emplace_back([](const std::any & val, WriteBuffer & buf) { writeBinary(std::any_cast<const TYPE &>(val), buf); });
    }
}

template <typename TYPE>
void buildValueReader(DataTypePtr data_type, std::vector<TupleOperators::Reader> & readers)
{
    if constexpr (std::is_same_v<TYPE, TupleValue>)
    {
        std::vector<TupleOperators::Reader> elem_readers;

        auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
        for (auto elem_type : tuple_type.getElements())
        {
            DISPATCH(elem_type, BUILD_VALUE_READER, elem_type, elem_readers);
        }

        /// build elem appenders
        readers.emplace_back([inner_readers = std::move(elem_readers)](ReadBuffer & buf, ArenaWithFreeLists * arena) -> std::any {
            std::vector<std::any> values;
            values.reserve(inner_readers.size());
            for (size_t index = 0; index < inner_readers.size(); ++index)
            {
                auto elem_value = inner_readers[index](buf, arena);
                values.emplace_back(std::move(elem_value));
            }
            return TupleValue{std::move(values), {}};
        });
    }
    else if constexpr (std::is_same_v<TYPE, StringRef>)
    {
        readers.emplace_back([](ReadBuffer & buf, ArenaWithFreeLists * arena) -> std::any {
            auto string_ref = readStringBinaryInto(*arena, buf);
            return string_ref;
        });
    }
    else
    {
        readers.emplace_back([](ReadBuffer & buf, ArenaWithFreeLists * arena) -> std::any {
            TYPE val;
            readBinary(val, buf);
            return val;
        });
    }
}
}

template <bool is_min>
TupleOperators AggregateFunctionMinMaxKTuple<is_min>::buildTupleValueOperators() const
{
    TupleOperators operators;
    for (size_t col_idx = 0; col_idx < this->argument_types.size(); ++col_idx)
    {
        const auto & arg_type = this->argument_types[col_idx];

        /// Assume the last argument is `_tp_delta`
        if (col_idx == this->argument_types.size() - 1)
        {
            assert(WhichDataType(this->argument_types[col_idx]).isInt8());
            continue;
        }

        /// Build operators for compare/get/append/write/read values from columns[col_idx] (Column)
        DISPATCH(arg_type, BUILD_VALUE_COMPARER, arg_type, operators.comparers);
        DISPATCH(arg_type, BUILD_VALUE_GETTER, arg_type, col_idx, operators.getters);
        DISPATCH(arg_type, BUILD_VALUE_APPENDER, arg_type, col_idx, operators.appenders);
        DISPATCH(arg_type, BUILD_VALUE_WRITER, arg_type, operators.writers);
        DISPATCH(arg_type, BUILD_VALUE_READER, arg_type, operators.readers);
    }

    assert(operators.comparers.size() == this->argument_types.size() - 1);
    assert(operators.getters.size() == this->argument_types.size() - 1);
    assert(operators.appenders.size() == this->argument_types.size() - 1);
    assert(operators.writers.size() == this->argument_types.size() - 1);
    assert(operators.readers.size() == this->argument_types.size() - 1);
#undef BUILD_PARTIAL_COMPARE
#undef BUILD_VALUE_GETTER
#undef BUILD_VALUE_APPENDER
#undef BUILD_VALUE_WRITER
#undef BUILD_VALUE_READER
    return operators;
}

template <typename TYPE>
void appendSingleValue(DataTypePtr data_type, ColumnArray & arr_to, auto & counted_map, UInt64 k)
{
    ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
    size_t res_count = 0;
    if constexpr (std::is_same_v<TYPE, TupleValue>)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expect append single value but got tuple value, it's bug");
    }
    else if constexpr (std::is_same_v<TYPE, StringRef>)
    {
        auto & col_to = arr_to.getData();
        for (const auto & [tuple_value, count] : counted_map)
        {
            const auto & string_ref = std::any_cast<const TYPE &>(tuple_value.values[0]);
            for (size_t i = 0; i < count; ++i)
            {
                if (res_count >= k)
                    return offsets_to.push_back(col_to.size());

                col_to.insertData(string_ref.data, string_ref.size);
                ++res_count;
            }
        }
        offsets_to.push_back(col_to.size());
    }
    else if constexpr (DB::is_decimal<TYPE>)
    {
        auto & col_to = assert_cast<ColumnDecimal<TYPE> &>(arr_to.getData());
        for (const auto & [tuple_value, count] : counted_map)
        {
            for (size_t i = 0; i < count; ++i)
            {
                if (res_count >= k)
                    return offsets_to.push_back(col_to.size());

                col_to.insertValue(std::any_cast<const TYPE &>(tuple_value.values[0]));
                ++res_count;
            }
        }

        offsets_to.push_back(col_to.size());
    }
    else
    {
        assert(DB::isColumnedAsNumber(data_type) || DB::isEnum(data_type));
        auto & col_to = assert_cast<ColumnVector<TYPE> &>(arr_to.getData());
        for (const auto & [tuple_value, count] : counted_map)
        {
            for (size_t i = 0; i < count; ++i)
            {
                if (res_count >= k)
                    return offsets_to.push_back(col_to.size());

                col_to.insertValue(std::any_cast<const TYPE &>(tuple_value.values[0]));
                ++res_count;
            }
        }

        offsets_to.push_back(col_to.size());
    }
}

template <bool is_min>
void AggregateFunctionMinMaxKTuple<is_min>::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const
{
#define APPENED_SINGLE_VALUE(TYPE, ...) appendSingleValue<TYPE>(__VA_ARGS__)
    auto & counted_map = this->data(place);
    auto & arr_to = assert_cast<ColumnArray &>(to);

    /// Return Array(type) if only one argument and which isn't tuple
    /// e.g. min_k(int, 3, _tp_delta) -> [1, 2 ,3]
    if (this->argument_types.size() == 2 && !isTuple(this->argument_types[0]))
    {
        DISPATCH(this->argument_types[0], APPENED_SINGLE_VALUE, this->argument_types[0], arr_to, counted_map, k);
    }
    else
    {
        /// Return Array(Tuple(type))
        /// e.g. min_k(int, 3, string, _tp_delta) -> [(1, 'a'), (2, 'b'), (3, 'c')]
        auto & tuple_to = assert_cast<ColumnTuple &>(arr_to.getData());
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        size_t res_count = 0;
        for (const auto & [tuple_value, count] : counted_map)
        {
            for (size_t j = 0; j < count; ++j)
            {
                if (res_count >= k)
                    return offsets_to.push_back(tuple_to.size());

                assert(tuple_value.values.size() == counted_map.operators.appenders.size());
                for (size_t i = 0; i < counted_map.operators.appenders.size(); ++i)
                    counted_map.operators.appenders[i](tuple_value.values[i], tuple_to);

                ++res_count;
            }
        }

        offsets_to.push_back(tuple_to.size());
    }
#undef APPENED_SINGLE_VALUE
}

#undef DISPATCH

void registerAggregateFunctionMinMaxKRetract(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    factory.registerFunction("__max_k_retract", {Streaming::createAggregateFunctionMinMaxK<false>, properties});
    factory.registerFunction("__min_k_retract", {Streaming::createAggregateFunctionMinMaxK<true>, properties});
}

}
}

#pragma clang diagnostic pop
