#include "AggregateFunctionMinMaxK.h"

#include "AggregateFunctionFactory.h"
#include "FactoryHelpers.h"
#include "Helpers.h"

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


namespace
{

    /// Substitute return type for Date and DateTime
    template <bool is_min>
    class AggregateFunctionMinMaxKDate : public AggregateFunctionMinMaxK<DataTypeDate::FieldType, is_min>
    {
        using AggregateFunctionMinMaxK<DataTypeDate::FieldType, is_min>::AggregateFunctionMinMaxK;
        DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
    };

    template <bool is_min>
    class AggregateFunctionMinMaxKDateTime : public AggregateFunctionMinMaxK<DataTypeDateTime::FieldType, is_min>
    {
        using AggregateFunctionMinMaxK<DataTypeDateTime::FieldType, is_min>::AggregateFunctionMinMaxK;
        DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
    };


    template <bool is_min>
    static IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, UInt64 k, const Array & params)
    {
        WhichDataType which(argument_type);
        if (which.idx == TypeIndex::Date)
            return new AggregateFunctionMinMaxKDate<is_min>(k, argument_type, params);
        if (which.idx == TypeIndex::DateTime)
            return new AggregateFunctionMinMaxKDateTime<is_min>(k, argument_type, params);

        /// Check that we can use plain version of AggregateFunctionMinMaxKGeneric
        if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggregateFunctionMinMaxKGeneric<true, is_min>(k, argument_type, params);
        else
            return new AggregateFunctionMinMaxKGeneric<false, is_min>(k, argument_type, params);
    }


    template <bool is_min>
    AggregateFunctionPtr
    createAggregateFunctionMinMaxK(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        if (argument_types.size() < 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires at least one argument.", name);

        UInt64 k = 10; /// default values

        if (!params.empty())
        {
            if (params.size() > 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires one parameter.", name);

            k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
            if (k > TOP_K_MAX_SIZE)

                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Too large parameter(s) for aggregate function {}. Maximum: {}",
                    name,
                    TOP_K_MAX_SIZE);

            if (k == 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter 0 is illegal for aggregate function {}", name);
        }

        AggregateFunctionPtr res;
        if (argument_types.size() == 1)
        {
            if (isDecimal(argument_types[0]))
                res.reset(createWithDecimalType<AggregateFunctionMinMaxK, is_min>(*argument_types[0], k, argument_types[0], params));
            else
                res.reset(createWithNumericType<AggregateFunctionMinMaxK, is_min>(*argument_types[0], k, argument_types[0], params));
            if (!res)
                res = AggregateFunctionPtr(createWithExtraTypes<is_min>(argument_types[0], k, params));
        }
        else
        {
            res = AggregateFunctionPtr(new AggregateFunctionMinMaxKTuple<is_min>(k, argument_types, params));
        }

        if (!res)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function {}",
                argument_types[0]->getName(),
                name);

        return res;
    }

    void cast_from(const IColumn &icolumn,std::vector<std::any> &values, size_t row_num){
        switch (icolumn.getDataType()){
            case TypeIndex::Int32 :
                values.emplace_back(assert_cast< const DB::ColumnVector<Int32> & >(icolumn).getElement(row_num));
                return ;
            case TypeIndex::Decimal32 :
                values.emplace_back(assert_cast< const DB::ColumnDecimal<Decimal32> & >(icolumn).getElement(row_num));
                return ;
        default:
            return ;
        }
    }


    // void  insert_into_cast(auto icolumn,std::any val)
    // {

    //     switch (icolumn.getDataType()){
    //         case TypeIndex::Int32 :
    //             auto & column= assert_cast< const DB::ColumnVector<Int32> & >(icolumn);
    //             column.insertValue(std::any_cast<const Int32 &>(val));
    //             return ;
    //         case TypeIndex::Decimal32 :
    //             column= assert_cast< const DB::ColumnDecimal<Decimal32> & >(icolumn);
    //             column.insertValue(std::any_cast<const Decimal32 &>(val));
    //             return ;
    //     default:
    //         return ;
    //     // auto & column= assert_cast<DB::ColumnVector<Int32> &>(icolumn);
    //     // column.insertValue(std::any_cast<const Int32 &>(tuple_value.values[idx]));
    // }

    template <typename TYPE, bool is_min>
    constexpr decltype(auto) buildValueComparer()
    {
        return [](const std::any & l, const std::any & r) {
            assert(l.type() == typeid(TYPE));
            assert(r.type() == typeid(TYPE));
            if constexpr (is_min)
                return DB::CompareHelper<TYPE>::compare(
                    std::any_cast<const TYPE &>(l), std::any_cast<const TYPE &>(r), /* nan_direction_hint */ 1);
            else
                return DB::CompareHelper<TYPE>::compare(
                    std::any_cast<const TYPE &>(l), std::any_cast<const TYPE &>(r), /* nan_direction_hint */ -1);
        };
    }

    void cast_operators_from(const IColumn &icolumn, TupleOperators & operators)
    {
        switch (icolumn.getDataType()){
            case TypeIndex::Int32 :
                operators.comparers.emplace_back(buildValueComparer<Int32 , false>());
                return ;
            case TypeIndex::Decimal32 :
                operators.comparers.emplace_back(buildValueComparer<Decimal32 , false>());
                return ;
        default:
            return ;
        }
    }
    
    template <typename TYPE, TypeCategory type_category>
    constexpr decltype(auto) buildValueGetter(size_t col_index)
    {
        return [=](const DB::IColumn ** columns, size_t row_num, Arena * arena) -> std::pair<std::any, TypeCategory> {
            if constexpr (type_category == TypeCategory::SEIRIALIZED_STRING_REF)
            {
                const char * begin = nullptr;
                StringRef str_serialized = columns[col_index]->serializeValueIntoArena(row_num, *arena, begin);
                return {str_serialized, type_category};
            }
            else if constexpr (type_category == TypeCategory::STRING_REF)
                return {columns[col_index]->getDataAt(row_num), type_category};
            else if constexpr (type_category == TypeCategory::DECIMAL)
                {
                    const auto & column = assert_cast<const DB::ColumnDecimal<TYPE> &>(*columns[col_index]);
                    return {column.getElement(row_num), type_category};
                }
            else if constexpr (type_category == TypeCategory::TUPLE)
                {
                    static TupleOperators  operators;
                    std::vector<size_t> string_ref_indexs;
                    std::vector<std::any> values;
                    operators.comparers.clear();

                    const auto & tuple_column = assert_cast<const DB::ColumnTuple &>(*columns[col_index]);
                    for (unsigned int i=0;i<tuple_column.getColumns().size();i++)
                    {
                        // values.push_back(assert_cast<const DB::ColumnVector<Int32> &>(tuple_column.getColumn(i)).getElement(row_num));
                        // operators.comparers.emplace_back(buildValueComparer<Int32 , false>());
                        
                        auto &icolumn=tuple_column.getColumn(i);
                        cast_from(icolumn,values,row_num);
                        cast_operators_from(icolumn,operators);
                    }
                    return {TupleValue(std::move(values),std::move(string_ref_indexs),std::move(operators)), type_category};
                }
            else
            {
                const auto & column = assert_cast<const DB::ColumnVector<TYPE> &>(*columns[col_index]);
                return {column.getElement(row_num), type_category};
            }
        };
    }

    template <typename TYPE, TypeCategory type_category>
    constexpr decltype(auto) buildValueAppender(size_t tuple_element_index)
    {
        return [=](const std::any & val, DB::ColumnTuple & to_column) {
            if constexpr (type_category == TypeCategory::SEIRIALIZED_STRING_REF)
            {
                const auto & string_ref = std::any_cast<const TYPE &>(val);
                to_column.getColumn(tuple_element_index).deserializeAndInsertFromArena(string_ref.data);
            }
            else if constexpr (type_category == TypeCategory::STRING_REF)
            {
                const auto & string_ref = std::any_cast<const TYPE &>(val);
                to_column.getColumn(tuple_element_index).insertData(string_ref.data, string_ref.size);
            }
            else if constexpr (type_category == TypeCategory::DECIMAL)
            {
                auto & column = assert_cast<DB::ColumnDecimal<TYPE> &>(to_column.getColumn(tuple_element_index));
                column.insertValue(std::any_cast<const TYPE &>(val));
            }
            else if constexpr (type_category == TypeCategory::TUPLE)
            {
                const auto & tuple_value = std::any_cast<const TYPE &>(val);
                auto & column_tuple = assert_cast<DB::ColumnTuple &>(to_column.getColumn(tuple_element_index));
                for (unsigned int idx = 0;idx < column_tuple.getColumns().size();idx++)
                {
                    auto & icolumn=column_tuple.getColumn(idx);
                    // auto & column= assert_cast<DB::ColumnVector<Int32> &>(icolumn);
                    // column.insertValue(std::any_cast<const Int32 &>(tuple_value.values[idx]));

                    // insert_into_cast(icolumn,tuple_value.values[idx]);

                    switch (icolumn.getDataType()){
                        case TypeIndex::Int32 :
                            {auto & column= assert_cast< DB::ColumnVector<Int32> & >(icolumn);
                            column.insertValue(std::any_cast<const Int32 &>(tuple_value.values[idx]));
                            break;}
                        case TypeIndex::Decimal32 :
                            {auto & column= assert_cast< DB::ColumnDecimal<Decimal32> & >(icolumn);
                            column.insertValue(std::any_cast<const Decimal32 &>(tuple_value.values[idx]));
                            break;}
                    default:
                        return ;
                    }
                }
            }
            else
            {
                auto & column = assert_cast<DB::ColumnVector<TYPE> &>(to_column.getColumn(tuple_element_index));
                column.insertValue(std::any_cast<const TYPE &>(val));
            }
        };
    }

    template <typename TYPE>
    constexpr decltype(auto) buildValueWriter()
    {
        return [](const std::any & val, WriteBuffer & buf) { writeBinary(std::any_cast<const TYPE &>(val), buf); };
    }

    template <typename TYPE>
    constexpr decltype(auto) buildValueReader()
    {
        return [](ReadBuffer & buf, Arena * arena) -> std::pair<std::any, size_t> {
            if constexpr (std::is_same_v<TYPE, StringRef> || std::is_same_v<TYPE, TupleValue> )
            {
                auto string_ref = readStringBinaryInto(*arena, buf);
                return {string_ref, string_ref.size};
            }
            else
            {
                TYPE val;
                readBinary(val, buf);
                return {val, 0};
            }
        };
    }

    #undef ASSERT_CAST_COLUMN
    #undef DISPATCH_BY_TYPEINDEX
}

#define DISPATCH(TYPE, M, ...) \
    do \
    { \
        switch (WhichDataType(TYPE).idx) \
        { \
            case TypeIndex::Date: \
                M(DataTypeDate::FieldType, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::DateTime: \
                M(typename DataTypeDateTime::FieldType, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Enum8: [[fallthrough]]; \
            case TypeIndex::UInt8: \
                M(UInt8, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Enum16: [[fallthrough]]; \
            case TypeIndex::UInt16: \
                M(UInt16, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::UInt32: \
                M(UInt32, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::UInt64: \
                M(UInt64, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::UInt128: \
                M(UInt128, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::UInt256: \
                M(UInt256, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Int8: \
                M(Int8, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Int16: \
                M(Int16, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Int32: \
                M(Int32, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Int64: \
                M(Int64, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Int128: \
                M(Int128, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Int256: \
                M(Int256, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Float32: \
                M(Float32, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Float64: \
                M(Float64, TypeCategory::NUMERIC, ##__VA_ARGS__); break; \
            case TypeIndex::Decimal32: \
                M(Decimal32, TypeCategory::DECIMAL, ##__VA_ARGS__); break; \
            case TypeIndex::Decimal64: \
                M(Decimal64, TypeCategory::DECIMAL, ##__VA_ARGS__); break; \
            case TypeIndex::Decimal128: \
                M(Decimal128, TypeCategory::DECIMAL, ##__VA_ARGS__); break; \
            case TypeIndex::Decimal256: \
                M(Decimal256, TypeCategory::DECIMAL, ##__VA_ARGS__); break; \
            case TypeIndex::Tuple: \
                M(TupleValue, TypeCategory::TUPLE, ##__VA_ARGS__); break; \
            default: \
            { \
                if (TYPE->isValueUnambiguouslyRepresentedInContiguousMemoryRegion()) \
                    M(StringRef, TypeCategory::STRING_REF, ##__VA_ARGS__); \
                else \
                    M(StringRef, TypeCategory::SEIRIALIZED_STRING_REF, ##__VA_ARGS__); \
            } \
        } \
    } while (0)

template <bool is_min>
void AggregateFunctionMinMaxKTuple<is_min>::buildTupleValueOperators(TupleOperators & operators) const
{
    assert(this->argument_types.size() > 1);

#define BUILD_PARTIAL_COMPARER(TYPE, TYPE_CATEGORY, ...) operators.comparers.emplace_back(buildValueComparer<TYPE, is_min>(__VA_ARGS__))
#define BUILD_VALUE_GETTER(TYPE, TYPE_CATEGORY, ...) operators.getters.emplace_back(buildValueGetter<TYPE, TYPE_CATEGORY>(__VA_ARGS__))
#define BUILD_VALUE_APPENDER(TYPE, TYPE_CATEGORY, ...) \
    operators.appenders.emplace_back(buildValueAppender<TYPE, TYPE_CATEGORY>(__VA_ARGS__))
#define BUILD_VALUE_WRITER(TYPE, TYPE_CATEGORY, ...) operators.writers.emplace_back(buildValueWriter<TYPE>(__VA_ARGS__))
#define BUILD_VALUE_READER(TYPE, TYPE_CATEGORY, ...) operators.readers.emplace_back(buildValueReader<TYPE>(__VA_ARGS__))

    for (size_t col_idx = 0; col_idx < this->argument_types.size(); ++col_idx)
    {
        const auto & arg_type = this->argument_types[col_idx];

        /// Compare by first argument
        if (col_idx == 0)
        {  
           if (WhichDataType(arg_type).idx == TypeIndex::Tuple)
            {
            auto  c0=assert_cast<const DataTypeTuple &>(*arg_type).getElement(0);
            String t=c0->getName();
            std::cout<<t;
            }
            DISPATCH(arg_type, BUILD_PARTIAL_COMPARER);
        }

        /// Get value from columns[col_idx] (Column)
        DISPATCH(arg_type, BUILD_VALUE_GETTER, col_idx);
        DISPATCH(arg_type, BUILD_VALUE_APPENDER, col_idx);
        DISPATCH(arg_type, BUILD_VALUE_WRITER);
        DISPATCH(arg_type, BUILD_VALUE_READER);
    }

    assert(operators.comparers.size() == 1);
    assert(operators.getters.size() == this->argument_types.size());
    assert(operators.appenders.size() == this->argument_types.size());
    assert(operators.writers.size() == this->argument_types.size());
    assert(operators.readers.size() == this->argument_types.size());
#undef BUILD_PARTIAL_COMPARE
#undef BUILD_VALUE_GETTER
#undef BUILD_VALUE_APPENDER
#undef BUILD_VALUE_WRITER
#undef BUILD_VALUE_READER
}

template <bool is_min>
void AggregateFunctionMinMaxKTuple<is_min>::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const
{
    auto & arr_to = assert_cast<ColumnArray &>(to);
    auto & tuple_to = assert_cast<ColumnTuple &>(arr_to.getData());
    ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

    auto & top_k = this->data(place);
    top_k.sort();

    for (const auto & tuple_value : top_k)
    {
        assert(tuple_value.values.size() == top_k.operators.appenders.size());
        for (size_t i = 0; i < top_k.operators.appenders.size(); ++i)
            top_k.operators.appenders[i](tuple_value.values[i], tuple_to);
    }

    offsets_to.push_back(tuple_to.size());
}

#undef DISPATCH

void registerAggregateFunctionMinMaxK(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    factory.registerFunction("max_k", {createAggregateFunctionMinMaxK<false>, properties});
    factory.registerFunction("min_k", {createAggregateFunctionMinMaxK<true>, properties});
}

}
