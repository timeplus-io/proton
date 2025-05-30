#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <Interpreters/castColumn.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Functions/array/arrayIndex.h>
#include <Functions/like.h>
#include <Functions/FunctionsStringSearch.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

namespace
{

/// proton : starts
DataTypePtr getReturnTypeImplForArrays(const DataTypes & arguments, const String & name)
{
    if (arguments.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} requires 2 arguments, but {} given",
            name,
            arguments.size());

    auto get_nested_type = [&](const DataTypePtr & type)
    {
        DataTypePtr nested;
        if (const auto * type_as_array = checkAndGetDataType<DataTypeArray>(type.get()))
            nested = type_as_array->getNestedType();
        else if (const auto * type_as_map = checkAndGetDataType<DataTypeMap>(type.get()))
            nested = std::make_shared<DataTypeTuple>(type_as_map->getKeyValueTypes());
        else
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Arguments of function {} must be Array or Map, but {} is given",
                name,
                type->getName());

        return nested;
    };

    auto key_type = get_nested_type(arguments[0]);
    auto value_type = get_nested_type(arguments[1]);

    /// We accept Array(Nullable(T)) or Array(LowCardinality(Nullable(T))) as key types as long as the actual array doesn't contain NULL value(this is checked in executeImpl).
    key_type = removeNullableOrLowCardinalityNullable(key_type);

    DataTypes key_value_types{key_type, value_type};
    return std::make_shared<DataTypeMap>(key_value_types);
}
/// proton : ends

// map_cast(x, y, ...) is a function that allows you to make key-value pair
// map_cast(array1, array2)
// map_cast(map1, map2)
class FunctionMap : public IFunction
{
public:
    static constexpr auto name = "map_cast";

    explicit FunctionMap(ContextPtr context_)
        : context(context_)
        , function_array(FunctionFactory::instance().get("array_cast", context))
        , function_map_from_arrays(FunctionFactory::instance().get("map_from_arrays", context))
    {
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionMap>(context);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    /// map_cast(..., nothing) -> map(..., nothing)
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() % 2 != 0)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires even number of arguments, but {} given", getName(), arguments.size());

        /// proton : starts
        if (checkAndGetDataType<DataTypeArray>(arguments[0].get()) || checkAndGetDataType<DataTypeMap>(arguments[0].get()))
            /// Proxy to map_from_arrays
            return getReturnTypeImplForArrays(arguments, getName());
        /// proton : ends

        DataTypes keys;
        DataTypes values;
        for (size_t i = 0; i < arguments.size(); i += 2)
        {
            keys.emplace_back(arguments[i]);
            values.emplace_back(arguments[i + 1]);
        }

        DataTypes tmp;
        tmp.emplace_back(getLeastSupertype(keys));
        tmp.emplace_back(getLeastSupertype(values));
        return std::make_shared<DataTypeMap>(tmp);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// proton : starts
        if (checkAndGetDataType<DataTypeArray>(arguments[0].type.get()) || checkAndGetDataType<DataTypeMap>(arguments[0].type.get()))
            /// Proxy to map_from_arrays
            return function_map_from_arrays->build(arguments)->execute(arguments, result_type, input_rows_count, /*dry_run=*/false);
        /// proton : ends

        size_t num_elements = arguments.size();
        if (num_elements == 0)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        ColumnsWithTypeAndName key_args;
        ColumnsWithTypeAndName value_args;
        for (size_t i = 0; i < num_elements; i += 2)
        {
            key_args.emplace_back(arguments[i]);
            value_args.emplace_back(arguments[i+1]);
        }

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();
        const DataTypePtr & key_array_type = std::make_shared<DataTypeArray>(key_type);
        const DataTypePtr & value_array_type = std::make_shared<DataTypeArray>(value_type);

        /// key_array = array(args[0], args[2]...)
        ColumnPtr key_array = function_array->build(key_args)->execute(key_args, key_array_type, input_rows_count, /* dry_run = */ false);
        /// value_array = array(args[1], args[3]...)
        ColumnPtr value_array = function_array->build(value_args)->execute(value_args, value_array_type, input_rows_count, /* dry_run = */ false);

        /// result = mapFromArrays(key_array, value_array)
        ColumnsWithTypeAndName map_args{{key_array, key_array_type, ""}, {value_array, value_array_type, ""}};
        return function_map_from_arrays->build(map_args)->execute(map_args, result_type, input_rows_count, /* dry_run = */ false);
    }

private:
    ContextPtr context;
    FunctionOverloadResolverPtr function_array;
    FunctionOverloadResolverPtr function_map_from_arrays;
};

/// mapFromArrays(keys, values) is a function that allows you to make key-value pair from a pair of arrays or maps
class FunctionMapFromArrays : public IFunction
{
public:
    static constexpr auto name = "map_from_arrays";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapFromArrays>(); }
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return getReturnTypeImplForArrays(arguments, getName());
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        auto get_array_column = [&](const ColumnPtr & column) -> std::pair<const ColumnArray *, ColumnPtr>
        {
            bool is_const = isColumnConst(*column);
            ColumnPtr holder = is_const ? column->convertToFullColumnIfConst() : column;

            const ColumnArray * col_res = nullptr;
            if (const auto * col_array = checkAndGetColumn<ColumnArray>(holder.get()))
                col_res = col_array;
            else if (const auto * col_map = checkAndGetColumn<ColumnMap>(holder.get()))
                col_res = &col_map->getNestedColumn();
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Argument columns of function {} must be Array or Map, but {} is given",
                    getName(),
                    holder->getName());

            return {col_res, holder};
        };

        auto [col_keys, key_holder] = get_array_column(arguments[0].column);
        auto [col_values, values_holder] = get_array_column(arguments[1].column);

        /// Nullable(T) or LowCardinality(Nullable(T)) are okay as nested key types but actual NULL values are not okay.
        ColumnPtr data_keys = col_keys->getDataPtr();
        if (isColumnNullableOrLowCardinalityNullable(*data_keys))
        {
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(data_keys.get()))
            {
                const auto * null_map = &nullable->getNullMapData();
                if (null_map && !memoryIsZero(null_map->data(), 0, null_map->size()))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "The nested column of first argument in function {} must not contain NULLs", getName());

                data_keys = nullable->getNestedColumnPtr();
            }
            else if (const auto * low_cardinality = checkAndGetColumn<ColumnLowCardinality>(data_keys.get()))
            {
                if (low_cardinality->containsNull())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "The nested column of first argument in function {} must not contain NULLs", getName());

                data_keys = low_cardinality->cloneWithDefaultOnNull();
            }
        }

        if (!col_keys->hasEqualOffsets(*col_values))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Two arguments of function {} must have equal sizes", getName());

        const auto & data_values = col_values->getDataPtr();
        const auto & offsets = col_keys->getOffsetsPtr();
        auto nested_column = ColumnArray::create(ColumnTuple::create(Columns{std::move(data_keys), data_values}), offsets);
        return ColumnMap::create(nested_column);
    }
};

struct NameMapContains { static constexpr auto name = "map_contains"; };

class FunctionMapContains : public IFunction
{
public:
    static constexpr auto name = NameMapContains::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapContains>(); }

    String getName() const override
    {
        return NameMapContains::name;
    }

    size_t getNumberOfArguments() const override { return impl.getNumberOfArguments(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return impl.isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return impl.getReturnTypeImpl(arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return impl.executeImpl(arguments, result_type, input_rows_count);
    }

private:
    FunctionArrayIndex<HasAction, NameMapContains> impl;
};


class FunctionMapKeys : public IFunction
{
public:
    static constexpr auto name = "map_keys";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapKeys>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        auto key_type = map_type->getKeyType();

        return std::make_shared<DataTypeArray>(key_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnMap * col_map = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        if (!col_map)
            return nullptr;

        const auto & nested_column = col_map->getNestedColumn();
        const auto & keys_data = col_map->getNestedData().getColumn(0);

        return ColumnArray::create(keys_data.getPtr(), nested_column.getOffsetsPtr());
    }
};


class FunctionMapValues : public IFunction
{
public:
    static constexpr auto name = "map_values";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapValues>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        auto value_type = map_type->getValueType();

        return std::make_shared<DataTypeArray>(value_type);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnMap * col_map = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        if (!col_map)
            return nullptr;

        const auto & nested_column = col_map->getNestedColumn();
        const auto & values_data = col_map->getNestedData().getColumn(1);

        return ColumnArray::create(values_data.getPtr(), nested_column.getOffsetsPtr());
    }
};

class FunctionMapContainsKeyLike : public IFunction
{
public:
    static constexpr auto name = "map_contains_key_like";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapContainsKeyLike>(); }
    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*info*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        bool is_const = isColumnConst(*arguments[0].column);
        const ColumnMap * col_map = is_const ? checkAndGetColumnConstData<ColumnMap>(arguments[0].column.get())
                                             : checkAndGetColumn<ColumnMap>(arguments[0].column.get());
        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        if (!col_map || !map_type)
            throw Exception{"First argument for function " + getName() + " must be a map", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        auto col_res = ColumnUInt8::create();
        typename ColumnUInt8::Container & vec_res = col_res->getData();

        if (input_rows_count == 0)
            return col_res;

        vec_res.resize(input_rows_count);

        const auto & column_array = typeid_cast<const ColumnArray &>(col_map->getNestedColumn());
        const auto & column_tuple = typeid_cast<const ColumnTuple &>(column_array.getData());

        const ColumnString * column_string = checkAndGetColumn<ColumnString>(column_tuple.getColumn(0));
        const ColumnFixedString * column_fixed_string = checkAndGetColumn<ColumnFixedString>(column_tuple.getColumn(0));

        FunctionLike func_like;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t element_start_row = row != 0 ? column_array.getOffsets()[row-1] : 0;
            size_t elem_size = column_array.getOffsets()[row]- element_start_row;

            ColumnPtr sub_map_column;
            DataTypePtr data_type;

            //The keys of one row map will be processed as a single ColumnString
            if (column_string)
            {
               sub_map_column = column_string->cut(element_start_row, elem_size);
               data_type = std::make_shared<DataTypeString>();
            }
            else
            {
               sub_map_column = column_fixed_string->cut(element_start_row, elem_size);
               data_type = std::make_shared<DataTypeFixedString>(checkAndGetColumn<ColumnFixedString>(sub_map_column.get())->getN());
            }

            size_t col_key_size = sub_map_column->size();
            auto column = is_const? ColumnConst::create(std::move(sub_map_column), std::move(col_key_size)) : std::move(sub_map_column);

            ColumnsWithTypeAndName new_arguments =
                {
                    {
                        column,
                        data_type,
                        ""
                    },
                    arguments[1]
                };

            auto res = func_like.executeImpl(new_arguments, result_type, input_rows_count);
            const auto & container = checkAndGetColumn<ColumnUInt8>(res.get())->getData();

            const auto it = std::find_if(container.begin(), container.end(), [](int element){ return element == 1; });  // NOLINT
            vec_res[row] = it == container.end() ? 0 : 1;
        }

        return col_res;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                            + toString(arguments.size()) + ", should be 2",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        const DataTypeString * pattern_type = checkAndGetDataType<DataTypeString>(arguments[1].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        if (!pattern_type)
            throw Exception{"Second argument for function " + getName() + " must be string",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!isStringOrFixedString(map_type->getKeyType()))
            throw Exception{"Key type of map for function " + getName() + " must be `string` or `fixed_string`",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        /// proton: starts. return bool
        return DataTypeFactory::instance().get("bool");
        /// proton: ends.
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
};

class FunctionExtractKeyLike : public IFunction
{
public:
    static constexpr auto name = "map_extract_key_like";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionExtractKeyLike>(); }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*info*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());

        if (!map_type)
            throw Exception{"First argument for function " + getName() + " must be a map",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};


        auto key_type = map_type->getKeyType();

        WhichDataType which(key_type);

        if (!which.isStringOrFixedString())
            throw Exception{"Function " + getName() + "only support the map with string or fixed_string key",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!isStringOrFixedString(arguments[1].type))
            throw Exception{"Second argument passed to function " + getName() + " must be string or fixed_string",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeMap>(map_type->getKeyType(), map_type->getValueType());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        bool is_const = isColumnConst(*arguments[0].column);
        const ColumnMap * col_map = typeid_cast<const ColumnMap *>(arguments[0].column.get());

        //It may not be necessary to check this condition, cause it will be checked in getReturnTypeImpl function
        if (!col_map)
            return nullptr;

        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        auto key_type = map_type->getKeyType();
        auto value_type = map_type->getValueType();

        const auto & nested_column = col_map->getNestedColumn();
        const auto & keys_column = col_map->getNestedData().getColumn(0);
        const auto & values_column = col_map->getNestedData().getColumn(1);
        const ColumnString * keys_string_column = checkAndGetColumn<ColumnString>(keys_column);
        const ColumnFixedString * keys_fixed_string_column = checkAndGetColumn<ColumnFixedString>(keys_column);

        FunctionLike func_like;

        //create result data
        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        IColumn::Offset current_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t element_start_row = row != 0 ? nested_column.getOffsets()[row-1] : 0;
            size_t element_size = nested_column.getOffsets()[row]- element_start_row;

            ColumnsWithTypeAndName new_arguments;
            ColumnPtr sub_map_column;
            DataTypePtr data_type;

            if (keys_string_column)
            {
                sub_map_column = keys_string_column->cut(element_start_row, element_size);
                data_type = std::make_shared<DataTypeString>();
            }
            else
            {
                sub_map_column = keys_fixed_string_column->cut(element_start_row, element_size);
                data_type =std::make_shared<DataTypeFixedString>(checkAndGetColumn<ColumnFixedString>(sub_map_column.get())->getN());
            }

            size_t col_key_size = sub_map_column->size();
            auto column = is_const? ColumnConst::create(std::move(sub_map_column), std::move(col_key_size)) : std::move(sub_map_column);

            new_arguments = {
                    {
                        column,
                        data_type,
                        ""
                        },
                    arguments[1]
                    };

            auto res = func_like.executeImpl(new_arguments, result_type, input_rows_count);
            const auto & container = checkAndGetColumn<ColumnUInt8>(res.get())->getData();

            for (size_t row_num = 0; row_num < element_size; ++row_num)
            {
                if (container[row_num] == 1)
                {
                    auto key_ref = keys_string_column ?
                                   keys_string_column->getDataAt(element_start_row + row_num) :
                                   keys_fixed_string_column->getDataAt(element_start_row + row_num);
                    auto value_ref = values_column.getDataAt(element_start_row + row_num);

                    keys_data->insertData(key_ref.data, key_ref.size);
                    values_data->insertData(value_ref.data, value_ref.size);
                    current_offset += 1;
                }
            }

            offsets->insert(current_offset);
        }

        auto result_nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

        return ColumnMap::create(result_nested_column);
    }
};

class FunctionMapUpdate : public IFunction
{
public:
    static constexpr auto name = "map_update";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapUpdate>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeMap * left = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        const DataTypeMap * right = checkAndGetDataType<DataTypeMap>(arguments[1].type.get());

        if (!left || !right)
            throw Exception{"The two arguments for function " + getName() + " must be both map type",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        if (!left->getKeyType()->equals(*right->getKeyType()) || !left->getValueType()->equals(*right->getValueType()))
            throw Exception{"The Key And Value type of Map for function " + getName() + " must be the same",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeMap>(left->getKeyType(), left->getValueType());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnMap * col_map_left = typeid_cast<const ColumnMap *>(arguments[0].column.get());
        const auto * col_const_map_left = checkAndGetColumnConst<ColumnMap>(arguments[0].column.get());
        bool col_const_map_left_flag = false;
        if (col_const_map_left)
        {
            col_const_map_left_flag = true;
            col_map_left = typeid_cast<const ColumnMap *>(&col_const_map_left->getDataColumn());
        }
        if (!col_map_left)
            return nullptr;

        const ColumnMap * col_map_right = typeid_cast<const ColumnMap *>(arguments[1].column.get());
        const auto * col_const_map_right = checkAndGetColumnConst<ColumnMap>(arguments[1].column.get());
        bool col_const_map_right_flag = false;
        if (col_const_map_right)
        {
            col_const_map_right_flag = true;
            col_map_right = typeid_cast<const ColumnMap *>(&col_const_map_right->getDataColumn());
        }
        if (!col_map_right)
            return nullptr;

        const auto & nested_column_left = col_map_left->getNestedColumn();
        const auto & keys_data_left = col_map_left->getNestedData().getColumn(0);
        const auto & values_data_left = col_map_left->getNestedData().getColumn(1);
        const auto & offsets_left = nested_column_left.getOffsets();

        const auto & nested_column_right = col_map_right->getNestedColumn();
        const auto & keys_data_right = col_map_right->getNestedData().getColumn(0);
        const auto & values_data_right = col_map_right->getNestedData().getColumn(1);
        const auto & offsets_right = nested_column_right.getOffsets();

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();
        MutableColumnPtr keys_data = key_type->createColumn();
        MutableColumnPtr values_data = value_type->createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        IColumn::Offset current_offset = 0;
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            size_t left_it_begin = col_const_map_left_flag ? 0 : offsets_left[row_idx - 1];
            size_t left_it_end = col_const_map_left_flag ? offsets_left.size() : offsets_left[row_idx];
            size_t right_it_begin = col_const_map_right_flag ? 0 : offsets_right[row_idx - 1];
            size_t right_it_end = col_const_map_right_flag ? offsets_right.size() : offsets_right[row_idx];

            for (size_t i = left_it_begin; i < left_it_end; ++i)
            {
                bool matched = false;
                auto key = keys_data_left.getDataAt(i);
                for (size_t j = right_it_begin; j < right_it_end; ++j)
                {
                    if (keys_data_right.getDataAt(j).toString() == key.toString())
                    {
                        matched = true;
                        break;
                    }
                }
                if (!matched)
                {
                    keys_data->insertFrom(keys_data_left, i);
                    values_data->insertFrom(values_data_left, i);
                    ++current_offset;
                }
            }

            for (size_t j = right_it_begin; j < right_it_end; ++j)
            {
                keys_data->insertFrom(keys_data_right, j);
                values_data->insertFrom(values_data_right, j);
                ++current_offset;
            }

            offsets->insert(current_offset);
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys_data), std::move(values_data)}),
            std::move(offsets));

        return ColumnMap::create(nested_column);
    }
};

}

REGISTER_FUNCTION(Map)
{
    factory.registerFunction<FunctionMap>();
    factory.registerFunction<FunctionMapContains>();
    factory.registerFunction<FunctionMapKeys>();
    factory.registerFunction<FunctionMapValues>();
    factory.registerFunction<FunctionMapContainsKeyLike>();
    factory.registerFunction<FunctionExtractKeyLike>();
    factory.registerFunction<FunctionMapUpdate>();
    factory.registerFunction<FunctionMapFromArrays>();
}

}
