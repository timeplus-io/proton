#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/typeIndexToTypeName.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/map.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>

#include <limits>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int NOT_IMPLEMENTED;
}
namespace
{
pcg64 rng(randomSeed());
ColumnPtr generateUniqueIdColumn(size_t rows_num, size_t num_of_uniques)
{
    auto unique_id_col = ColumnVector<UInt64>::create(rows_num);
    auto & unique_ids = unique_id_col->getData();
    for (auto idx = 0; idx < rows_num; ++idx)
        unique_ids[idx] = static_cast<UInt64>(rng() % num_of_uniques);
    return unique_id_col;
}

using DataGenerator = std::function<ColumnPtr(ColumnPtr &&, const DataTypePtr &, size_t rows_num)>;
template <TypeIndex TYPE_INDEX, typename TYPE>
DataGenerator createDataGenerator()
{
    return [](ColumnPtr && unique_id_col, const DataTypePtr & result_type, size_t rows_num) -> ColumnPtr {
        auto res = result_type->createColumn();
        res->reserve(rows_num);
        const auto & unique_ids = assert_cast<const DB::ColumnVector<UInt64> &>(*unique_id_col).getData();

        if constexpr (
            TYPE_INDEX == TypeIndex::Bool || TYPE_INDEX == TypeIndex::Int8 || TYPE_INDEX == TypeIndex::Int16
            || TYPE_INDEX == TypeIndex::Int32 || TYPE_INDEX == TypeIndex::Int64 || TYPE_INDEX == TypeIndex::Int128
            || TYPE_INDEX == TypeIndex::Int256 || TYPE_INDEX == TypeIndex::UInt8 || TYPE_INDEX == TypeIndex::UInt16
            || TYPE_INDEX == TypeIndex::UInt32 || TYPE_INDEX == TypeIndex::UInt64 || TYPE_INDEX == TypeIndex::UInt128
            || TYPE_INDEX == TypeIndex::UInt256 || TYPE_INDEX == TypeIndex::Float32 || TYPE_INDEX == TypeIndex::Float64
            || TYPE_INDEX == TypeIndex::Date || TYPE_INDEX == TypeIndex::Date32 || TYPE_INDEX == TypeIndex::DateTime
            || TYPE_INDEX == TypeIndex::UUID)
        {
            auto & data = assert_cast<DB::ColumnVector<TYPE> &>(*res).getData();
            data.resize(rows_num);
            for (auto idx = 0; idx < rows_num; idx++)
                data[idx] = static_cast<TYPE>(unique_ids[idx]);
        }
        else if constexpr (
            TYPE_INDEX == TypeIndex::Decimal32 || TYPE_INDEX == TypeIndex::Decimal64 || TYPE_INDEX == TypeIndex::Decimal128
            || TYPE_INDEX == TypeIndex::DateTime64)
        {
            auto & data = assert_cast<DB::ColumnDecimal<TYPE> &>(*res).getData();
            data.resize(rows_num);
            for (auto idx = 0; idx < rows_num; idx++)
                data[idx] = static_cast<typename TYPE::NativeType>(unique_ids[idx]);
        }
        else if constexpr (TYPE_INDEX == TypeIndex::String)
        {
            for (auto idx = 0; idx < rows_num; idx++)
                res->insertData(reinterpret_cast<const char *>(&unique_ids[idx]), sizeof(UInt64));
        }
        else if constexpr (TYPE_INDEX == TypeIndex::FixedString)
        {
            size_t length = std::min(sizeof(UInt64), assert_cast<ColumnFixedString &>(*res).getN());
            for (auto idx = 0; idx < rows_num; idx++)
                res->insertData(reinterpret_cast<const char *>(&unique_ids[idx]), length);
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "No support type '{}'", TypeName<TYPE>);

        return res;
    };
}

DataGenerator createDefaultDataGenerator(const DataTypePtr & result_type)
{
    switch (WhichDataType(result_type).idx)
    {
#define CREATE_DATA_GENERATOR(TYPE_INDEX, TYPE) \
    case TYPE_INDEX: \
        return createDataGenerator<TYPE_INDEX, TYPE>();

        CREATE_DATA_GENERATOR(TypeIndex::Bool, UInt8)
        CREATE_DATA_GENERATOR(TypeIndex::UInt8, UInt8)
        CREATE_DATA_GENERATOR(TypeIndex::UInt16, UInt16)
        CREATE_DATA_GENERATOR(TypeIndex::UInt32, UInt32)
        CREATE_DATA_GENERATOR(TypeIndex::UInt64, UInt64)
        CREATE_DATA_GENERATOR(TypeIndex::UInt128, UInt128)
        CREATE_DATA_GENERATOR(TypeIndex::UInt256, UInt256)
        CREATE_DATA_GENERATOR(TypeIndex::Int8, Int8)
        CREATE_DATA_GENERATOR(TypeIndex::Int16, Int16)
        CREATE_DATA_GENERATOR(TypeIndex::Int32, Int32)
        CREATE_DATA_GENERATOR(TypeIndex::Int64, Int64)
        CREATE_DATA_GENERATOR(TypeIndex::Int128, Int128)
        CREATE_DATA_GENERATOR(TypeIndex::Int256, Int256)
        CREATE_DATA_GENERATOR(TypeIndex::Float32, Float32)
        CREATE_DATA_GENERATOR(TypeIndex::Float64, Float64)
        CREATE_DATA_GENERATOR(TypeIndex::Date, DataTypeDate::FieldType)
        CREATE_DATA_GENERATOR(TypeIndex::Date32, DataTypeDate32::FieldType)
        CREATE_DATA_GENERATOR(TypeIndex::DateTime, DataTypeDateTime::FieldType)
        CREATE_DATA_GENERATOR(TypeIndex::DateTime64, DataTypeDateTime64::FieldType)
        CREATE_DATA_GENERATOR(TypeIndex::String, StringRef)
        CREATE_DATA_GENERATOR(TypeIndex::FixedString, StringRef)
        CREATE_DATA_GENERATOR(TypeIndex::Decimal32, Decimal32)
        CREATE_DATA_GENERATOR(TypeIndex::Decimal64, Decimal64)
        CREATE_DATA_GENERATOR(TypeIndex::Decimal128, Decimal128)
        CREATE_DATA_GENERATOR(TypeIndex::Decimal256, Decimal256)
        CREATE_DATA_GENERATOR(TypeIndex::UUID, UUID)
#undef CREATE_DATA_GENERATOR
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The unique_random doesn't support argument type '{}'", result_type->getName());
    }
    UNREACHABLE();
}

class FunctionUniqueRandom : public IFunction
{
    // usage:unique_random('int8', [lambda expression] ,[num_of_uniques]), this function will return a Unique and Random 'int8' type data
    // lambda expression and num_of_uniques can switch places
private:
    UInt64 num_of_uniques = std::numeric_limits<UInt64>::max();
    int lambda_function_pos;
    DataGenerator default_data_generator;

public:
    static constexpr auto name = "unique_random";

    FunctionUniqueRandom(UInt64 num_of_uniques_, int lambda_function_pos_, DataGenerator default_data_generator_)
        : num_of_uniques(num_of_uniques_), lambda_function_pos(lambda_function_pos_), default_data_generator(default_data_generator_)
    {
    }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (unlikely(input_rows_count == 0))
            return result_type->createColumn();

        auto unique_id_col = generateUniqueIdColumn(input_rows_count, num_of_uniques);

        /// Use default generator if exists
        if (default_data_generator)
            return default_data_generator(std::move(unique_id_col), result_type, input_rows_count);

        /// Else, use lambda function to generate data
        assert(lambda_function_pos > 0);
        auto lambda_function_col = IColumn::mutate(arguments[lambda_function_pos].column);
        auto & lambda_function = assert_cast<ColumnFunction &>(*lambda_function_col);
        lambda_function.appendArguments(
            {ColumnWithTypeAndName(std::move(unique_id_col), std::make_shared<DataTypeUInt64>(), "__unique_id")});
        auto lambda_result = lambda_function.reduce();
        return std::move(lambda_result.column);
    }
};

class FunctionUniqueRandomOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "unique_random";

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<FunctionUniqueRandomOverloadResolver>(); }

    void getLambdaArgumentTypesImpl(DataTypes & arguments) const override
    {
        if (arguments.size() < 1 || arguments.size() > 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1 , 2 or 3",
                getName(),
                arguments.size());

        DataTypes nested_types{std::make_shared<DataTypeUInt64>()};
        if (WhichDataType(arguments[1]).isFunction())
            arguments[1] = std::make_shared<DataTypeFunction>(std::move(nested_types));
        else if (arguments.size() == 3 && WhichDataType(arguments[2]).isFunction())
            arguments[2] = std::make_shared<DataTypeFunction>(std::move(nested_types));
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 1 || arguments.size() > 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1 , 2 or 3",
                getName(),
                arguments.size());

        const ColumnConst * const_type_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!const_type_column)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} should be constant type string", getName());

        auto return_type = DataTypeFactory::instance().get(const_type_column->getValue<String>());

        if (arguments.size() >= 2)
        {
            size_t lambda_pos = 1;
            auto * lambda_func_type = checkAndGetDataType<DataTypeFunction>(arguments[lambda_pos].type.get());
            if (!lambda_func_type && arguments.size() == 3)
                lambda_func_type = checkAndGetDataType<DataTypeFunction>(arguments[++lambda_pos].type.get());
            if (!lambda_func_type)
                return return_type;

            auto lambda_func_return_type = lambda_func_type->getReturnType();
            if (!lambda_func_return_type->equals(*return_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "{} argument of function {} should be lambda function with result type {}, but actual type {}",
                    lambda_pos == 1 ? "Second" : "Third",
                    getName(),
                    return_type->getName(),
                    lambda_func_return_type->getName());
        }

        return return_type;
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        UInt64 num_of_uniques = std::numeric_limits<UInt64>::max();
        int lambda_function_pos = -1;
        if (arguments.size() > 1)
        {
            if (WhichDataType(arguments[1].type).isFunction())
            {
                lambda_function_pos = 1;
                if (arguments.size() == 3)
                    num_of_uniques = arguments[2].column->getUInt(0);
            }
            else
            {
                num_of_uniques = arguments[1].column->getUInt(0);
                if (arguments.size() == 3)
                    lambda_function_pos = 2;
            }
        }

        if (num_of_uniques == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument max unique number must be greater than 0");

        return std::make_shared<FunctionToFunctionBaseAdaptor>(
            std::make_shared<FunctionUniqueRandom>(
                num_of_uniques, lambda_function_pos, lambda_function_pos == -1 ? createDefaultDataGenerator(result_type) : nullptr),
            collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
            result_type);
    }
};
}

REGISTER_FUNCTION(UniqueRandom)
{
    factory.registerFunction<FunctionUniqueRandomOverloadResolver>({}, FunctionFactory::CaseInsensitive);
}

}
