#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/typeIndexToTypeName.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <DataTypes/getLeastSupertype.h>
/// proton: starts.
#include <string>
#include <limits.h>
#include <DataTypes/DataTypeFactory.h>
#include <base/map.h>
#include <Common/Exception.h>
/// proton: ends.

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ARGUMENT_OUT_OF_BOUND;
}
namespace
{

enum class TypeCategory : UInt8
{
    NUMERIC,
    STRING_REF,
    SEIRIALIZED_STRING_REF,
    DECIMAL,
    DATETIME64,
    TUPLE,
    OTHERS,
};

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
            case TypeIndex::Int8: \
                M(Int8, ##__VA_ARGS__); \
                break; \
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
            default: { \
                throw Exception( \
                    ErrorCodes::BAD_ARGUMENTS, \
                    "The unique_set doesn't support argument type '{}'", \
                    typeIndexToTypeName(WhichDataType(TYPE).idx)); \
            } \
        } \
    } while (0)

#define GENERATE_RANDOM_UNIQUE_DATA(TYPE, ...) generateRandomUniqueData<TYPE>(__VA_ARGS__)
#define CREATE_FUNCTION_UNIQUE_RANDOM(TYPE, ...) createFunctionUniqueRandom<TYPE>(__VA_ARGS__)


template <typename TYPE>
class FunctionUniqueRandom : public IFunction
{
    // usage:unique_random('int8', [lambda expression] ,[num_of_uniques]), this function will return a Unique and Random 'int8' type data
    // lambda expression and num_of_uniques can switch places
private:
    // unique_random can only generate blocks less than UInt64 size ,
    // because {paramter size_t(aka unsigned long int) input_rows_count} is less than UInt64(aka unsigned long int)
    mutable std::atomic<UInt64> next_generate_key = 0;
    UInt64 num_of_uniques = std::numeric_limits<UInt64>::max();
    int lambda_function_pos;
    ColumnsWithTypeAndName arguments;

public:
    static constexpr auto name = "unique_random";

    FunctionUniqueRandom(ColumnsWithTypeAndName arguments_, UInt64 num_of_uniques_, int lambda_function_pos_)
        : num_of_uniques(num_of_uniques_), lambda_function_pos(lambda_function_pos_), arguments(arguments_)
    {
    }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUniqueRandom>(); }

    std::string getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isStateful() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    void getLambdaArgumentTypes(DataTypes & arguments) const override { return; }

    void generateRowNumberColumn(MutableColumnPtr & unique_id_col, size_t input_rows_count) const
    {
        auto & column = assert_cast<DB::ColumnVector<UInt64> &>(*unique_id_col);
        for (auto idx = 0; idx < input_rows_count; idx++)
        {
            column.insertValue(next_generate_key.load());
            next_generate_key++;
            if (next_generate_key == num_of_uniques)
                next_generate_key = 0;
        }
    }

    void convertTypeToReturnType(
        const MutableColumnPtr & unique_id_col, MutableColumnPtr & res, const DataTypePtr & data_type, size_t input_rows_count) const
    {
        auto & unique_id_col_column = assert_cast<DB::ColumnVector<UInt64> &>(*unique_id_col);

        if constexpr (is_integer<TYPE>)
        {
            auto & column = assert_cast<DB::ColumnVector<TYPE> &>(*res);
            for (auto idx = 0; idx < input_rows_count; idx++)
            {
                column.insertValue(TYPE(unique_id_col_column.getElement(idx)));
            }
        }
        else if constexpr (is_decimal<TYPE>)
        {
            auto & column = assert_cast<DB::ColumnDecimal<TYPE> &>(*res);
            for (auto idx = 0; idx < input_rows_count; idx++)
            {
                TYPE tmp_decimal(typename TYPE::NativeType(unique_id_col_column.getElement(idx)));
                column.insertValue(tmp_decimal);
            }
        }
        else if constexpr (std::is_same<TYPE, StringRef>())
        {
            auto & column = assert_cast<DB::ColumnString &>(*res);
            for (auto idx = 0; idx < input_rows_count; idx++)
            {
                String tmp_string = std::to_string(unique_id_col_column.getElement(idx));
                column.insertData(tmp_string.c_str(), tmp_string.size());
            }
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & data_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return data_type->createColumn();
        auto unique_id_col = DB::DataTypeFactory::instance().get("uint64")->createColumn();

        unique_id_col->reserve(input_rows_count);
        String return_type = data_type->getName();
        generateRowNumberColumn(unique_id_col, input_rows_count);

        const ColumnFunction * lambda_function = nullptr;
        if (lambda_function_pos != -1)
            lambda_function = typeid_cast<const ColumnFunction *>(arguments[lambda_function_pos].column.get());

        if (lambda_function == nullptr)
        {
            auto res = data_type->createColumn();
            convertTypeToReturnType(unique_id_col, res, data_type, input_rows_count);
            return res;
        }
        else
        {
            ColumnsWithTypeAndName capture;
            capture.emplace_back(ColumnWithTypeAndName(std::move(unique_id_col), DB::DataTypeFactory::instance().get("uint64"), "uint64"));
            auto replicated_column_function_ptr = lambda_function->cloneResized(lambda_function->size());
            auto * replicated_column_function = typeid_cast<ColumnFunction *>(replicated_column_function_ptr.get());
            replicated_column_function->appendArguments(capture);
            auto lambda_result = replicated_column_function->reduce();

            return lambda_result.column;
        }
    }
};

};

class FunctionUniqueRandomOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "unique_random";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<FunctionUniqueRandomOverloadResolver>(); }

    void getLambdaArgumentTypesImpl(DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs at least one argument, passed {}",
                getName(),
                arguments.size());

        if (arguments.size() == 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} needs at least one argument with data", getName());

        if (arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} needs one argument with data", getName());

        DataTypes nested_types(1);

        nested_types[0] = recursiveRemoveLowCardinality(DataTypeFactory::instance().get("uint64"));
        auto family_name = arguments[1]->getFamilyName();
        if (std::strcmp(family_name, "function") == 0)
            arguments[1] = std::make_shared<DataTypeFunction>(nested_types);
        else
            arguments[2] = std::make_shared<DataTypeFunction>(nested_types);
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
            auto * lambda_func_type = checkAndGetDataType<DataTypeFunction>(arguments[1].type.get());
            if (!lambda_func_type && arguments.size() == 3)
                lambda_func_type = checkAndGetDataType<DataTypeFunction>(arguments[2].type.get());
            if (!lambda_func_type)
                return return_type;

            auto lambda_func_return_type = lambda_func_type->getReturnType();
            if (!lambda_func_return_type->equals(*return_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument of function {} should be lambda function with result type {}, but actual type {}",
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
            const ColumnConst * column_num_of_uniques = nullptr;
            if (strcmp(arguments[1].type->getFamilyName(), "function") != 0)
            {
                column_num_of_uniques = assert_cast<const ColumnConst *>(arguments[1].column.get());
                num_of_uniques = column_num_of_uniques->getUInt(0);
                if (arguments.size() == 3)
                    lambda_function_pos = 2;
            }
            else
            {
                lambda_function_pos = 1;
                if (arguments.size() == 3)
                {
                    column_num_of_uniques = assert_cast<const ColumnConst *>(arguments[2].column.get());
                    num_of_uniques = column_num_of_uniques->getUInt(0);
                }
            }
        }
        DISPATCH(result_type, CREATE_FUNCTION_UNIQUE_RANDOM, arguments, result_type, num_of_uniques, lambda_function_pos);
        return function_ptr;
    }

private:
    mutable FunctionBasePtr function_ptr;
    template <typename TYPE>
    void createFunctionUniqueRandom(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, UInt64 num_of_uniques, int lambda_function_pos) const
    {
        function_ptr = std::make_shared<FunctionToFunctionBaseAdaptor>(
            std::make_shared<FunctionUniqueRandom<TYPE>>(arguments, num_of_uniques, lambda_function_pos),
            collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
            result_type);
    }
};
REGISTER_FUNCTION(UniqueRandom)
{
    factory.registerFunction<FunctionUniqueRandomOverloadResolver>({}, FunctionFactory::CaseInsensitive);
}

}
