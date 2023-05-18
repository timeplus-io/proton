#pragma once

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/UserDefined/ExternalUserDefinedFunctionsLoader.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>


namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_DATA;
}

class UserDefinedFunctionBase : public IFunction
{
public:
    explicit UserDefinedFunctionBase(
        ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_,
        ContextPtr context_,
        const String logger_name)
        : executable_function(std::move(executable_function_)), context(context_), log(&Poco::Logger::get(logger_name))
    {
    }

    virtual ~UserDefinedFunctionBase() = default;

    String getName() const override { return executable_function->getLoadableName(); }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return executable_function->getConfiguration()->arguments.size(); }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        const auto & configuration = executable_function->getConfiguration();
        return configuration->result_type;
    }

    virtual ColumnPtr
    userDefinedExecuteImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
        = 0;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// No need to execute UDF if it is an empty block.
        if (input_rows_count == 0)
        {
            auto result_column = result_type->createColumn();
            return result_column;
        }

        const auto & configuration = executable_function->getConfiguration();
        size_t argument_size = arguments.size();
        auto arguments_copy = arguments;

        for (size_t i = 0; i < argument_size; ++i)
        {
            auto & column_with_type = arguments_copy[i];
            column_with_type.column = column_with_type.column->convertToFullColumnIfConst();
            column_with_type.name = configuration->arguments[i].name;

            const auto & argument_type = configuration->arguments[i].type;
            if (areTypesEqual(arguments_copy[i].type, argument_type))
                continue;

            ColumnWithTypeAndName column_to_cast = column_with_type;
            column_with_type.column = castColumnAccurate(column_to_cast, argument_type);
            column_with_type.type = argument_type;
        }

        ColumnPtr result_col_ptr = userDefinedExecuteImpl(arguments_copy, result_type, input_rows_count);

        size_t result_column_size = result_col_ptr->size();
        if (result_column_size != input_rows_count)
            throw Exception(
                ErrorCodes::INVALID_DATA,
                "Function {}: wrong result, expected {} row(s), actual {}",
                quoteString(getName()),
                input_rows_count,
                result_column_size);

        return result_col_ptr;
    }

protected:
    ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function;
    ContextPtr context;
    Poco::Logger * log;
};
}
