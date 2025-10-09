#pragma once

#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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
        cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc_, ContextPtr context_, const String logger_name)
        : udf_desc(std::move(udf_desc_)), context(std::move(context_)), logger(getLogger(logger_name))
    {
    }

    virtual ~UserDefinedFunctionBase() override = default;
    String getName() const override { return udf_desc->name; }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return udf_desc->arguments.size(); }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return DataTypeFactory::instance().get(udf_desc->return_type); }

    virtual ColumnPtr
    userDefinedExecuteImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
        = 0;

    ColumnPtr
    executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto result_column = result_type->createColumn();

        for (size_t i = 0; i < input_rows_count; ++i)
            result_column->insertDefault();

        return result_column;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// No need to execute UDF if it is an empty block.
        if (input_rows_count == 0)
        {
            auto result_column = result_type->createColumn();
            return result_column;
        }

        size_t argument_size = arguments.size();
        auto arguments_copy = arguments;

        for (size_t i = 0; i < argument_size; ++i)
        {
            auto & column_with_type = arguments_copy[i];
            column_with_type.column = column_with_type.column->convertToFullColumnIfConst();
            column_with_type.name = udf_desc->arguments[i].name;

            const auto & argument_type = DataTypeFactory::instance().get(udf_desc->arguments[i].type);
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
    cluster::protocol::UserDefinedFunctionDescriptorPtr udf_desc;
    ContextPtr context;

public:
    LoggerPtr logger;
};
}
