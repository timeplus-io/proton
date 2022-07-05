#include "UserDefinedExecutableFunctionFactory.h"

#include <filesystem>

#include <Common/filesystemHelpers.h>

#include <IO/WriteHelpers.h>

#include <Processors/Sources/ShellCommandSource.h>
#include <Formats/formatBlock.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>

#include <Processors/Formats/IOutputFormat.h>

/// proton: starts
#include <Common/sendRequest.h>
#include <IO/ReadBufferFromString.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    /// proton: starts
    extern const int REMOTE_CALL_FAILED;
    /// proton: ends
}

class UserDefinedFunction final : public IFunction
{
public:

    explicit UserDefinedFunction(
        ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_,
        ContextPtr context_)
        : executable_function(std::move(executable_function_))
        , context(context_)
        , log(&Poco::Logger::get("UserDefinedFunction"))
    {
    }

    ~UserDefinedFunction() override
    {
        /// proton: starts
        /// query ends, stop all udf processes in process pool
        executable_function->getCoordinator()->stopProcessPool();
        /// proton: ends
    }

    String getName() const override { return executable_function->getConfiguration().name; }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return executable_function->getConfiguration().arguments.size(); }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        const auto & configuration = executable_function->getConfiguration();
        return configuration.result_type;
    }

    /// proton: starts.
    ColumnPtr executeLocal(const Block & block, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        auto coordinator = executable_function->getCoordinator();
        const auto & configuration = executable_function->getConfiguration();

        ColumnWithTypeAndName result(result_type, "result");
        Block result_header({result});

        auto ctx = coordinator->getUDFContext(configuration.command, configuration.command_arguments, block.cloneEmpty(), result_header, context);

        coordinator->sendData(ctx, block);

        auto result_column = coordinator->pull(ctx, result_type, input_rows_count);
        LOG_TRACE(log, "pull {} rows", result_column->size());
        coordinator->release(std::move(ctx));

        return result_column;
    }

    ColumnPtr executeRemote(const Block & block, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        auto coordinator = executable_function->getCoordinator();
        const auto & configuration = executable_function->getConfiguration();

        /// convert block to 'JSONColumns' string
        String out;
        WriteBufferFromString out_buf{out};
        auto convert = [&](const Block & block_to_send) {
            auto out_format = context->getOutputFormat("JSONColumns", out_buf, block_to_send.cloneEmpty());
            out_format->setAutoFlush();
            out_format->write(block_to_send);
            out_format->finalize();
            out_buf.finalize();
        };
        convert(block);

        /// send data to remote endpoint
        auto [resp, http_status] = sendRequest(
            configuration.url,
            Poco::Net::HTTPRequest::HTTP_POST,
            context->getCurrentQueryId(),
            context->getUserName(),
            context->getPasswordByUserName(context->getUserName()),
            out,
            {{configuration.auth_context.key_name, configuration.auth_context.key_value}},
            &Poco::Logger::get("UserDefinedFunction"));

        if (http_status != Poco::Net::HTTPResponse::HTTP_OK)
            throw Exception(
                ErrorCodes::REMOTE_CALL_FAILED,
                "Call remote uri {} failed, message: {}, http_status: {}",
                configuration.url.toString(),
                resp,
                http_status);

        /// Convert resp to block
        ColumnWithTypeAndName result(result_type, "result");
        Block result_header({result});
        ReadBufferFromString read_buffer(resp);
        auto input_format = context->getInputFormat("JSONColumns", read_buffer, result_header, input_rows_count);
        return coordinator->pull(input_format, result_type, input_rows_count);
    }
    /// proton: ends

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// proton: starts
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
            column_with_type.name = configuration.arguments[i].name;

            const auto & argument_type = configuration.arguments[i].type;
            if (areTypesEqual(arguments_copy[i].type, argument_type))
                continue;

            ColumnWithTypeAndName column_to_cast = {column_with_type.column, column_with_type.type, column_with_type.name};
            column_with_type.column = castColumnAccurate(column_to_cast, argument_type);
            column_with_type.type = argument_type;

            column_with_type = std::move(column_to_cast);
        }

        Block arguments_block(arguments_copy);
        ColumnPtr result_col_ptr;
        switch (configuration.type)
        {
            case UserDefinedExecutableFunctionConfiguration::FuncType::EXECUTABLE:
                result_col_ptr = executeLocal(arguments_block, result_type, input_rows_count);
                break;
            case UserDefinedExecutableFunctionConfiguration::FuncType::REMOTE:
                result_col_ptr = executeRemote(arguments_block, result_type, input_rows_count);
                break;
            default:
                throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "UDF type {} does not support yet.", configuration.type);
        }

        size_t result_column_size = result_col_ptr->size();
        if (result_column_size != input_rows_count)
            throw Exception(
                ErrorCodes::UNSUPPORTED_METHOD,
                "Function {}: wrong result, expected {} row(s), actual {}",
                quoteString(getName()),
                input_rows_count,
                result_column_size);

        return result_col_ptr;
        /// proton: ends
    }

private:

    ExternalUserDefinedExecutableFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function;
    ContextPtr context;
    Poco::Logger * log;
};

UserDefinedExecutableFunctionFactory & UserDefinedExecutableFunctionFactory::instance()
{
    static UserDefinedExecutableFunctionFactory result;
    return result;
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::get(const String & function_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(loader.load(function_name));
    auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context));
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
}

FunctionOverloadResolverPtr UserDefinedExecutableFunctionFactory::tryGet(const String & function_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context));
        return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
    }

    return nullptr;
}

bool UserDefinedExecutableFunctionFactory::has(const String & function_name, ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto load_result = loader.getLoadResult(function_name);

    bool result = load_result.object != nullptr;
    return result;
}

std::vector<String> UserDefinedExecutableFunctionFactory::getRegisteredNames(ContextPtr context)
{
    const auto & loader = context->getExternalUserDefinedExecutableFunctionsLoader();
    auto loaded_objects = loader.getLoadedObjects();

    std::vector<std::string> registered_names;
    registered_names.reserve(loaded_objects.size());

    for (auto & loaded_object : loaded_objects)
        registered_names.emplace_back(loaded_object->getLoadableName());

    return registered_names;
}

}
