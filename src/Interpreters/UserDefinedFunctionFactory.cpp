#include "UserDefinedFunctionFactory.h"

/// proton: starts
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionJavaScriptAdapter.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Formats/formatBlock.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalUserDefinedFunctionsLoader.h>
#include <Interpreters/castColumn.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/ShellCommandSource.h>
#include <V8/Utils.h>
#include <Common/sendRequest.h>

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
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int INVALID_DATA;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    /// proton: ends
}

class UserDefinedFunction final : public IFunction
{
public:

    explicit UserDefinedFunction(
        ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_,
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
            {{configuration.auth_context.key_name, configuration.auth_context.key_value}, {"", context->getCurrentQueryId()}},
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
        try
        {
            return coordinator->pull(input_format, result_type, input_rows_count);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED)
                e.addMessage("The result format is invalid, which should be 'JSONColumns'");
            throw;
        }
    }

    ColumnPtr executeJavaScript(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
    {
        auto coordinator = executable_function->getCoordinator();
        const auto & config = executable_function->getConfiguration();
        auto js_ctx = coordinator->getJavaScriptContext(config, context);
        SCOPE_EXIT(coordinator->release(std::move(js_ctx)));

        assert(arguments[0].column);
        size_t row_num = arguments[0].column->size();
        MutableColumns columns;
        for (auto & arg : arguments)
            columns.emplace_back(IColumn::mutate(arg.column));

        auto result_column = result_type->createColumn();
        result_column->reserve(row_num);

        v8::Isolate * isolate = js_ctx->isolate.get();
        assert(isolate);
        auto execute = [&](v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
            /// First, get local function of UDF
            v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate, js_ctx->func);

            /// Second, convert the input column into the corresponding object used by UDF
            auto argv = V8::prepareArguments(isolate, config.arguments, columns);

            /// Third, execute the UDF and get result
            v8::Local<v8::Value> res;
            if (!local_func->CallAsFunction(ctx, ctx->Global(), static_cast<int>(config.arguments.size()), argv.data()).ToLocal(&res))
                V8::throwException(isolate, try_catch, ErrorCodes::REMOTE_CALL_FAILED, "call JavaScript UDF '{}' failed", config.name);

            /// Forth, insert the result to result_column
            V8::insertResult(isolate, *result_column, result_type, true, res);
        };

        V8::run(isolate, js_ctx->context, execute);
        return result_column;
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

            ColumnWithTypeAndName column_to_cast = column_with_type;
            column_with_type.column = castColumnAccurate(column_to_cast, argument_type);
            column_with_type.type = argument_type;
        }

        ColumnPtr result_col_ptr;
        switch (configuration.type)
        {
            case UserDefinedFunctionConfiguration::FuncType::EXECUTABLE: {
                Block arguments_block(arguments_copy);
                result_col_ptr = executeLocal(arguments_block, result_type, input_rows_count);
                break;
            }
            case UserDefinedFunctionConfiguration::FuncType::REMOTE: {
                Block arguments_block(arguments_copy);
                result_col_ptr = executeRemote(arguments_block, result_type, input_rows_count);
                break;
            }
            case UserDefinedFunctionConfiguration::FuncType::JAVASCRIPT: {
                result_col_ptr = executeJavaScript(std::move(arguments_copy), result_type, input_rows_count);
                break;
            }
            default:
                throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "UDF type {} does not support yet.", configuration.type);
        }

        size_t result_column_size = result_col_ptr->size();
        if (result_column_size != input_rows_count)
            throw Exception(
                ErrorCodes::INVALID_DATA,
                "Function {}: wrong result, expected {} row(s), actual {}",
                quoteString(getName()),
                input_rows_count,
                result_column_size);

        return result_col_ptr;
        /// proton: ends
    }

private:
    ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function;
    ContextPtr context;
    Poco::Logger * log;
};

UserDefinedFunctionFactory & UserDefinedFunctionFactory::instance()
{
    static UserDefinedFunctionFactory result;
    return result;
}

FunctionOverloadResolverPtr UserDefinedFunctionFactory::get(const String & function_name, ContextPtr context)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(loader.load(function_name));
    auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context));
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
}

/// proton: starts
AggregateFunctionPtr UserDefinedFunctionFactory::getAggregateFunction(
    const String & function_name, const DataTypes & types, const Array & parameters, AggregateFunctionProperties & /*properties*/)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(nullptr);
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        const auto & config = executable_function->getConfiguration();

        if (!config.is_aggregation)
            return nullptr;

        /// check arguments
        if (types.size() != config.arguments.size())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "expect {} arguments but get {} for UDF {}",
                config.arguments.size(),
                types.size(),
                config.name);

        for (int i = 0; i < config.arguments.size(); i++)
        {
            if (types[i]->getTypeId() != config.arguments[i].type->getTypeId())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "argument {} of UDF {}: expect type '{}' but get '{}'",
                    i,
                    config.name,
                    config.arguments[i].type->getName(),
                    types[i]->getName());
        }

        ContextPtr query_context;
        if (CurrentThread::isInitialized())
            query_context = CurrentThread::get().getQueryContext();

        if (!query_context || !query_context->getSettingsRef().javascript_max_memory_bytes)
        {
            LOG_ERROR(&Poco::Logger::get("UserDefinedExecutableFunctionFactory"), "query_context is invalid");
            return nullptr;
        }

        return std::make_shared<AggregateFunctionJavaScriptAdapter>(
            config, types, parameters, query_context->getSettingsRef().javascript_max_memory_bytes);
    }

    return nullptr;
}

bool UserDefinedFunctionFactory::isAggregateFunctionName(const String & function_name)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(nullptr);
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        const auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        const auto & config = executable_function->getConfiguration();

        return config.is_aggregation;
    }
    return false;
}
/// proton: ends

FunctionOverloadResolverPtr UserDefinedFunctionFactory::tryGet(const String & function_name, ContextPtr context)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    auto load_result = loader.getLoadResult(function_name);

    if (load_result.object)
    {
        auto executable_function = std::static_pointer_cast<const UserDefinedExecutableFunction>(load_result.object);
        auto function = std::make_shared<UserDefinedFunction>(std::move(executable_function), std::move(context));
        return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
    }

    return nullptr;
}

bool UserDefinedFunctionFactory::has(const String & function_name, ContextPtr context)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    auto load_result = loader.getLoadResult(function_name);

    bool result = load_result.object != nullptr;
    return result;
}

std::vector<String> UserDefinedFunctionFactory::getRegisteredNames(ContextPtr context)
{
    const auto & loader = ExternalUserDefinedFunctionsLoader::instance(context);
    auto loaded_objects = loader.getLoadedObjects();

    std::vector<std::string> registered_names;
    registered_names.reserve(loaded_objects.size());

    for (auto & loaded_object : loaded_objects)
        registered_names.emplace_back(loaded_object->getLoadableName());

    return registered_names;
}

}
