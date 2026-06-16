#include <Server/RestRouterHandlers/SchemaValidator.h>
#include <Server/RestRouterHandlers/UDFHandler.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>
#include <Interpreters/Streaming/ASTToJSONUtils.h>
#if USE_V8
#include <V8/Utils.h>
#endif

#include <numeric>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_REQUEST_PARAMETER;
extern const int UDF_INVALID_NAME;
}

namespace
{
Poco::JSON::Object::Ptr fromUserDefinedFunctionDescriptor(const cluster::protocol::UserDefinedFunctionDescriptorPtr & func_desc)
{
    Poco::JSON::Object::Ptr func(new Poco::JSON::Object());
    func->set("name", func_desc->name);
    func->set("return_type", func_desc->return_type);
    func->set("created_at", func_desc->create_timestamp_ms);
    func->set("last_modified_at", func_desc->last_modify_timestamp_ms);
    func->set("created_by", func_desc->created_by);
    func->set("last_modified_by", func_desc->last_modified_by);

    Poco::JSON::Array::Ptr args(new Poco::JSON::Array());
    for (const cluster::protocol::UserDefinedFunctionDescriptor::Argument & arg : func_desc->arguments)
    {
        Poco::JSON::Object::Ptr arg_obj(new Poco::JSON::Object());
        arg_obj->set("name", arg.name);
        arg_obj->set("type", arg.type);
        args->add(arg_obj);
    }

    func->set("arguments", args);

    using cluster::protocol::UDFType;
    switch (func_desc->type())
    {
        case UDFType::Executable:
        {
            func->set("type", "executable");

            auto payload = std::dynamic_pointer_cast<cluster::protocol::ExecutableUserDefinedFunctionPayload>(func_desc->payload);

            func->set("format", payload->format);
            func->set("command", payload->command);

            Poco::JSON::Array::Ptr command_arguments(new Poco::JSON::Array());
            for (const String & arg : payload->command_arguments)
            {
                command_arguments->add(arg);
            }
            func->set("command_arguments", command_arguments);

            func->set("command_termination_timeout_seconds", payload->command_termination_timeout_seconds);
            func->set("command_read_timeout_milliseconds", payload->command_read_timeout_milliseconds);
            func->set("command_write_timeout_milliseconds", payload->command_write_timeout_milliseconds);
            func->set("max_command_execution_time_seconds", payload->max_command_execution_time_seconds);
            func->set("pool_size", payload->pool_size);
            func->set("is_executable_pool", payload->is_executable_pool);
            func->set("send_chunk_header", payload->send_chunk_header);
            func->set("execute_direct", payload->execute_direct);
            break;
        }
        case UDFType::Remote:
        {
            func->set("type", "remote");

            auto payload = std::dynamic_pointer_cast<cluster::protocol::RemoteUserDefinedFunctionPayload>(func_desc->payload);

            func->set("url", payload->url);
            func->set("command_read_timeout_milliseconds", payload->command_read_timeout_milliseconds);

            using AuthMethod = cluster::protocol::RemoteUserDefinedFunctionPayload::AuthMethod;
            if (payload->auth_method == AuthMethod::AuthHeader)
            {
                func->set("auth_method", "auth_header");

                Poco::JSON::Object::Ptr auth_context(new Poco::JSON::Object());
                auth_context->set("auth_header", payload->auth_context.key_name);
                auth_context->set("auth_value", payload->auth_context.key_value);

                func->set("auth_context", auth_context);
            }

            break;
        }
        case UDFType::Javascript:
        {
            func->set("type", "javascript");

            auto payload = std::dynamic_pointer_cast<cluster::protocol::JavaScriptUserDefinedFunctionPayload>(func_desc->payload);

            func->set("source", payload->source);
            func->set("is_aggregation", payload->is_aggregation);
            break;
        }
        case UDFType::Python:
        {
            func->set("type", "python");
            auto payload = std::dynamic_pointer_cast<cluster::protocol::PythonUserDefinedFunctionPayload>(func_desc->payload);
            func->set("source", payload->source);
            func->set("is_aggregation", payload->is_aggregation);
            func->set("numpy_optimize_enable", payload->using_numpy);
            if (!payload->init_function_name.empty())
                func->set("init_function_name", payload->init_function_name);
            if (!payload->init_function_parameters.empty())
                func->set("init_function_parameters", payload->init_function_parameters);
            if (!payload->named_collection.empty())
                func->set("named_collection", payload->named_collection);
            break;
        }
        case UDFType::SQL:
        {
            func->set("type", "sql");

            auto payload = std::dynamic_pointer_cast<cluster::protocol::SQLUserDefinedFunctionPayload>(func_desc->payload);
            func->set("query", payload->query);
            break;
        }
    }

    return func;
}

String buildResponse(const String & query_id)
{
    Poco::JSON::Object resp;
    resp.set("request_id", query_id);

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String buildResponse(const Poco::JSON::Array::Ptr & object, const String & query_id)
{
    Poco::JSON::Object resp;
    resp.set("request_id", query_id);
    resp.set("data", object);
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String buildResponse(const Poco::JSON::Object::Ptr & object, const String & query_id)
{
    Poco::JSON::Object resp;
    resp.set("request_id", query_id);
    resp.set("data", object);
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String buildResponse(const cluster::protocol::UserDefinedFunctionDescriptorPtrs & func_descs, const String & query_id)
{
    Poco::JSON::Array::Ptr functions(new Poco::JSON::Array());
    for (const auto & func_desc : func_descs)
    {
        functions->add(fromUserDefinedFunctionDescriptor(func_desc));
    }
    return buildResponse(functions, query_id);
}
}

std::pair<String, Int32> UDFHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    auto timeout_ms = query_context->getSettingsRef().query_timeout_sec * 1000;

    const auto & func = getPathParameter("func");
    auto & metastore = Globals::getMetaStore();
    if (func.empty())
    {
        /// List all functions
        auto list_all_udfs_resp = metastore.listAllUserDefinedFunctions();

        if (list_all_udfs_resp->hasError())
        {
            const auto & err = list_all_udfs_resp->error();
            return {
                jsonErrorResponse(
                    fmt::format("Failed to list all functions: error_code={} error_message={}", err.error_code, err.error_message),
                    err.error_code),
                HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
        }

        return {buildResponse(list_all_udfs_resp->data().descs, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }

    auto request = std::make_shared<cluster::ListUserDefinedFunctionsRequest>(
        func, metastore.nodeID(), /*consistent_read_=*/false, timeout_ms, /*request_version=*/2);
    auto list_udfs_resp = metastore.listUserDefinedFunctions(request);
    if (list_udfs_resp->hasError())
    {
        const auto & err = list_udfs_resp->error();
        return {
            jsonErrorResponse(
                fmt::format("Failed to get function {}: error_code={} error_message={}", func, err.error_code, err.error_message),
                err.error_code),
            HTTPResponse::HTTP_NOT_FOUND};
    }

    const auto & func_descs = list_udfs_resp->data().descs;

    if (func_descs.size() == 1)
    {
        return {
            buildResponse(fromUserDefinedFunctionDescriptor(list_udfs_resp->data().descs[0]), query_context->getCurrentQueryId()),
            HTTPResponse::HTTP_OK};
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "More than one function with the same name");
    }
}

std::map<String, std::map<String, String>> UDFHandler::create_schema
    = {{"required", {{"name", "string"}, {"type", "string"}, {"return_type", "string"}}},
       {"optional", {{"format", "string"}, {"command", "string"}, {"url", "string"}, {"auth_method", "string"}}}};

bool UDFHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(create_schema, payload, error_msg);
}

std::pair<String, Int32> UDFHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    String name = payload->get("name").toString();
    Poco::JSON::Object::Ptr func(new Poco::JSON::Object());
    func->set("function", payload);

    bool replace_if_exists = payload->optValue<bool>("replace_if_exists", false);

    try
    {
        UserDefinedFunctionFactory::instance().registerFunction(query_context, name, func, true /* throw_if_exists*/, replace_if_exists);
    }
    catch (const DB::Exception & e)
    {
        return {jsonErrorResponse(e.displayText(), e.code()), HTTPResponse::HTTP_BAD_REQUEST};
    }

    return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> UDFHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    const auto & func = getPathParameter("func", "");
    if (func.empty())
        return {jsonErrorResponse("Missing UDF name", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

    UserDefinedFunctionFactory::instance().unregisterFunction(query_context, func, true /*throw_if_not_exists*/);
    return {buildResponse(query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}
}
