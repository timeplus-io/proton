#include <Functions/UserDefined/ExecutableUserDefinedFunction.h>
#include <Functions/UserDefined/RemoteUserDefinedFunction.h>
#include <Functions/UserDefined/UDFHelper.h>
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>

#include "config.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/EnsurePython.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#if USE_V8
#include <AggregateFunctions/AggregateFunctionJavaScriptAdapter.h>
#include <Functions/UserDefined/JavaScriptUserDefinedFunction.h>
#include <V8/Utils.h>
#endif

#if USE_PYTHON_UDF
#include <AggregateFunctions/AggregateFunctionPythonAdapter.h>
#include <CPython/validatePython.h>
#include <Functions/UserDefined/PythonUserDefinedFunction.h>
#endif

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/UDFType.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Cluster/Requests/ListUserDefinedFunctionsRequest.h>
#include <Cluster/Requests/ListUserDefinedFunctionsResponse.h>


#include <magic_enum.hpp>
#include <Poco/Util/JSONConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int FUNCTION_ALREADY_EXISTS;
extern const int CANNOT_CREATE_RECURSIVE_FUNCTION;
extern const int CANNOT_DROP_FUNCTION;
extern const int UNKNOWN_FUNCTION;
extern const int UDF_INTERNAL_ERROR;
extern const int UNSUPPORTED;
extern const int UNSUPPORTED_METHOD;
extern const int INTERNAL_ERROR;
}

namespace
{
cluster::protocol::UserDefinedFunctionDescriptorPtr tryGetUDFDescription(const String & function_name, const ContextPtr & context)
{
    if (!Globals::isMetaStoreInited())
        /// Proton client/unittest doesn't initialize metastore
        /// throw DB::Exception(ErrorCodes::UNKNOWN_FUNCTION, "MetaStore is not initialized");
        return nullptr;

    int64_t timeout_ms = 10'000;
    if (context)
        timeout_ms = context->getSettingsRef().query_timeout_sec * 1000;

    auto & metastore = Globals::getMetaStore();
    auto udf_req = std::make_shared<cluster::ListUserDefinedFunctionsRequest>(
        function_name, metastore.nodeID(), /*consistent_read_=*/false, timeout_ms, /*request_version=*/2);
    auto udf_resp = metastore.listUserDefinedFunctions(udf_req);
    if (udf_resp->hasError() || udf_resp->data().descs.empty())
        return nullptr;

    if (udf_resp->data().descs.size() > 1)
        throw DB::Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Multiple UDFs with the same name '{}'", function_name);

    auto udf_desc = udf_resp->data().descs[0];
    if (!udf_desc || !udf_desc->payload)
        throw DB::Exception(ErrorCodes::UDF_INTERNAL_ERROR, "UDF '{}' has no payload", function_name);

    return udf_desc;
}

ASTPtr parseCreateSQLFunctionQuery(const String & query, ContextPtr context)
{
    ParserCreateFunctionQuery parser;
    return parseQuery(parser, query.data(), query.data() + query.size(), "", 0, context ? context->getSettingsRef().max_parser_depth : 100);
}

void validateSQLFunctionRecursiveness(const IAST & node, const String & function_to_create)
{
    for (const auto & child : node.children)
    {
        auto function_name_opt = tryGetFunctionName(child);
        if (function_name_opt && function_name_opt.value() == function_to_create)
            throw Exception(ErrorCodes::CANNOT_CREATE_RECURSIVE_FUNCTION, "You cannot create recursive function");

        validateSQLFunctionRecursiveness(*child, function_to_create);
    }
}

ASTPtr normalizeCreateFunctionQuery(const IAST & create_function_query)
{
    auto ptr = create_function_query.clone();
    auto & res = typeid_cast<ASTCreateFunctionQuery &>(*ptr);
    res.if_not_exists = false;
    res.or_replace = false;
    FunctionNameNormalizer().visit(res.function_core.get());
    return ptr;
}
}

UserDefinedFunctionFactory::UserDefinedFunctionFactory() : logger(::getLogger("UserDefinedFunctionFactory"))
{
}

UserDefinedFunctionFactory & UserDefinedFunctionFactory::instance()
{
    static UserDefinedFunctionFactory result;
    return result;
}

FunctionOverloadResolverPtr UserDefinedFunctionFactory::get(const String & function_name, ContextPtr context)
{
    auto udf_func = tryGet(function_name, context);
    if (!udf_func)
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "UserDefined function '{}' doesn't exist", function_name);

    return udf_func;
}

AggregateFunctionPtr UserDefinedFunctionFactory::tryGetAggregateFunction(
    const String & function_name,
    const DataTypes & types,
    [[maybe_unused]] const Array & parameters,
    AggregateFunctionProperties & /*properties*/,
    ContextPtr context,
    [[maybe_unused]] bool is_changelog_input)
{
    auto udf_desc = tryGetUDFDescription(function_name, context);
    if (!udf_desc)
        return nullptr;

    [[maybe_unused]] auto validate_arguments = [&](size_t num_args) {
        if (types.size() != num_args)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "expect {} arguments but get {} for UDF {}",
                udf_desc->arguments.size(),
                types.size(),
                udf_desc->name);

        for (size_t i = 0; i < num_args; i++)
        {
            auto arg_type = DataTypeFactory::instance().get(udf_desc->arguments[i].type);
            if (types[i]->getTypeId() != arg_type->getTypeId())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "argument {} of UDF {}: expect type '{}' but get '{}'",
                    i,
                    udf_desc->name,
                    arg_type->getName(),
                    types[i]->getName());
        }
    };

    switch (udf_desc->type())
    {
        case cluster::protocol::UDFType::Javascript:
        {
#if USE_V8
            const auto & js_payload = assert_cast<const cluster::protocol::JavaScriptUserDefinedFunctionPayload &>(*udf_desc->payload);
            if (!js_payload.is_aggregation)
                return nullptr;

            /// check arguments
            size_t num_of_args = udf_desc->arguments.size();
            validate_arguments(types.back()->getName() == "int8" ? num_of_args : num_of_args - 1);

            if (!context || !context->getSettingsRef().javascript_max_memory_bytes)
            {
                LOG_ERROR(instance().getLogger(), "query_context is invalid");
                return nullptr;
            }
            return std::make_shared<AggregateFunctionJavaScriptAdapter>(
                std::move(udf_desc),
                types,
                parameters,
                is_changelog_input,
                context->getSettingsRef().javascript_max_memory_bytes,
                context->getSettingsRef().v8_log_interval_ms);
#else
            throw Exception(ErrorCodes::UNSUPPORTED, "JavaScript UDF is not enabled");
#endif
        }
        case cluster::protocol::UDFType::Python:
        {
            ensurePythonInited();
#if USE_PYTHON_UDF
            const auto & py_payload = assert_cast<const cluster::protocol::PythonUserDefinedFunctionPayload &>(*udf_desc->payload);
            if (!py_payload.is_aggregation)
                return nullptr;

            /// check arguments
            size_t num_of_args = udf_desc->arguments.size();
            validate_arguments(types.back()->getName() == "int8" ? num_of_args : num_of_args - 1);

            return std::make_shared<AggregateFunctionPythonAdapter>(std::move(udf_desc), types, parameters, is_changelog_input);
#else
            throw Exception(ErrorCodes::UNSUPPORTED, "Python UDF is not enabled");
#endif
        }
        default:
            throw Exception(ErrorCodes::UNSUPPORTED, "UDF type '{}' is not supported", magic_enum::enum_name(udf_desc->type()));
    }
    UNREACHABLE();
}

ASTPtr UserDefinedFunctionFactory::tryGetSQLFunction(const String & function_name)
{
    auto context = Context::getGlobalContextInstance();
    auto udf_desc = tryGetUDFDescription(function_name, context);
    if (!udf_desc)
        return nullptr;

    if (udf_desc->type() != cluster::protocol::UDFType::SQL)
        return nullptr;

    const auto & sql_payload = assert_cast<const cluster::protocol::SQLUserDefinedFunctionPayload &>(*udf_desc->payload);
    return parseCreateSQLFunctionQuery(sql_payload.query, context);
}

bool UserDefinedFunctionFactory::isAggregateFunctionName(const String & function_name)
{
    try
    {
        auto udf_desc = tryGetUDFDescription(function_name, nullptr);
        if (udf_desc == nullptr)
            return false;

        if (udf_desc->type() == cluster::protocol::UDFType::Javascript || udf_desc->type() == cluster::protocol::UDFType::Python)
        {
            assert(udf_desc->payload);
            return udf_desc->payload->isAggregation();
        }
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(instance().getLogger(), "UserDefinedFunctionFactory::isAggregateFunctionName() failed: {}", e.displayText());
        return false;
    }
    return false;
}

bool UserDefinedFunctionFactory::isSQLFunctionName(const String & function_name)
{
    try
    {
        auto udf_desc = tryGetUDFDescription(function_name, nullptr);
        if (udf_desc == nullptr)
            return false;

        return udf_desc->type() == cluster::protocol::UDFType::SQL;
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(instance().getLogger(), "UserDefinedFunctionFactory::isSQLFunctionName() failed: {}", e.displayText());
        return false;
    }
}

bool UserDefinedFunctionFactory::isOrdinaryFunctionName(const String & function_name)
{
    try
    {
        auto udf_desc = tryGetUDFDescription(function_name, nullptr);
        if (udf_desc == nullptr)
            return false;

        if (udf_desc->type() == cluster::protocol::UDFType::Javascript || udf_desc->type() == cluster::protocol::UDFType::Python)
        {
            assert(udf_desc->payload);
            return !udf_desc->payload->isAggregation();
        }
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(instance().getLogger(), "UserDefinedFunctionFactory::isOrdinaryFunctionName() failed: {}", e.displayText());
        return false;
    }

    return false;
}

FunctionOverloadResolverPtr UserDefinedFunctionFactory::tryGet(const String & function_name, ContextPtr context)
{
    try
    {
        using UDFType = cluster::protocol::UDFType;

        auto udf_desc = tryGetUDFDescription(function_name, context);
        if (!udf_desc)
            return nullptr;

        switch (udf_desc->type())
        {
            case UDFType::Executable:
            {
                auto function = std::make_shared<ExecutableUserDefinedFunction>(std::move(udf_desc), std::move(context));
                return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
            }
            case UDFType::Remote:
            {
                auto function = std::make_shared<RemoteUserDefinedFunction>(std::move(udf_desc), std::move(context));
                return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
            }
            case UDFType::Javascript:
            {
#if USE_V8
                auto function = std::make_shared<JavaScriptUserDefinedFunction>(std::move(udf_desc), std::move(context));
                return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
#else
                throw Exception(ErrorCodes::UNSUPPORTED, "JavaScript UDF is not enabled");
#endif
            }
            case UDFType::Python:
            {
                ensurePythonInited();
#if USE_PYTHON_UDF
                auto function = std::make_shared<PythonUserDefinedFunction>(std::move(udf_desc), std::move(context));
                return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(function));
#else
                throw Exception(ErrorCodes::UNSUPPORTED, "Python UDF is not enabled");
#endif
            }
            case UDFType::SQL:
            {
                return nullptr;
            }
        }
        UNREACHABLE();
    }
    catch (const Exception & e)
    {
        LOG_ERROR(instance().getLogger(), "UserDefinedFunctionFactory::tryGet() failed: {}", e.displayText());
        return nullptr;
    }
}

bool UserDefinedFunctionFactory::has(const String & function_name, ContextPtr context)
{
    try
    {
        return tryGetUDFDescription(function_name, context) != nullptr;
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(instance().getLogger(), "UserDefinedFunctionFactory::has() failed: {}", e.displayText());
        return false;
    }
}

std::vector<std::pair<String, cluster::protocol::UDFType>> UserDefinedFunctionFactory::getRegisteredNameAndTypes(ContextPtr /*context*/)
{
    /// timeout_ms was used for distributed requests, not needed in single-instance
    /// int64_t timeout_ms = context ? context->getSettingsRef().query_timeout_sec * 1000 : 10'000;
    auto & metastore = Globals::getMetaStore();
    auto list_udf_resp = metastore.listAllUserDefinedFunctions();
    if (list_udf_resp->hasError())
    {
        LOG_ERROR(instance().getLogger(), "listAllUserDefinedFunctions failed");
        return {};
    }

    auto & udf_descs = list_udf_resp->data().descs;

    std::vector<std::pair<std::string, cluster::protocol::UDFType>> registered_names;
    registered_names.reserve(udf_descs.size());

    for (auto & udf_desc : udf_descs)
        registered_names.emplace_back(udf_desc->name, udf_desc->type());

    return registered_names;
}

bool UserDefinedFunctionFactory::registerFunction(
    ContextPtr context, const String & function_name, Poco::JSON::Object::Ptr json_func, bool throw_if_exists, bool replace_if_exists)
{
    Streaming::validateUDFName(function_name);

    assert(json_func->has("function"));
    Poco::JSON::Object::Ptr config = json_func->getObject("function");
    assert(config);

    assert(config->has("type"));
    if (config->get("type") == "javascript")
        validateJavaScriptFunction(config);
#if USE_PYTHON_UDF
    else if (config->get("type") == "python")
        validatePythonFunction(config);
#endif
    else if (config->get("type") == "sql")
    {
        auto function_ast = parseCreateSQLFunctionQuery(config->get("query").toString(), context);
        validateSQLFunction(assert_cast<const ASTCreateFunctionQuery &>(*function_ast).function_core, function_name);

        function_ast = normalizeCreateFunctionQuery(*function_ast);
        config->set("query", serializeAST(*function_ast, /*one_line=*/false));
    }

    /// Check whether the function already exists.
    if (FunctionFactory::instance().hasBuiltInNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);

    if (AggregateFunctionFactory::instance().hasBuiltInNameOrAlias(function_name))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The aggregate function '{}' already exists", function_name);

    if (!replace_if_exists && has(function_name, context))
    {
        if (throw_if_exists)
            throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "The function '{}' already exists", function_name);
        else
            return false;
    }

    cluster::protocol::ExistsOperation exists_op;
    if (replace_if_exists)
        exists_op = cluster::protocol::ExistsOperation::Replace;
    else if (throw_if_exists)
        exists_op = cluster::protocol::ExistsOperation::Throw;
    else
        exists_op = cluster::protocol::ExistsOperation::Ignore;

    try
    {
        auto original_desc = tryGetUDFDescription(function_name, context);

        auto create_udf_req = Streaming::createUserDefinedFunctionRequest(
            context, function_name, Poco::Util::JSONConfiguration(json_func), exists_op, original_desc);
        auto & metastore = Globals::getMetaStore();
        auto create_udf_resp = metastore.createUserDefinedFunction(create_udf_req);
        if (create_udf_resp->hasError())
        {
            const auto & err = create_udf_resp->error();
            throw Exception(
                ErrorCodes::FUNCTION_ALREADY_EXISTS,
                "Failed to create function {}: error_code={} error_message={}",
                function_name,
                err.error_code,
                err.error_message);
        }
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("save UDF {} to MetaStore failed", function_name));
        throw;
    }

    return true;
}

#if USE_V8
void UserDefinedFunctionFactory::validateJavaScriptFunction(Poco::JSON::Object::Ptr config)
{
    const String & function_name = config->getValue<String>("name");
    if (!config->has("source"))
        throw Exception(
            ErrorCodes::FUNCTION_ALREADY_EXISTS, "Missing 'source' property of JavaScript function '{}' already exists", function_name);

    /// check whether the source can be compiled
    if (config->has("is_aggregation") && config->getValue<bool>("is_aggregation"))
    {
        /// UDA V8::validateStatelessFunctionSource(function_name, config->get("source"));
        V8::validateAggregationFunctionSource(function_name, {"initialize", "process", "finalize"}, config->get("source"));

        /// add _tp_delta column as the last argument
        assert(config->has("arguments"));
        assert(config->isArray("arguments"));

        bool has_delta_column = false;
        /// Below is a workaround, because Poco::JSON::Object::getArray cannot work well
        Poco::JSON::Array::Ptr json_arguments = config->get("arguments").extract<Poco::JSON::Array::Ptr>();
        for (uint32_t i = 0; i < static_cast<uint32_t>(json_arguments->size()); i++)
        {
            const auto & arg = json_arguments->getObject(i);
            if (arg->has("name") && arg->get("name") == "_tp_delta")
            {
                has_delta_column = true;
                break;
            }
        }

        if (!has_delta_column)
        {
            Poco::JSON::Object delta_col;
            delta_col.set("name", "_tp_delta");
            delta_col.set("type", "int8");
            json_arguments->add(delta_col);
            config->set("arguments", json_arguments);
        }
    }
    else
        /// UDF
        V8::validateStatelessFunctionSource(function_name, config->get("source"));
}
#else
[[noreturn]] void UserDefinedFunctionFactory::validateJavaScriptFunction(Poco::JSON::Object::Ptr config)
{
    UNUSED(config);
    throw Exception(ErrorCodes::UNSUPPORTED, "JavaScript UDF is not enabled");
}
#endif

void UserDefinedFunctionFactory::validatePythonFunction([[maybe_unused]] Poco::JSON::Object::Ptr config)
{
    ensurePythonInited();
#if USE_PYTHON_UDF
    const String & function_name = config->getValue<String>("name");
    if (!config->has("source"))
        throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "Missing 'source' property of Python function '{}'", function_name);

    /// check whether the source can be compiled
    if (config->optValue<bool>("is_aggregation", false))
    {
        cpython::validateAggregationFunctionSource(config, {"process", "finalize"});
        assert(config->has("arguments"));
        assert(config->isArray("arguments"));

        [[maybe_unused]] bool has_delta_column = false;
        Poco::JSON::Array::Ptr json_arguments = config->get("arguments").extract<Poco::JSON::Array::Ptr>();

        for (size_t i = 0; i < json_arguments->size(); i++)
        {
            const auto & arg = json_arguments->getObject(static_cast<unsigned int>(i));
            if (arg->has("name") && arg->get("name") == "_tp_delta")
            {
                has_delta_column = true;
                break;
            }
        }

        if (!has_delta_column)
        {
            Poco::JSON::Object delta_col;
            delta_col.set("name", "_tp_delta");
            delta_col.set("type", "int8");
            json_arguments->add(delta_col);
            config->set("arguments", json_arguments);
        }
    }
    else
    {
        cpython::validateStatelessFunctionSource(config);
    }
#endif
}

void UserDefinedFunctionFactory::validateSQLFunction(ASTPtr function, const String & name)
{
    ASTFunction * lambda_function = function->as<ASTFunction>();

    if (!lambda_function)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Expected function, got: {}", function->formatForErrorMessage());

    auto & lambda_function_expression_list = lambda_function->arguments->children;

    if (lambda_function_expression_list.size() != 2)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have arguments and body");

    const ASTFunction * tuple_function_arguments = lambda_function_expression_list[0]->as<ASTFunction>();

    if (!tuple_function_arguments || !tuple_function_arguments->arguments)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have valid arguments");

    std::unordered_set<String> arguments;

    for (const auto & argument : tuple_function_arguments->arguments->children)
    {
        const auto * argument_identifier = argument->as<ASTIdentifier>();

        if (!argument_identifier)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda argument must be identifier");

        const auto & argument_name = argument_identifier->name();
        auto [_, inserted] = arguments.insert(argument_name);
        if (!inserted)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Identifier {} already used as function parameter", argument_name);
    }

    ASTPtr function_body = lambda_function_expression_list[1];
    if (!function_body)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have valid function body");

    validateSQLFunctionRecursiveness(*function_body, name);
}

bool UserDefinedFunctionFactory::unregisterFunction(
    const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists)
{
    if (FunctionFactory::instance().hasBuiltInNameOrAlias(function_name)
        || AggregateFunctionFactory::instance().hasBuiltInNameOrAlias(function_name))
        throw Exception(ErrorCodes::CANNOT_DROP_FUNCTION, "Cannot drop system function '{}'", function_name);

    if (!has(function_name, context))
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", function_name);
        else
            return false;
    }

    return forceUnregisterFunction(context, function_name, throw_if_not_exists);
}

bool UserDefinedFunctionFactory::forceUnregisterFunction(
    [[maybe_unused]] const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists)
{
    auto & metastore = Globals::getMetaStore();
    auto delete_udf_req = std::make_shared<cluster::DeleteUserDefinedFunctionRequest>(
        function_name, metastore.nodeID(), /*timeout_ms_=*/10'000, /*request_version_=*/2);
    auto delete_udf_resp = metastore.deleteUserDefinedFunction(delete_udf_req);
    if (delete_udf_resp->hasError())
    {
        if (throw_if_not_exists)
            throw Exception(
                ErrorCodes::CANNOT_DROP_FUNCTION,
                "Failed to delete function {}: error_code={} error_message={}",
                function_name,
                delete_udf_resp->error().error_code,
                delete_udf_resp->error().error_message);
        else
            return false;
    }

    return true;
}

}
