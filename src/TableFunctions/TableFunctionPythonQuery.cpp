#include <TableFunctions/TableFunctionPythonQuery.h>

#if USE_PYTHON_UDF

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListUserDefinedFunctionsRequest.h>
#include <Core/Streaming/DataStreamSemantic.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Storages/ExternalStream/Python/StoragePythonTable.h>
#include <Storages/Proxy/ProxyStream.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/assert_cast.h>

#include <boost/algorithm/string.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int RESOURCE_NOT_FOUND;
extern const int UDF_INTERNAL_ERROR;
extern const int UNSUPPORTED;
}

namespace
{
cluster::protocol::UserDefinedFunctionDescriptorPtr getUDFDescription(const String & function_name, const ContextPtr & context)
{
    auto timeout_ms = context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();

    auto udf_req = std::make_shared<cluster::ListUserDefinedFunctionsRequest>(
        function_name, metastore.nodeID(), /*consistent_read_=*/false, timeout_ms, /*request_version=*/2);
    auto udf_resp = metastore.listUserDefinedFunctions(udf_req);

    if (udf_resp->hasError())
        throw Exception(udf_resp->error().error_code, "Failed to load UDF {}, error={}", function_name, udf_resp->error().error_message);

    if (udf_resp->data().descs.empty())
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "UDF {} was not found", function_name);

    if (udf_resp->data().descs.size() > 1)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Found multiple UDFs with the same name '{}'", function_name);

    auto udf_desc = udf_resp->data().descs[0];
    if (!udf_desc || !udf_desc->payload)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "UDF '{}' has no payload", function_name);

    if (udf_desc->type() != cluster::protocol::UDFType::Python)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "UDF '{}' is not a Python function", function_name);

    return udf_desc;
}

String extractTableStructure(const String & return_type)
{
    auto trimmed_type = boost::trim_copy(return_type);
    auto lower = boost::algorithm::to_lower_copy(trimmed_type);
    if (!boost::starts_with(lower, "table"))
        throw Exception(
            ErrorCodes::UNSUPPORTED, "Python table function requires RETURN TABLE(...), but UDF return type is {}", return_type);

    auto open_pos = trimmed_type.find('(');
    auto close_pos = trimmed_type.rfind(')');
    if (open_pos == String::npos || close_pos == String::npos || close_pos <= open_pos + 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid TABLE return type format: {}", return_type);

    auto definition = trimmed_type.substr(open_pos + 1, close_pos - open_pos - 1);
    return boost::trim_copy(definition);
}
}

void TableFunctionPythonQuery::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (function == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' is not a function", getName());

    const auto & arguments = function->arguments == nullptr ? ASTs{} : function->arguments->children;
    if (arguments.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' requires 1 argument but got {}", getName(), arguments.size());

    auto function_name
        = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(arguments[0], context), "function_name");

    udf_desc = getUDFDescription(function_name, context);

    const auto & python_payload = assert_cast<cluster::protocol::PythonUserDefinedFunctionPayload &>(*udf_desc->payload);
    if (python_payload.is_aggregation)
        throw Exception(ErrorCodes::UNSUPPORTED, "python table function does not support aggregation UDF {}", function_name);
    if (!udf_desc->arguments.empty())
        throw Exception(ErrorCodes::UNSUPPORTED, "python table function only supports UDFs without arguments");

    auto structure_definition = extractTableStructure(udf_desc->return_type);
    result_columns = parseColumnsListFromString(structure_definition, context);
}

ColumnsDescription TableFunctionPythonQuery::getActualTableStructure(ContextPtr) const
{
    return result_columns;
}

StoragePtr TableFunctionPythonQuery::executeImpl(const ASTPtr &, ContextPtr context, const String & table_name, ColumnsDescription) const
{
    auto storage_id = StorageID(getDatabaseName(), table_name);
    const auto & python_payload = assert_cast<cluster::protocol::PythonUserDefinedFunctionPayload &>(*udf_desc->payload);

    auto storage = StoragePythonTable::create(storage_id, result_columns, udf_desc->name, python_payload.source);
    storage->startup();
    Streaming::DataStreamSemanticEx data_stream_semantic{Streaming::DataStreamSemantic::Append};
    data_stream_semantic.streaming = false;

    return Streaming::ProxyStream::create(
        storage_id,
        result_columns,
        context,
        /*table_func_desc=*/nullptr,
        /*timestamp_func_desc=*/nullptr,
        /*nested_proxy_storage=*/nullptr,
        getName(),
        storage,
        /*subquery=*/nullptr,
        data_stream_semantic,
        /*streaming=*/false);
}

void registerTableFunctionPythonQuery(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPythonQuery>({.documentation = {}, .allow_readonly = true});
    factory.registerAlias("python_query", TableFunctionPythonQuery::name);
}
}

#else
namespace DB
{
class TableFunctionFactory;

void registerTableFunctionPythonQuery(TableFunctionFactory &)
{
}
}
#endif
