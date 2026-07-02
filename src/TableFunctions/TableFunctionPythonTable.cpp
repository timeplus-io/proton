#include <TableFunctions/TableFunctionPythonTable.h>

#if USE_PYTHON_UDF

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/Python/StoragePythonTable.h>
#include <Storages/ExternalStream/Python/StoragePythonTableWithInput.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/storageUtil.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UNKNOWN_TABLE;
}

void TableFunctionPythonTable::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires arguments", name);

    const auto & args = function->arguments->children;
    if (args.size() < 3)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires at least 3 arguments: external_stream_name, source, and column names. "
            "Usage: {}(py_external_stream, source_table_or_subquery, col1, col2, ...)",
            name,
            name);

    /// First argument is the Python external stream name (identifier or string)
    input_column_names.clear();
    source_ast.reset();
    source_storage.reset();
    python_function = {};
    python_mode = PythonTableMode::Auto;
    if (auto storage_id_opt = tryGetStorageID(args[0]))
        python_table_id = *storage_id_opt;
    else
        python_table_id = StorageID(
            context->getCurrentDatabase(),
            checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context), "external_table_name"));

    if (python_table_id.database_name.empty())
        python_table_id.database_name = context->getCurrentDatabase();

    /// Look up the Python external stream
    auto external_storage = DatabaseCatalog::instance().tryGetTable(python_table_id, context);

    if (!external_storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Python external stream '{}' not found", python_table_id.getNameForLogs());

    auto * external_stream = external_storage->as<StorageExternalStream>();
    if (!external_stream || external_stream->getType() != ExternalStreamTypes::PYTHON)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table '{}' is not a Python external stream", python_table_id.getNameForLogs());

    auto python_storage = external_stream->getNested();
    auto * python_table = python_storage ? python_storage->as<StoragePythonTable>() : nullptr;
    if (!python_table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python external stream '{}' is not initialized", python_table_id.getNameForLogs());

    /// Get the Python source code and output schema from the external stream
    output_columns = external_storage->getInMemoryMetadataPtr()->getColumns();
    python_function = python_table->getFunction();
    python_mode = python_table->getMode();

    if (python_function.source_code.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python external stream '{}' has empty source", python_table_id.getNameForLogs());

    /// Second argument is the source (subquery or table reference)
    source_ast.reset();
    source_storage.reset();

    /// Remaining arguments are column names
    for (size_t i = 2; i < args.size(); ++i)
    {
        if (auto * identifier = args[i]->as<ASTIdentifier>())
        {
            input_column_names.push_back(identifier->name());
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument {} of table function '{}' must be a column name identifier", i + 1, name);
        }
    }

    /// Resolve the source
    if (auto * subquery = args[1]->as<ASTSubquery>())
    {
        /// Source is a subquery
        source_ast = args[1]->clone();
        auto interpreter
            = std::make_unique<InterpreterSelectWithUnionQuery>(subquery->children[0], context, SelectQueryOptions().subquery().analyze());
        auto source_header = interpreter->getSampleBlock();

        /// Validate that the requested columns exist in the source
        for (const auto & col_name : input_column_names)
        {
            if (!source_header.has(col_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' not found in source for table function '{}'", col_name, name);
        }
    }
    else if (args[1]->as<ASTFunction>())
    {
        /// Source is a table function call
        auto query_context = context->getQueryContext();
        source_storage = query_context->executeTableFunction(args[1]);
        auto source_metadata = source_storage->getInMemoryMetadataPtr();
        auto source_columns = source_metadata->getColumns();

        for (const auto & col_name : input_column_names)
        {
            if (!source_columns.has(col_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' not found in source for table function '{}'", col_name, name);
        }
    }
    else if (auto storage_id_opt = tryGetStorageID(args[1]))
    {
        /// Source is a table/stream reference
        auto storage_id = *storage_id_opt;
        if (storage_id.database_name.empty())
            storage_id.database_name = context->getCurrentDatabase();

        source_storage = DatabaseCatalog::instance().getTable(storage_id, context);
        auto source_metadata = source_storage->getInMemoryMetadataPtr();
        auto source_columns = source_metadata->getColumns();

        for (const auto & col_name : input_column_names)
        {
            if (!source_columns.has(col_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' not found in source for table function '{}'", col_name, name);
        }
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Second argument of table function '{}' must be a table name, table function, or subquery", name);
    }
}

ColumnsDescription TableFunctionPythonTable::getActualTableStructure(ContextPtr /*context*/) const
{
    return output_columns;
}

StoragePtr TableFunctionPythonTable::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto storage = StoragePythonTableWithInput::create(
        StorageID(getDatabaseName(), table_name),
        output_columns,
        python_function,
        source_ast,
        source_storage,
        input_column_names,
        python_mode,
        context);

    storage->startup();
    return storage;
}

void registerTableFunctionPythonTable(TableFunctionFactory & factory)
{
    factory.registerFunction(
        TableFunctionPythonTable::name,
        TableFunctionFactoryData{
            []() -> TableFunctionPtr { return std::make_shared<TableFunctionPythonTable>(); },
            {.documentation = {
                 .description = "Call a Python external stream as a table function with input data",
                 .examples = {{"basic", "SELECT * FROM python_table(py_transform, my_stream, col1, col2)", ""}},
             }},
        },
        TableFunctionFactory::CaseSensitive,
        /*support_subquery=*/true);
    factory.registerAlias("python_pipe", TableFunctionPythonTable::name);
}
}

#else

namespace DB
{
class TableFunctionFactory;

void registerTableFunctionPythonTable(TableFunctionFactory &)
{
}
}

#endif
