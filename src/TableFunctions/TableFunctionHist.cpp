#include "TableFunctionHist.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Storages/IStorage.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

TableFunctionHist::TableFunctionHist(const String & name_) : TableFunctionProxyBase(name_)
{
    help_message = fmt::format(
        "Function '{}' requires only 1 parameter"
        "<name of the stream>, it should be a stream storage",
        name);
}

void TableFunctionHist::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    if (func_ast->children.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// table(table_id)
    auto * node = func_ast->as<ASTFunction>();
    ASTs & args = node->arguments->children;
    if (args.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// First argument is expected to be table or table function
    storage_id = resolveStorageID(args[0], context);

    /// Calculate column description
    init(context, func_ast, functionNamePrefix(), nullptr);
}

void TableFunctionHist::init(
    ContextPtr context, ASTPtr /*streaming_func_ast*/, const String & /*func_name_prefix*/, ASTPtr /*timestamp_expr_ast*/)
{
    streaming = false;
    auto storage = DatabaseCatalog::instance().getTable(storage_id, context);
    if (storage->getName() != "Stream" && storage->getName() != "MaterializedView" && storage->getName() != "Kafka")
        throw Exception("Storage engine is not stream or MaterializedView or Kafka", ErrorCodes::BAD_ARGUMENTS);

    underlying_storage_metadata_snapshot = storage->getInMemoryMetadataPtr();
    columns = underlying_storage_metadata_snapshot->getColumns();
}

DataTypePtr TableFunctionHist::getElementType(const DataTypeTuple * tuple) const
{
    DataTypePtr element_type = tuple->getElements()[0];
    assert(isArray(element_type));

    const auto * array_type = checkAndGetDataType<DataTypeArray>(element_type.get());
    return array_type->getNestedType();
}

void registerTableFunctionHist(TableFunctionFactory & factory)
{
    factory.registerFunction("table", []() -> TableFunctionPtr { return std::make_shared<TableFunctionHist>("table"); });
}
}
