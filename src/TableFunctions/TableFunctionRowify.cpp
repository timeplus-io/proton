#include <TableFunctions/TableFunctionRowify.h>

#include <Parsers/ASTFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Streaming
{
TableFunctionRowify::TableFunctionRowify(const String & name_) : TableFunctionProxyBase(name_)
{
    help_message = fmt::format(
        "Function '{}' requires exactly 1 parameter: the source stream or table. "
        "This function splits batch input into separate rows output. "
        "For example, rowify(my_stream). Usage: SELECT sum(x) FROM rowify(stream) EMIT ON UPDATE;",
        name);
}

void TableFunctionRowify::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    if (func_ast->children.size() != 1)
        throw Exception::createRuntime(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, help_message);

    /// rowify(table_id)
    auto * node = func_ast->as<ASTFunction>();
    ASTs & args = node->arguments->children;
    if (args.size() != 1)
        throw Exception::createRuntime(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, help_message);

    /// First (and only) argument is expected to be table/stream
    resolveStorageID(args[0], context);

    /// Calculate column description
    TableFunctionProxyBase::calculateColumnDescriptions(context);
}

void registerTableFunctionRowify(TableFunctionFactory & factory)
{
    factory.registerFunction(
        "rowify",
        TableFunctionFactoryData{
            []() -> TableFunctionPtr { return std::make_shared<TableFunctionRowify>("rowify"); },
            {},
        },
        TableFunctionFactory::CaseSensitive,
        /*support subquery=*/true);
}
}
}
