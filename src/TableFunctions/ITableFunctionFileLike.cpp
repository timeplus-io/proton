#include <TableFunctions/ITableFunctionFileLike.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Common/Exception.h>

#include <Storages/StorageFile.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    void checkIfFormatSupportsAutoStructure(const String & name, const String & format)
    {
        if (name == "file" && format == "Distributed")
            return;

        if (FormatFactory::instance().checkIfFormatHasAnySchemaReader(format))
            return;
        /// proton: starts
        throw Exception(
            "Function '" + name
                + "' allows automatic structure determination only for formats that support schema inference and for Distributed format in function "
                  "'file'",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        /// proton: ends
    }
}

void ITableFunctionFileLike::parseFirstArguments(const ASTPtr & arg, const ContextPtr &)
{
        filename = checkAndGetLiteralArgument<String>(arg, "source");
}

void ITableFunctionFileLike::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        /// proton: starts
        throw Exception("Function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);
        /// proton: ends

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        /// proton: starts
        throw Exception("Function '" + getName() + "' requires at least 1 argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        /// proton: ends

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    parseFirstArguments(args[0], context);

    if (args.size() > 1)
        format = checkAndGetLiteralArgument<String>(args[1], "format");

    if (format == "auto")
        format = FormatFactory::instance().getFormatFromFileName(filename, true);

    if (args.size() <= 2)
    {
        checkIfFormatSupportsAutoStructure(getName(), format);
        return;
    }

    if (args.size() != 3 && args.size() != 4)
        /// proton: starts
        throw Exception("Function '" + getName() + "' requires 1, 2, 3 or 4 arguments: filename, format (default auto), structure (default auto) and compression method (default auto)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        /// proton: ends

    structure = checkAndGetLiteralArgument<String>(args[2], "structure");
    if (structure == "auto")
        checkIfFormatSupportsAutoStructure(getName(), format);

    if (structure.empty())
        /// proton: starts
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Stream structure is empty for function '{}'. If you want to use automatic schema inference, use 'auto'",
            ast_function->formatForErrorMessage());
        /// proton: ends

    if (args.size() == 4)
        compression_method = checkAndGetLiteralArgument<String>(args[3], "compression_method");
}

StoragePtr ITableFunctionFileLike::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    ColumnsDescription columns;
    if (structure != "auto")
        columns = parseColumnsListFromString(structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = getStorage(filename, format, columns, context, table_name, compression_method);
    storage->startup();
    return storage;
}

}
