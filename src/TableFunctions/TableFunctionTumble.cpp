#include "TableFunctionTumble.h"

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
extern const int BAD_ARGUMENTS;
}

namespace Streaming
{
TableFunctionTumble::TableFunctionTumble(const String & name_) : TableFunctionWindow(name_)
{
}

void TableFunctionTumble::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    doParseArguments(func_ast, context, TUMBLE_HELP_MESSAGE);
}

ASTs TableFunctionTumble::checkAndExtractArguments(ASTFunction * node) const
{
    /// tumble(table, [timestamp_column_expr], win_interval, [timezone])
    return checkAndExtractTumbleArguments(node);
}

void TableFunctionTumble::postArgs(ASTs & args) const
{
    //// [timezone]
    /// Prune the empty timezone if user doesn't specify one
    if (!args.back())
        args.pop_back();
}

String TableFunctionTumble::functionNamePrefix() const
{
    return ProtonConsts::TUMBLE_FUNC_NAME + "(";
}

DataTypePtr TableFunctionTumble::getElementType(const DataTypeTuple * tuple) const
{
    return tuple->getElements()[0];
}

void TableFunctionTumble::validateWindow(FunctionDescriptionPtr desc) const
{
    assert(desc && desc->type == WindowType::TUMBLE);
    UInt32 time_scale = 0;
    if (auto * datetime64 = checkAndGetDataType<DataTypeDateTime64>(desc->argument_types[0].get()))
        time_scale = datetime64->getScale();

    auto & args = desc->func_ast->as<ASTFunction &>().arguments->children;
    auto [window_interval, window_interval_kind] = extractInterval(args[1]->as<ASTFunction>());
    auto window_scale = getAutoScaleByInterval(window_interval, window_interval_kind);
    if (window_scale > time_scale)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid window interval, the window scale '{}' cannot exceed the event time scale '{}' in tumble function",
            window_scale,
            time_scale);

    if ((window_interval_kind == IntervalKind::Millisecond && (3600 * common::exp10_i64(3)) % window_interval != 0)
        || (window_interval_kind == IntervalKind::Microsecond && (3600 * common::exp10_i64(6)) % window_interval != 0)
        || (window_interval_kind == IntervalKind::Nanosecond && (3600 * common::exp10_i64(9)) % window_interval != 0))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Invalid window interval, one hour must have an integer number of windows in tumble function");
}

void registerTableFunctionTumble(TableFunctionFactory & factory)
{
    factory.registerFunction("tumble", []() -> TableFunctionPtr { return std::make_shared<TableFunctionTumble>("tumble"); });
}
}
}
