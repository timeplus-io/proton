#include "TableFunctionHop.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
extern const int BAD_ARGUMENTS;
}

namespace Streaming
{
TableFunctionHop::TableFunctionHop(const String & name_) : TableFunctionWindow(name_)
{
}

void TableFunctionHop::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    doParseArguments(func_ast, context, HOP_HELP_MESSAGE);
}

ASTs TableFunctionHop::checkAndExtractArguments(ASTFunction * node) const
{
    /// hop(table, [timestamp_column], hop_interval, hop_win_interval, [timezone])
    return checkAndExtractHopArguments(node);
}

void TableFunctionHop::postArgs(ASTs & args) const
{
    //// [timezone]
    /// Prune the empty timezone if user doesn't specify one
    if (!args.back())
        args.pop_back();

    /// Try do the same scale conversion of hop_interval and win_interval
    convertToSameKindIntervalAST(
        BaseScaleInterval::toBaseScale(extractInterval(args[1]->as<ASTFunction>())),
        BaseScaleInterval::toBaseScale(extractInterval(args[2]->as<ASTFunction>())),
        args[1],
        args[2]);
}

String TableFunctionHop::functionNamePrefix() const
{
    return ProtonConsts::HOP_FUNC_NAME + "(";
}

DataTypePtr TableFunctionHop::getElementType(const DataTypeTuple * tuple) const
{
    DataTypePtr element_type = tuple->getElements()[0];
    assert(isArray(element_type));

    auto array_type = checkAndGetDataType<DataTypeArray>(element_type.get());
    return array_type->getNestedType();
}

void TableFunctionHop::validateWindow(FunctionDescriptionPtr desc) const
{
    assert(desc && desc->type == WindowType::HOP);
    UInt32 time_scale = 0;
    if (auto * datetime64 = checkAndGetDataType<DataTypeDateTime64>(desc->argument_types[0].get()))
        time_scale = datetime64->getScale();

    auto & args = desc->func_ast->as<ASTFunction &>().arguments->children;
    assert(args.size() >= 3);
    auto [hop_interval, hop_interval_kind] = extractInterval(args[1]->as<ASTFunction>());
    auto [window_interval, window_interval_kind] = extractInterval(args[2]->as<ASTFunction>());
    if (hop_interval_kind != window_interval_kind)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal type of window and hop column of function hop must be same");

    if (hop_interval > window_interval)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Slide size shall be less than or equal to window size in hop function");

    auto hop_window_scale = getAutoScaleByInterval(hop_interval, hop_interval_kind);
    if (hop_window_scale > time_scale)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid slide interval, the slide scale '{}' cannot exceed the event time scale '{}' in hop function",
            hop_window_scale,
            time_scale);

    if ((hop_interval_kind == IntervalKind::Millisecond && (3600 * common::exp10_i64(3)) % hop_interval != 0)
        || (hop_interval_kind == IntervalKind::Microsecond && (3600 * common::exp10_i64(6)) % hop_interval != 0)
        || (hop_interval_kind == IntervalKind::Nanosecond && (3600 * common::exp10_i64(9)) % hop_interval != 0))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Invalid slide interval, one hour must have an integer number of slides in hop function");
}

void registerTableFunctionHop(TableFunctionFactory & factory)
{
    factory.registerFunction("hop", []() -> TableFunctionPtr { return std::make_shared<TableFunctionHop>("hop"); });
}
}
}
