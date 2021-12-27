#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/DateLUTImpl.h>
#include <base/types.h>
#include <Common/IntervalKind.h>

namespace DB
{
class ASTFunction;

const String TUMBLE_HELP_MESSAGE = "Table function 'tumble' requires from 2 to 4 parameters: "
                                   "<name of the table>, [timestamp column], <tumble window size>, [time zone]";
const String HOP_HELP_MESSAGE = "Table function 'hop' requires from 3 to 5 parameters: "
                                "<name of the table>, [timestamp column], <hop interval size>, <hop window size>, [time zone]";

bool isTableFunctionTumble(const ASTFunction * ast);
bool isTableFunctionHop(const ASTFunction * ast);

/// Note: the extracted arguments is whole (include omitted parameters represented by an empty ASTPtr)
/// for example:
/// tumble(table, interval 5 second)
///   v
/// [table, timestamp(nullptr), win_interval, timezone(nullptr)]
ASTs checkAndExtractTumbleArguments(const ASTFunction * func_ast);
ASTs checkAndExtractHopArguments(const ASTFunction * func_ast);

void checkIntervalAST(const ASTPtr & ast, const String & msg = "Invalid interval");
void extractInterval(const ASTFunction * ast, Int64 & interval, IntervalKind::Kind & kind);
void extractInterval(const ASTFunction * ast, Int64 & interval, IntervalKind & kind);
std::pair<Int64, IntervalKind> extractInterval(const ASTFunction * ast);

Int64 addTime(Int64 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone);

/// [seconds -> auto]
/// example: secondsToInterval(3600)  - convert 3600s to 1h
std::pair<Int64, IntervalKind> secondsToInterval(Int64 time_sec);
/// [seconds -> to_kind]
/// example: secondsToInterval(3600, Minute)  - convert 3600s to 60m
std::pair<Int64, IntervalKind> secondsToInterval(Int64 time_sec, IntervalKind::Kind to_kind);
/// [kind -> seconds]
/// example: intervalToSeconds(60, Minute)  - convert 60m to 3600s
Int64 intervalToSeconds(Int64 num_units, IntervalKind::Kind kind);

ASTPtr makeASTInterval(Int64 num_units, IntervalKind kind);
}
