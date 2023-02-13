#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <Common/DateLUTImpl.h>
#include <Common/IntervalKind.h>

namespace DB
{

class ASTFunction;

namespace Streaming
{
enum class WindowType
{
    NONE,
    HOP,
    TUMBLE,
    SESSION
};

const String TUMBLE_HELP_MESSAGE = "Function 'tumble' requires from 2 to 4 parameters: "
                                   "<name of the table>, [timestamp column], <tumble window size>, [time zone]";
const String HOP_HELP_MESSAGE = "Function 'hop' requires from 3 to 5 parameters: "
                                "<name of the table>, [timestamp column], <hop interval size>, <hop window size>, [time zone]";
const String SESSION_HELP_MESSAGE = "Function 'session' requires at least 2 parameters: "
                                    "<name of the stream>, [timestamp column], <timeout interval>, [max emit interval], [session range comparision] | [start_prediction, end_prediction]";


bool isTableFunctionTumble(const ASTFunction * ast);
bool isTableFunctionHop(const ASTFunction * ast);
bool isTableFunctionTable(const ASTFunction * ast);
bool isTableFunctionSession(const ASTFunction * ast);

/// Note: the extracted arguments is whole (include omitted parameters represented by an empty ASTPtr)
/// for example:
/// tumble(table, interval 5 second)
///   v
/// [table, timestamp(nullptr), win_interval, timezone(nullptr)]
ASTs checkAndExtractTumbleArguments(const ASTFunction * func_ast);
ASTs checkAndExtractHopArguments(const ASTFunction * func_ast);
ASTs checkAndExtractSessionArguments(const ASTFunction * func_ast);

void checkIntervalAST(const ASTPtr & ast, const String & msg = "Invalid interval");
void extractInterval(const ASTFunction * ast, Int64 & interval, IntervalKind::Kind & kind);
std::pair<Int64, IntervalKind> extractInterval(const ASTFunction * ast);

Int64 addTime(Int64 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone);
Int64 addTime(Int64 dt, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone, Int64 time_scale);

WindowType toWindowType(const String & func_name);

/// BaseScaleInterval util class converts interval in different scale to a common base scale.
/// BaseScale-1: Second    Range: Second, Minute, Hour, Day, Week
/// BaseScale-2: Month     Range: Month, Quarter, Year
/// example: '1h' -> '3600s'   '1y' -> '12M'
class BaseScaleInterval
{
public:
    static constexpr IntervalKind::Kind SCALE_SECOND = IntervalKind::Second;
    static constexpr IntervalKind::Kind SCALE_MONTH = IntervalKind::Month;

    Int64 num_units = 0;
    IntervalKind::Kind scale = SCALE_SECOND;
    IntervalKind::Kind src_kind = SCALE_SECOND;

    BaseScaleInterval() = default;

    static constexpr BaseScaleInterval toBaseScale(Int64 num_units, IntervalKind::Kind kind)
    {
        switch (kind)
        {
            /// FIXME, TIME
            case IntervalKind::Nanosecond:
            case IntervalKind::Microsecond:
            case IntervalKind::Millisecond:
                return BaseScaleInterval{static_cast<Int64>(std::ceil(num_units * IntervalKind(kind).toSeconds())), SCALE_SECOND, kind};
            /// Based on SCALE_SECOND
            case IntervalKind::Second:
                return BaseScaleInterval{num_units, SCALE_SECOND, kind};
            case IntervalKind::Minute:
                return BaseScaleInterval{num_units * 60, SCALE_SECOND, kind};
            case IntervalKind::Hour:
                return BaseScaleInterval{num_units * 3600, SCALE_SECOND, kind};
            case IntervalKind::Day:
                return BaseScaleInterval{num_units * 86400, SCALE_SECOND, kind};
            case IntervalKind::Week:
                return BaseScaleInterval{num_units * 604800, SCALE_SECOND, kind};
            /// Based on SCALE_MONTH
            case IntervalKind::Month:
                return BaseScaleInterval{num_units, SCALE_MONTH, kind};
            case IntervalKind::Quarter:
                return BaseScaleInterval{num_units * 3, SCALE_MONTH, kind};
            case IntervalKind::Year:
                return BaseScaleInterval{num_units * 12, SCALE_MONTH, kind};
        }
        UNREACHABLE();
    }

    static BaseScaleInterval toBaseScale(const std::pair<Int64, IntervalKind> & interval)
    {
        return toBaseScale(interval.first, interval.second);
    }

    std::pair<Int64, IntervalKind> toIntervalKind(IntervalKind::Kind to_kind) const;

    BaseScaleInterval & operator+(const BaseScaleInterval & bs)
    {
        assert(scale == bs.scale);
        num_units += bs.num_units;
        return *this;
    }

    BaseScaleInterval & operator-(const BaseScaleInterval & bs)
    {
        assert(scale == bs.scale);
        num_units -= bs.num_units;
        return *this;
    }

    Int64 operator/(const BaseScaleInterval & bs) const
    {
        assert(scale == bs.scale);
        assert(bs.num_units != 0);
        return num_units / bs.num_units;
    }

    BaseScaleInterval operator/(Int64 num) const
    {
        assert(num != 0);
        return {num_units / num, scale, src_kind};
    }

    String toString() const;

protected:
    constexpr BaseScaleInterval(Int64 num_units_, IntervalKind::Kind scale_, IntervalKind::Kind src_kind_)
        : num_units(num_units_), scale(scale_), src_kind(src_kind_)
    {
    }
};
using BasedScaleIntervalPtr = std::shared_ptr<BaseScaleInterval>;

ASTPtr makeASTInterval(Int64 num_units, IntervalKind kind);
ASTPtr makeASTInterval(const std::pair<Int64, IntervalKind> & interval);

void convertToSameKindIntervalAST(const BaseScaleInterval & bs1, const BaseScaleInterval & bs2, ASTPtr & ast1, ASTPtr & ast2);
}
}
