#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnsDateTime.h>
#include <Interpreters/Streaming/SessionInfo.h>
#include <Interpreters/Streaming/TableFunctionDescription_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <Common/DateLUTImpl.h>
#include <Common/IntervalKind.h>
#include <Common/TypePromotion.h>

namespace DB
{

class ASTFunction;
class Chunk;

namespace Streaming
{
#define DISPATCH_FOR_WINDOW_INTERVAL(interval_kind, M) \
    do \
    { \
        switch (interval_kind) \
        { \
            case IntervalKind::Nanosecond: { \
                M(IntervalKind::Nanosecond); \
                break; \
            } \
            case IntervalKind::Microsecond: { \
                M(IntervalKind::Microsecond); \
                break; \
            } \
            case IntervalKind::Millisecond: { \
                M(IntervalKind::Millisecond); \
                break; \
            } \
            case IntervalKind::Second: { \
                M(IntervalKind::Second); \
                break; \
            } \
            case IntervalKind::Minute: { \
                M(IntervalKind::Minute); \
                break; \
            } \
            case IntervalKind::Hour: { \
                M(IntervalKind::Hour); \
                break; \
            } \
            case IntervalKind::Day: { \
                M(IntervalKind::Day); \
                break; \
            } \
            case IntervalKind::Week: { \
                M(IntervalKind::Week); \
                break; \
            } \
            case IntervalKind::Month: { \
                M(IntervalKind::Month); \
                break; \
            } \
            case IntervalKind::Quarter: { \
                M(IntervalKind::Quarter); \
                break; \
            } \
            case IntervalKind::Year: { \
                M(IntervalKind::Year); \
                break; \
            } \
        } \
    } while (0);

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
                                    "<name of the stream>, [timestamp column], <timeout interval>, [max session time], [session range comparision] | [start_prediction, end_prediction]";


bool isTableFunctionTumble(const ASTFunction * ast);
bool isTableFunctionHop(const ASTFunction * ast);
bool isTableFunctionTable(const ASTFunction * ast);
bool isTableFunctionSession(const ASTFunction * ast);
bool isTableFunctionChangelog(const ASTFunction * ast);

/// Note: the extracted arguments is whole (include omitted parameters represented by an empty ASTPtr)
/// for example:
/// tumble(table, interval 5 second)
///   v
/// [table, timestamp(nullptr), win_interval, timezone(nullptr)]
ASTs checkAndExtractTumbleArguments(const ASTFunction * func_ast);
ASTs checkAndExtractHopArguments(const ASTFunction * func_ast);
ASTs checkAndExtractSessionArguments(const ASTFunction * func_ast);

struct WindowInterval
{
    Int64 interval = 0;
    IntervalKind::Kind unit = IntervalKind::Second;

    operator bool() const { return interval != 0; }
};

void checkIntervalAST(const ASTPtr & ast, const String & msg = "Invalid interval");
void extractInterval(const ASTFunction * ast, Int64 & interval, IntervalKind::Kind & kind);
WindowInterval extractInterval(const ASTFunction * ast);
WindowInterval extractInterval(const ColumnWithTypeAndName & interval_column);

UInt32 toStartTime(UInt32 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone);
Int64 toStartTime(Int64 dt, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone, UInt32 time_scale);

UInt32 addTime(UInt32 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone);
Int64 addTime(Int64 dt, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone, UInt32 time_scale);

WindowType toWindowType(const String & func_name);

/// BaseScaleInterval util class converts interval in different scale to a common base scale.
/// BaseScale-1: Nanosecond     Range: Nanosecond, Microsecond, Millisecond, Second, Minute, Hour, Day, Week
/// BaseScale-2: Month          Range: Month, Quarter, Year
/// example: '1m' -> '60000000000ns'   '1y' -> '12M'
class BaseScaleInterval
{
public:
    static constexpr IntervalKind::Kind SCALE_NANOSECOND = IntervalKind::Nanosecond;
    static constexpr IntervalKind::Kind SCALE_MONTH = IntervalKind::Month;

    Int64 num_units = 0;
    IntervalKind::Kind scale = SCALE_NANOSECOND;
    IntervalKind::Kind src_kind = SCALE_NANOSECOND;

    BaseScaleInterval() = default;

    static constexpr BaseScaleInterval toBaseScale(Int64 num_units, IntervalKind::Kind kind)
    {
        switch (kind)
        {
            /// FIXME: check overflow ?
            /// Based on SCALE_NANOSECOND
            case IntervalKind::Nanosecond:
                return BaseScaleInterval{num_units, SCALE_NANOSECOND, kind};
            case IntervalKind::Microsecond:
                return BaseScaleInterval{num_units * 1'000, SCALE_NANOSECOND, kind};
            case IntervalKind::Millisecond:
                return BaseScaleInterval{num_units * 1'000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Second:
                return BaseScaleInterval{num_units * 1'000000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Minute:
                return BaseScaleInterval{num_units * 60'000000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Hour:
                return BaseScaleInterval{num_units * 3600'000000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Day:
                return BaseScaleInterval{num_units * 86400'000000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Week:
                return BaseScaleInterval{num_units * 604800'000000000, SCALE_NANOSECOND, kind};
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

    static BaseScaleInterval toBaseScale(const WindowInterval & interval)
    {
        return toBaseScale(interval.interval, interval.unit);
    }

    Int64 toIntervalKind(IntervalKind::Kind to_kind) const;

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
ASTPtr makeASTInterval(const WindowInterval & interval);

void convertToSameKindIntervalAST(const BaseScaleInterval & bs1, const BaseScaleInterval & bs2, ASTPtr & ast1, ASTPtr & ast2);

UInt32 getAutoScaleByInterval(Int64 num_units, IntervalKind kind);

/// Window Params
struct WindowParams;
using WindowParamsPtr = std::shared_ptr<WindowParams>;
struct WindowParams : public TypePromotion<WindowParams>
{
    WindowType type;
    TableFunctionDescriptionPtr desc;

    String time_col_name;
    bool time_col_is_datetime64; /// DateTime64 or DateTime
    UInt32 time_scale;
    const DateLUTImpl * time_zone;

    static WindowParamsPtr create(const TableFunctionDescriptionPtr & desc);

protected:
    WindowParams(TableFunctionDescriptionPtr window_desc);
    virtual ~WindowParams() = default;
};

/// __tumble(time_expr, win_interval, [timezone])
struct TumbleWindowParams : WindowParams
{
    Int64 window_interval = 0;
    IntervalKind::Kind interval_kind = IntervalKind::Second;

    TumbleWindowParams(TableFunctionDescriptionPtr window_desc);
};

/// __hop(time_expr, hop_interval, win_interval, [timezone])
struct HopWindowParams : WindowParams
{
    Int64 window_interval = 0;
    Int64 slide_interval = 0;
    /// Base interval is the greatest common divisor of window_interval and slide_interval
    /// By splitting a window into multiple base-windows, a base-window can be shared by multiple windows
    Int64 gcd_interval = 0;
    IntervalKind::Kind interval_kind = IntervalKind::Second;

    HopWindowParams(TableFunctionDescriptionPtr window_desc);
};

/// __session(timestamp_expr, timeout_interval, max_emit_interval, start_cond, start_with_inclusion, end_cond, end_with_inclusion)
struct SessionWindowParams : WindowParams
{
    Int64 session_timeout;
    Int64 max_session_size;
    IntervalKind::Kind interval_kind;
    bool start_with_inclusion;
    bool end_with_inclusion;

    /// So far, only for session window, we evaluate the watermark and window for the events in Aggregate Transform
    /// For other windows, we assigned the watermark in window assignment step.
    bool pushdown_window_assignment = true;

    SessionWindowParams(TableFunctionDescriptionPtr window_desc);
};

struct Window
{
    Int64 start;
    Int64 end;

    bool isValid() const { return end > start; }
    operator bool() const { return isValid(); }
};

struct WindowWithBuckets
{
    Window window;
    /// The time buckets where the current window data is located in window aggregation
    /// For hop window, there are multiple base time buckets
    std::vector<Int64> buckets;
};
using WindowsWithBuckets = std::vector<WindowWithBuckets>;

void assignWindow(
    Columns & columns, const WindowInterval & interval, size_t time_col_pos, bool time_col_is_datetime64, const DateLUTImpl & time_zone);
void reassignWindow(
    Chunk & chunk, const Window & window, bool time_col_is_datetime64, std::optional<size_t> start_pos, std::optional<size_t> end_pos);
}
}
