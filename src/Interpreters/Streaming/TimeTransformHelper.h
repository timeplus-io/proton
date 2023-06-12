#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/DateLUT.h>
#include <Common/IntervalKind.h>

namespace DB
{
namespace Streaming
{
template <typename DateOrTime, typename Divisor>
inline DateOrTime roundDown(DateOrTime x, Divisor divisor)
{
    static_assert(std::is_integral_v<DateOrTime> && std::is_integral_v<Divisor>);
    assert(divisor > 0);

    if (likely(x >= 0))
        return static_cast<DateOrTime>(x / divisor * divisor);

    /// Integer division for negative numbers rounds them towards zero (up).
    /// We will shift the number so it will be rounded towards -inf (down).
    return static_cast<DateOrTime>((x + 1 - divisor) / divisor * divisor);
}

template <IntervalKind::Kind unit>
struct ToStartOfTransform;

#define TRANSFORM_DATE(INTERVAL_KIND) \
    template <> \
    struct ToStartOfTransform<IntervalKind::INTERVAL_KIND> \
    { \
        static UInt32 execute(UInt32 time_sec, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            auto days = time_zone.toStartOf##INTERVAL_KIND##Interval(time_zone.toDayNum(time_sec), delta); \
            return static_cast<UInt32>(time_zone.fromDayNum(days)); \
        } \
        template <typename Timestamp> \
            requires(std::is_same_v<Timestamp, Int64> || std::is_same_v<Timestamp, DateTime64>) \
        static Timestamp execute(Timestamp t, UInt64 delta, const DateLUTImpl & time_zone, UInt32 scale) \
        { \
            auto scale_multiplier = common::exp10_i64(static_cast<int>(scale)); \
            auto days = time_zone.toStartOf##INTERVAL_KIND##Interval(time_zone.toDayNum(t / scale_multiplier), delta); \
            return time_zone.fromDayNum(days) * scale_multiplier; \
        } \
    };
TRANSFORM_DATE(Year)
TRANSFORM_DATE(Quarter)
TRANSFORM_DATE(Month)
TRANSFORM_DATE(Week)
#undef TRANSFORM_DATE

template <>
struct ToStartOfTransform<IntervalKind::Day>
{
    static UInt32 execute(UInt32 time_sec, UInt64 delta, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfDayInterval(time_zone.toDayNum(time_sec), delta));
    }
    template <typename Timestamp>
        requires(std::is_same_v<Timestamp, Int64> || std::is_same_v<Timestamp, DateTime64>)
    static Timestamp execute(Timestamp t, UInt64 delta, const DateLUTImpl & time_zone, UInt32 scale)
    {
        auto scale_multiplier = common::exp10_i64(static_cast<int>(scale));
        return time_zone.toStartOfDayInterval(time_zone.toDayNum(t / scale_multiplier), delta) * scale_multiplier;
    }
};

#define TRANSFORM_TIME(INTERVAL_KIND) \
    template <> \
    struct ToStartOfTransform<IntervalKind::INTERVAL_KIND> \
    { \
        static UInt32 execute(UInt32 time_sec, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.toStartOf##INTERVAL_KIND##Interval(time_sec, delta); \
        } \
        template <typename Timestamp> \
            requires(std::is_same_v<Timestamp, Int64> || std::is_same_v<Timestamp, DateTime64>) \
        static Timestamp execute(Timestamp t, UInt64 delta, const DateLUTImpl & time_zone, UInt32 scale) \
        { \
            auto scale_multiplier = common::exp10_i64(static_cast<int>(scale)); \
            return time_zone.toStartOf##INTERVAL_KIND##Interval(t / scale_multiplier, delta) * scale_multiplier; \
        } \
    };
TRANSFORM_TIME(Hour)
TRANSFORM_TIME(Minute)
TRANSFORM_TIME(Second)
#undef TRANSFORM_TIME

/// Subsceonds: assume an hour has integer intervals
#define TRANSFORM_TIME(INTERVAL_KIND, INTERVAL_SCALE) \
    template <> \
    struct ToStartOfTransform<IntervalKind::INTERVAL_KIND> \
    { \
        static constexpr auto interval_scale_multiplier = common::exp10_i64(INTERVAL_SCALE); \
        static UInt32 execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            if (likely(interval_scale_multiplier % delta == 0)) \
                return t; \
\
            Int64 start_of_hour = time_zone.toStartOfHour(t); \
            auto start_interval_of_hour = roundDown((t - start_of_hour) * interval_scale_multiplier, delta) / interval_scale_multiplier; \
            return static_cast<UInt32>(start_of_hour + start_interval_of_hour); \
        } \
        template <typename Timestamp> \
            requires(std::is_same_v<Timestamp, Int64> || std::is_same_v<Timestamp, DateTime64>) \
        static Timestamp execute(Timestamp t, UInt64 delta, const DateLUTImpl & time_zone, UInt32 scale) \
        { \
            if (likely(interval_scale_multiplier % delta == 0)) \
            { \
                if (likely(scale >= INTERVAL_SCALE)) \
                    return roundDown(static_cast<Int64>(t), delta * common::exp10_i64(scale - INTERVAL_SCALE)); \
\
                auto multiplier = common::exp10_i64(INTERVAL_SCALE - scale); \
                return roundDown(t * multiplier, delta) / multiplier; \
            } \
\
            Int64 start_of_hour = time_zone.toStartOfHour(t / interval_scale_multiplier) * interval_scale_multiplier; \
            if (likely(scale >= INTERVAL_SCALE)) \
                return start_of_hour + roundDown(t - start_of_hour, delta * common::exp10_i64(scale - INTERVAL_SCALE)); \
\
            auto multiplier = common::exp10_i64(INTERVAL_SCALE - scale); \
            return start_of_hour + roundDown((t - start_of_hour) * multiplier, delta) / multiplier; \
        } \
    };
TRANSFORM_TIME(Millisecond, 3)
TRANSFORM_TIME(Microsecond, 6)
TRANSFORM_TIME(Nanosecond, 9)
#undef TRANSFORM_TIME

template <IntervalKind::Kind unit>
struct AddTime;

#define ADD_DATE(INTERVAL_KIND) \
    template <> \
    struct AddTime<IntervalKind::INTERVAL_KIND> \
    { \
        static inline UInt32 execute(UInt32 time_sec, Int64 delta, const DateLUTImpl & time_zone) \
        { \
            return static_cast<UInt32>(time_zone.add##INTERVAL_KIND##s(time_sec, delta)); \
        } \
        template <typename Timestamp> \
            requires(std::is_same_v<Timestamp, Int64> || std::is_same_v<Timestamp, DateTime64>) \
        static inline Timestamp execute(Timestamp t, Int64 delta, const DateLUTImpl & time_zone, UInt32 scale) \
        { \
            auto scale_multiplier = common::exp10_i64(static_cast<int>(scale)); \
            return time_zone.add##INTERVAL_KIND##s(t / scale_multiplier, delta) * scale_multiplier + (t % scale_multiplier); \
        } \
    };
ADD_DATE(Year)
ADD_DATE(Quarter)
ADD_DATE(Month)
ADD_DATE(Week)
ADD_DATE(Day)
#undef ADD_DATE

#define ADD_TIME(INTERVAL_KIND, INTERVAL) \
    template <> \
    struct AddTime<IntervalKind::INTERVAL_KIND> \
    { \
        static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 time_sec, Int64 delta, const DateLUTImpl &) \
        { \
            return static_cast<UInt32>(time_sec + INTERVAL * delta); \
        } \
        template <typename Timestamp> \
            requires(std::is_same_v<Timestamp, Int64> || std::is_same_v<Timestamp, DateTime64>) \
        static inline NO_SANITIZE_UNDEFINED Timestamp execute(Timestamp t, Int64 delta, const DateLUTImpl &, UInt32 scale) \
        { \
            return t + delta * INTERVAL * common::exp10_i64(static_cast<int>(scale)); \
        } \
    };
ADD_TIME(Hour, 3600)
ADD_TIME(Minute, 60)
ADD_TIME(Second, 1)
#undef ADD_TIME

#define ADD_TIME(INTERVAL_KIND, INTERVAL_SCALE) \
    template <> \
    struct AddTime<IntervalKind::INTERVAL_KIND> \
    { \
        static constexpr auto interval_scale_multiplier = common::exp10_i64(INTERVAL_SCALE); \
        static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 time_sec, Int64 delta, const DateLUTImpl &) \
        { \
            return static_cast<UInt32>(time_sec + delta / interval_scale_multiplier); \
        } \
        template <typename Timestamp> \
            requires(std::is_same_v<Timestamp, Int64> || std::is_same_v<Timestamp, DateTime64>) \
        static inline NO_SANITIZE_UNDEFINED Timestamp execute(Timestamp t, Int64 delta, const DateLUTImpl &, UInt32 scale) \
        { \
            if (likely(scale >= INTERVAL_SCALE)) \
                return t + delta * common::exp10_i64(scale - INTERVAL_SCALE); \
            else \
                return t + delta / common::exp10_i64(INTERVAL_SCALE - scale); \
        } \
    };
ADD_TIME(Millisecond, 3)
ADD_TIME(Microsecond, 6)
ADD_TIME(Nanosecond, 9)
#undef ADD_TIME

}
}
