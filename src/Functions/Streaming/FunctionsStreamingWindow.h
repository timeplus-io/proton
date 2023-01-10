#pragma once

#include <Common/DateLUT.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
/** Streaming Window functions:
  *
  * __tumble(time_attr, interval [, alignment, [, timezone]])
  *
  * __hop(time_attr, hop_interval, window_interval [, alignment, [, timezone]])
  *
  */
enum WindowFunctionName
{
    TUMBLE,
    HOP,
    SESSION,
};

template <IntervalKind::Kind unit>
struct ToStartOfTransform;

#define TRANSFORM_DATE(INTERVAL_KIND) \
    template <> \
    struct ToStartOfTransform<IntervalKind::INTERVAL_KIND> \
    { \
        static auto execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.toStartOf##INTERVAL_KIND##Interval(time_zone.toDayNum(t), delta); \
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
    static UInt32 execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfDayInterval(time_zone.toDayNum(t), delta));
    }
};

#define TRANSFORM_TIME(INTERVAL_KIND) \
    template <> \
    struct ToStartOfTransform<IntervalKind::INTERVAL_KIND> \
    { \
        static UInt32 execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.toStartOf##INTERVAL_KIND##Interval(t, delta); \
        } \
    };
TRANSFORM_TIME(Hour)
TRANSFORM_TIME(Minute)
TRANSFORM_TIME(Second)
#undef TRANSFORM_DATE

template <IntervalKind::Kind unit>
struct AddTime;

#define ADD_DATE(INTERVAL_KIND) \
    template <> \
    struct AddTime<IntervalKind::INTERVAL_KIND> \
    { \
        static inline auto execute(UInt16 d, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.add##INTERVAL_KIND##s(ExtendedDayNum(d), delta); \
        } \
    };
ADD_DATE(Year)
ADD_DATE(Quarter)
ADD_DATE(Month)
#undef ADD_DATE

template <>
struct AddTime<IntervalKind::Week>
{
    static inline NO_SANITIZE_UNDEFINED ExtendedDayNum execute(UInt16 d, UInt64 delta, const DateLUTImpl &) { return ExtendedDayNum(static_cast<ExtendedDayNum::UnderlyingType>(d + 7 * delta)); }
};

#define ADD_TIME(INTERVAL_KIND, INTERVAL) \
    template <> \
    struct AddTime<IntervalKind::INTERVAL_KIND> \
    { \
        static inline NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &) { return static_cast<UInt32>(t + INTERVAL * delta); } \
    };
ADD_TIME(Day, 86400)
ADD_TIME(Hour, 3600)
ADD_TIME(Minute, 60)
ADD_TIME(Second, 1)
#undef ADD_TIME

template <WindowFunctionName type>
struct WindowImpl
{
    static constexpr auto name = "unknown";

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name);

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name);
};

template <WindowFunctionName type>
class FunctionWindow : public IFunction
{
public:
    static constexpr auto name = WindowImpl<type>::name;
    static constexpr auto external_name = WindowImpl<type>::external_name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionWindow>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNothing() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override;
};

using FunctionTumble = FunctionWindow<TUMBLE>;
using FunctionHop = FunctionWindow<HOP>;
using FunctionSession = FunctionWindow<SESSION>;
}
