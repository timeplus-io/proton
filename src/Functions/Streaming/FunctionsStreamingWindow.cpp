#include "FunctionsStreamingWindow.h"

#include <Common/ProtonCommon.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Processors/Transforms/Streaming/HopHelper.h>

#include <numeric>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    using ColumnDateTime64 = ColumnDecimal<DateTime64>;
    using ColumnDateTime32 = ColumnVector<UInt32>;

    void checkFirstArgument(const ColumnWithTypeAndName & argument, const String & function_name)
    {
        if (!isDateTime64(argument.type) && !isDateTime(argument.type))
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name + ". Should be a date with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void checkIntervalArgument(
        const ColumnWithTypeAndName & argument,
        const String & function_name,
        IntervalKind & interval_kind,
        Int64 & interval)
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(argument.type.get());
        if (!interval_type)
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name
                    + ". Should be an interval of time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(argument.column.get());
        if (!interval_column_const_int64)
            throw Exception("Illegal column " + argument.name + " of argument of function " + function_name, ErrorCodes::ILLEGAL_COLUMN);

        interval = interval_column_const_int64->getValue<Int64>();
        if (interval <= 0)
            throw Exception(
                "Value for column " + argument.name + " of function " + function_name + " must be positive", ErrorCodes::BAD_ARGUMENTS);

        interval_kind = interval_type->getKind();
    }

    void checkIntervalArgument(const ColumnWithTypeAndName & argument, const String & function_name)
    {
        IntervalKind interval_kind;
        Int64 interval;
        checkIntervalArgument(argument, function_name, interval_kind, interval);
    }

    void checkTimeZoneArgument(const ColumnWithTypeAndName & argument, const String & function_name)
    {
        if (!isString(argument.type))
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name
                    + ". This argument is optional and must be a constant string with timezone name",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    DataTypePtr getReturnDataType(const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num_check)
    {
        size_t time_zone_arg_num = 0;
        if (time_zone_arg_num_check != 0 && static_cast<size_t>(arguments.size()) == time_zone_arg_num_check + 1
            && isString(arguments[time_zone_arg_num_check].type))
            /// check pos `time_zone_arg_num_check` to see if it is a string, if yes, try to treat it as time zone
            time_zone_arg_num = time_zone_arg_num_check;

        const auto & timezone = extractTimeZoneNameFromFunctionArguments(arguments, time_zone_arg_num, 0);

        if (isDateTime(arguments[0].type))
        {
            return std::make_shared<DataTypeDateTime>(timezone);
        }
        else
        {
            const auto * datetime64_type = checkAndGetDataType<DataTypeDateTime64>(arguments[0].type.get());
            return std::make_shared<DataTypeDateTime64>(datetime64_type->getScale(), timezone);
        }
    }

    /// Calculate window start / end for time column in num_units
    /// @return (window_start, window_end) tuple
    template <IntervalKind::Kind unit, typename TimeColumnType>
    ColumnPtr getWindowStartAndEndFor(const TimeColumnType & time_column, UInt64 num_units, const DateLUTImpl & time_zone)
    {
        constexpr bool time_col_is_datetime64 = std::is_same_v<TimeColumnType, ColumnDateTime64>;
    
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();
        typename TimeColumnType::MutablePtr start, end;
        if constexpr (time_col_is_datetime64)
        {
            start = TimeColumnType::create(size, time_column.getScale());
            end = TimeColumnType::create(size, time_column.getScale());
        }
        else
        {
            start = TimeColumnType::create(size);
            end = TimeColumnType::create(size);
        }

        auto & start_data = start->getData();
        auto & end_data = end->getData();

        for (size_t i = 0; i != size; ++i)
        {
            if constexpr (time_col_is_datetime64)
            {
                start_data[i] = ToStartOfTransform<unit>::execute(time_data[i], num_units, time_zone, time_column.getScale());
                end_data[i] = AddTime<unit>::execute(start_data[i], num_units, time_zone, time_column.getScale());
            }
            else
            {
                start_data[i] = ToStartOfTransform<unit>::execute(time_data[i], num_units, time_zone);
                end_data[i] = AddTime<unit>::execute(start_data[i], num_units, time_zone);
            }
        }

        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }

    template <typename TimeColumnType>
    ColumnPtr dispatchForColumnsDateTime(
        const TimeColumnType & time_column, const Streaming::WindowInterval & interval, const DateLUTImpl & time_zone)
    {
        switch (interval.unit)
        {
            case IntervalKind::Nanosecond:
                return getWindowStartAndEndFor<IntervalKind::Nanosecond>(time_column, interval.interval, time_zone);
            case IntervalKind::Microsecond:
                return getWindowStartAndEndFor<IntervalKind::Microsecond>(time_column, interval.interval, time_zone);
            case IntervalKind::Millisecond:
                return getWindowStartAndEndFor<IntervalKind::Millisecond>(time_column, interval.interval, time_zone);
            case IntervalKind::Second:
                return getWindowStartAndEndFor<IntervalKind::Second>(time_column, interval.interval, time_zone);
            case IntervalKind::Minute:
                return getWindowStartAndEndFor<IntervalKind::Minute>(time_column, interval.interval, time_zone);
            case IntervalKind::Hour:
                return getWindowStartAndEndFor<IntervalKind::Hour>(time_column, interval.interval, time_zone);
            case IntervalKind::Day:
                return getWindowStartAndEndFor<IntervalKind::Day>(time_column, interval.interval, time_zone);
            case IntervalKind::Week:
                return getWindowStartAndEndFor<IntervalKind::Week>(time_column, interval.interval, time_zone);
            case IntervalKind::Month:
                return getWindowStartAndEndFor<IntervalKind::Month>(time_column, interval.interval, time_zone);
            case IntervalKind::Quarter:
                return getWindowStartAndEndFor<IntervalKind::Quarter>(time_column, interval.interval, time_zone);
            case IntervalKind::Year:
                return getWindowStartAndEndFor<IntervalKind::Year>(time_column, interval.interval, time_zone);
        }
        __builtin_unreachable();
    }
}

template <>
struct WindowImpl<TUMBLE>
{
    static constexpr auto name = "__tumble";
    static constexpr auto external_name = "tumble";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        /// FIXME, alignment
        if (arguments.size() == 2)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name);
        }
        else if (arguments.size() == 3)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name);
            checkTimeZoneArgument(arguments[2], function_name);
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypePtr data_type = getReturnDataType(arguments, 2);
        return std::make_shared<DataTypeTuple>(
            DataTypes{data_type, data_type}, Names{ProtonConsts::STREAMING_WINDOW_START, ProtonConsts::STREAMING_WINDOW_END});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        auto interval = Streaming::extractInterval(arguments[1]);
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        auto time_col_type = WhichDataType(*time_column.type.get());
        if (time_col_type.isDateTime64())
        {
            const auto & time_column_vec = assert_cast<const ColumnDateTime64 &>(*time_column.column.get());
            return dispatchForColumnsDateTime(time_column_vec, interval, time_zone);
        }
        else if (time_col_type.isDateTime())
        {
            const auto & time_column_vec = assert_cast<const ColumnDateTime32 &>(*time_column.column.get());
            return dispatchForColumnsDateTime(time_column_vec, interval, time_zone);
        }
        else
        {
            throw Exception(
                "Illegal column " + time_column.name + " argument of function " + function_name + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }
};

template <>
struct WindowImpl<HOP>
{
    static constexpr auto name = "__hop";
    static constexpr auto external_name = "hop";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        IntervalKind slide_kind;
        IntervalKind window_kind;
        Int64 slide_size = 0;
        Int64 window_size = 0;

        if (arguments.size() == 3)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, slide_kind, slide_size);
            checkIntervalArgument(arguments[2], function_name, window_kind, window_size);
        }
        else if (arguments.size() == 4)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, slide_kind, slide_size);
            checkIntervalArgument(arguments[2], function_name, window_kind, window_size);
            checkTimeZoneArgument(arguments[3], function_name);
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 3 or 4",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (slide_kind != window_kind)
            throw Exception(
                "Illegal type of window and hop column of function " + function_name + ", must be same", ErrorCodes::ILLEGAL_COLUMN);

        if (slide_size > window_size)
            throw Exception("Slide size shall be less than or equal to window size in hop function", ErrorCodes::BAD_ARGUMENTS);

        size_t time_zone_arg_num_check = arguments.size() == 4 ? 3 : 0;
        DataTypePtr data_type = getReturnDataType(arguments, time_zone_arg_num_check);
        return std::make_shared<DataTypeTuple>(
            DataTypes{data_type, data_type}, Names{ProtonConsts::STREAMING_WINDOW_START, ProtonConsts::STREAMING_WINDOW_END});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        auto gcd_interval = Streaming::HopHelper::gcdWindowInterval(arguments[1], arguments[2]);
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);
        auto time_col_type = WhichDataType(*time_column.type.get());
        if (time_col_type.isDateTime64())
        {
            const auto & time_column_vec = assert_cast<const ColumnDateTime64 &>(*time_column.column.get());
            return dispatchForColumnsDateTime(time_column_vec, gcd_interval, time_zone);
        }
        else if (time_col_type.isDateTime())
        {
            const auto & time_column_vec = assert_cast<const ColumnDateTime32 &>(*time_column.column.get());
            return dispatchForColumnsDateTime(time_column_vec, gcd_interval, time_zone);
        }
        else
        {
            throw Exception(
                "Illegal column " + time_column.name + " argument of function " + function_name + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }
};

template <>
struct WindowImpl<SESSION>
{
    static constexpr auto name = "__session";
    static constexpr auto external_name = "session";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        /// __session(timestamp_expr, timeout_interval, max_session_size, start_cond, start_with_inclusion, end_cond, end_with_inclusion)
        if (arguments.size() == 7)
        {
            checkFirstArgument(arguments[0], external_name);
            checkIntervalArgument(arguments[1], external_name);
            checkIntervalArgument(arguments[2], external_name);
        }
        else
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function session doesn't match: passed {}, should be 7",
                arguments.size());

        return std::make_shared<DataTypeTuple>(
            DataTypes{arguments[0].type, arguments[0].type, arguments[3].type, arguments[5].type},
            Names{
                ProtonConsts::STREAMING_WINDOW_START,
                ProtonConsts::STREAMING_WINDOW_END,
                ProtonConsts::STREAMING_SESSION_START,
                ProtonConsts::STREAMING_SESSION_END});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & /*function_name*/)
    {
        /// return tuple(window_start, window_end, __tp_session_start, __tp_session_end)
        assert(arguments.size() == 7);
        MutableColumns result;
        auto time_column = IColumn::mutate(arguments[0].column->convertToFullColumnIfConst());
        result.emplace_back(time_column->cloneResized(time_column->size())); /// No calculate window start here
        result.emplace_back(std::move(time_column)); /// No calculate window end here
        result.emplace_back(IColumn::mutate(arguments[3].column->convertToFullColumnIfConst()));
        result.emplace_back(IColumn::mutate(arguments[5].column->convertToFullColumnIfConst()));
        return ColumnTuple::create(std::move(result));
    }
};

template <>
bool FunctionWindow<SESSION>::useDefaultImplementationForNothing() const
{
    return false;
}

template <>
ColumnNumbers FunctionWindow<SESSION>::getArgumentsThatAreAlwaysConstant() const
{
    return {1};
}

template <WindowFunctionName type>
DataTypePtr FunctionWindow<type>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    return WindowImpl<type>::getReturnType(arguments, external_name);
}

template <WindowFunctionName type>
ColumnPtr FunctionWindow<type>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const
{
    return WindowImpl<type>::dispatchForColumns(arguments, external_name);
}

REGISTER_FUNCTION(StreamingWindow)
{
    factory.registerFunction<FunctionTumble>();
    factory.registerFunction<FunctionHop>();
    factory.registerFunction<FunctionSession>();
}
}
