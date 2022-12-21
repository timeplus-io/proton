#include "FunctionsStreamingWindow.h"

#include <Common/ProtonCommon.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

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
    using ColumnDate = ColumnVector<UInt16>;

    std::tuple<IntervalKind::Kind, Int64> intervalKindAndUnits(const ColumnWithTypeAndName & interval_column)
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
        assert(interval_type);
        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
        assert(interval_column_const_int64);
        return {interval_type->getKind(), interval_column_const_int64->getValue<Int64>()};
    }

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
        Int64 & interval,
        bool & result_type_is_date)
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
        result_type_is_date = (interval_kind == IntervalKind::Year) || (interval_kind == IntervalKind::Quarter)
            || (interval_kind == IntervalKind::Month) || (interval_kind == IntervalKind::Week);
    }

    void checkIntervalArgument(const ColumnWithTypeAndName & argument, const String & function_name, bool & result_type_is_date)
    {
        IntervalKind interval_kind;
        Int64 interval;
        checkIntervalArgument(argument, function_name, interval_kind, interval, result_type_is_date);
    }

    void checkTimeZoneArgument(const ColumnWithTypeAndName & argument, const String & function_name)
    {
        if (!isString(argument.type))
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name
                    + ". This argument is optional and must be a constant string with timezone name",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    DataTypePtr getReturnDataType(bool result_type_is_date, const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num_check)
    {
        if (result_type_is_date)
        {
            return std::make_shared<DataTypeDate>();
        }

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
}

template <>
struct WindowImpl<TUMBLE>
{
    static constexpr auto name = "__tumble";
    static constexpr auto external_name = "tumble";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        bool result_type_is_date;

        /// FIXME, alignment
        if (arguments.size() == 2)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, result_type_is_date);
        }
        else if (arguments.size() == 3)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, result_type_is_date);
            checkTimeZoneArgument(arguments[2], function_name);
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        DataTypePtr data_type = getReturnDataType(result_type_is_date, arguments, 2);
        return std::make_shared<DataTypeTuple>(DataTypes{data_type, data_type});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & from_datatype = *time_column.type.get();
        if (isDateTime64(from_datatype))
        {
            return dispatchForColumnsDateTime64(arguments);
        }
        else if (isDateTime(from_datatype))
        {
            return dispatchForColumnsDateTime32(arguments);
        }
        else
        {
            throw Exception(
                "Illegal column " + time_column.name + " argument of function " + function_name + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    static ColumnPtr dispatchForColumnsDateTime64(const ColumnsWithTypeAndName & arguments)
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);

        auto interval = intervalKindAndUnits(interval_column);

        switch (std::get<0>(interval))
        {
            /// FIXME, TIME
            case IntervalKind::Nanosecond:
            case IntervalKind::Microsecond:
            case IntervalKind::Millisecond:
            case IntervalKind::Second:
                return executeTumbleDateTime64<IntervalKind::Second>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Minute:
                return executeTumbleDateTime64<IntervalKind::Minute>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Hour:
                return executeTumbleDateTime64<IntervalKind::Hour>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Day:
                return executeTumbleDateTime64<IntervalKind::Day>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Week:
                return executeTumbleDate<IntervalKind::Week>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Month:
                return executeTumbleDate<IntervalKind::Month>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Quarter:
                return executeTumbleDate<IntervalKind::Quarter>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Year:
                return executeTumbleDate<IntervalKind::Year>(*time_column_vec, std::get<1>(interval), time_zone);
        }
        __builtin_unreachable();
    }

    template <IntervalKind::Kind unit>
    static ColumnPtr executeTumbleDateTime64(const ColumnDateTime64 & time_column, UInt64 num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();

        auto scale = time_column.getScale();
        auto start = ColumnDateTime64::create(size, scale);
        auto end = ColumnDateTime64::create(size, scale);

        auto & start_data = start->getData();
        auto & end_data = end->getData();

        for (size_t i = 0; i != size; ++i)
        {
            auto components = DecimalUtils::split(time_data[i], scale);
            components.fractional = 0;

            components.whole = ToStartOfTransform<unit>::execute(components.whole, num_units, time_zone);
            start_data[i] = DecimalUtils::decimalFromComponents(components, scale);

            components.whole = AddTime<unit>::execute(components.whole, num_units, time_zone);
            end_data[i] = DecimalUtils::decimalFromComponents(components, scale);
        }
        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }

    template <IntervalKind::Kind unit>
    static ColumnPtr executeTumbleDate(const ColumnDateTime64 & time_column, UInt64 num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();

        auto start = ColumnDate::create(size);
        auto end = ColumnDate::create(size);
        auto & start_data = start->getData();
        auto & end_data = end->getData();

        for (size_t i = 0; i != size; ++i)
        {
            auto whole = DecimalUtils::getWholePart(time_data[i], time_column.getScale());
            start_data[i] = ToStartOfTransform<unit>::execute(whole, num_units, time_zone);
            end_data[i] = AddTime<unit>::execute(start_data[i], num_units, time_zone);
        }
        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }

    static ColumnPtr dispatchForColumnsDateTime32(const ColumnsWithTypeAndName & arguments)
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime32>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);

        auto interval = intervalKindAndUnits(interval_column);

        switch (std::get<0>(interval))
        {
            /// FIXME, TIME
            case IntervalKind::Nanosecond:
            case IntervalKind::Microsecond:
            case IntervalKind::Millisecond:
            case IntervalKind::Second:
                return executeTumble<UInt32, IntervalKind::Second>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Minute:
                return executeTumble<UInt32, IntervalKind::Minute>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Hour:
                return executeTumble<UInt32, IntervalKind::Hour>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Day:
                return executeTumble<UInt32, IntervalKind::Day>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Week:
                return executeTumble<UInt16, IntervalKind::Week>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Month:
                return executeTumble<UInt16, IntervalKind::Month>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Quarter:
                return executeTumble<UInt16, IntervalKind::Quarter>(*time_column_vec, std::get<1>(interval), time_zone);
            case IntervalKind::Year:
                return executeTumble<UInt16, IntervalKind::Year>(*time_column_vec, std::get<1>(interval), time_zone);
        }
        __builtin_unreachable();
    }

    template <typename ToType, IntervalKind::Kind unit>
    static ColumnPtr executeTumble(const ColumnUInt32 & time_column, UInt64 num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();
        auto start = ColumnVector<ToType>::create(size);
        auto end = ColumnVector<ToType>::create(size);
        auto & start_data = start->getData();
        auto & end_data = end->getData();

        for (size_t i = 0; i != size; ++i)
        {
            start_data[i] = ToStartOfTransform<unit>::execute(time_data[i], num_units, time_zone);
            end_data[i] = AddTime<unit>::execute(start_data[i], num_units, time_zone);
        }
        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }
};

template <>
struct WindowImpl<HOP>
{
    static constexpr auto name = "__hop";
    static constexpr auto external_name = "hop";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        bool result_type_is_date = true;
        IntervalKind slide_kind;
        IntervalKind window_kind;
        Int64 slide_size = 0;
        Int64 window_size = 0;

        if (arguments.size() == 3)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, slide_kind, slide_size, result_type_is_date);
            checkIntervalArgument(arguments[2], function_name, window_kind, window_size, result_type_is_date);
        }
        else if (arguments.size() == 4)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, slide_kind, slide_size, result_type_is_date);
            checkIntervalArgument(arguments[2], function_name, window_kind, window_size, result_type_is_date);
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
        DataTypePtr data_type = std::make_shared<DataTypeArray>(getReturnDataType(result_type_is_date, arguments, time_zone_arg_num_check));
        return std::make_shared<DataTypeTuple>(DataTypes{data_type, data_type});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & from_datatype = *time_column.type.get();
        auto time_col_type = WhichDataType(from_datatype);
        if (time_col_type.isDateTime64())
        {
            return dispatchForColumnsDateTime64(arguments);
        }
        else if (time_col_type.isDateTime())
        {
            return dispatchForColumnsDateTime32(arguments);
        }
        else
        {
            throw Exception(
                "Illegal column " + time_column.name + " argument of function " + function_name + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    static ColumnPtr dispatchForColumnsDateTime64(const ColumnsWithTypeAndName & arguments)
    {
        const auto & time_column = arguments[0];
        const auto & hop_interval_column = arguments[1];
        const auto & window_interval_column = arguments[2];
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);

        auto hop_interval = intervalKindAndUnits(hop_interval_column);
        auto window_interval = intervalKindAndUnits(window_interval_column);

        switch (std::get<0>(window_interval))
        {
            /// FIXME, TIME
            case IntervalKind::Nanosecond:
            case IntervalKind::Microsecond:
            case IntervalKind::Millisecond:
            case IntervalKind::Second:
                return executeHopDateTime64<IntervalKind::Second>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Minute:
                return executeHopDateTime64<IntervalKind::Minute>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Hour:
                return executeHopDateTime64<IntervalKind::Hour>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Day:
                return executeHopDateTime64<IntervalKind::Day>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Week:
                return executeHopDate<IntervalKind::Week>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Month:
                return executeHopDate<IntervalKind::Month>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Quarter:
                return executeHopDate<IntervalKind::Quarter>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Year:
                return executeHopDate<IntervalKind::Year>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
        }
        __builtin_unreachable();
    }

    template <IntervalKind::Kind unit>
    static ColumnPtr executeHopDateTime64(
        const ColumnDecimal<DateTime64> & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();

        auto scale = time_column.getScale();

        auto final_size = size * (window_num_units / hop_num_units);
        auto start = ColumnArray::create(ColumnDateTime64::create(0, scale));
        start->reserve(final_size);
        auto end = ColumnArray::create(ColumnDateTime64::create(0, scale));
        end->reserve(final_size);

        /// In order to avoid memory copy, we manipulate array and offsets by ourselves
        auto & start_data = start->getData();
        auto & start_offsets = start->getOffsets();

        auto & end_data = end->getData();
        auto & end_offsets = end->getOffsets();

        UInt64 offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            auto components = DecimalUtils::split(time_data[i], scale);
            components.fractional = 0;

            UInt32 event_ts = components.whole;
            /// Note: hop_num_units as the starting of the `last` window of the hopping
            UInt32 wstart = ToStartOfTransform<unit>::execute(event_ts, hop_num_units, time_zone);
            UInt32 wend = AddTime<unit>::execute(wstart, window_num_units, time_zone);

            do
            {
                components.whole = wstart;
                start_data.insert(DecimalUtils::decimalFromComponents(components, scale));

                components.whole = wend;
                end_data.insert(DecimalUtils::decimalFromComponents(components, scale));
                ++offset;

                /// Slide to left until the right of the window passes (<) `component.whole`
                wstart = AddTime<unit>::execute(wstart, -1 * hop_num_units, time_zone);
                wend = AddTime<unit>::execute(wend, -1 * hop_num_units, time_zone);
            } while (wend > event_ts);

            start_offsets.push_back(offset);
            end_offsets.push_back(offset);
        }

        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }

    template <IntervalKind::Kind unit>
    static ColumnPtr
    executeHopDate(const ColumnDateTime64 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();

        auto scale = time_column.getScale();

        auto final_size = size * (window_num_units / hop_num_units);
        auto start = ColumnArray::create(ColumnDate::create(0));
        start->reserve(final_size);
        auto end = ColumnArray::create(ColumnDate::create(0));
        end->reserve(final_size);

        /// In order to avoid memory copy, we manipulate array and offsets by ourselves
        auto & start_data = start->getData();
        auto & start_offsets = start->getOffsets();

        auto & end_data = end->getData();
        auto & end_offsets = end->getOffsets();

        UInt64 offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            UInt32 whole = DecimalUtils::getWholePart(time_data[i], scale);
            UInt16 event_ts = ToStartOfTransform<unit>::execute(whole, 1, time_zone);
            UInt16 wstart = ToStartOfTransform<unit>::execute(event_ts, hop_num_units, time_zone);
            UInt16 wend = AddTime<unit>::execute(wstart, window_num_units, time_zone);

            do
            {
                start_data.insert(Field(wstart));
                end_data.insert(Field(wend));
                ++offset;

                /// Slide to left until the right of the window passes (<) `component.whole`
                wstart = AddTime<unit>::execute(wstart, -1 * hop_num_units, time_zone);
                wend = AddTime<unit>::execute(wend, -1 * hop_num_units, time_zone);
            } while (wend > event_ts);

            start_offsets.push_back(offset);
            end_offsets.push_back(offset);
        }
        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }

    static ColumnPtr dispatchForColumnsDateTime32(const ColumnsWithTypeAndName & arguments)
    {
        const auto & time_column = arguments[0];
        const auto & hop_interval_column = arguments[1];
        const auto & window_interval_column = arguments[2];
        const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);

        auto hop_interval = intervalKindAndUnits(hop_interval_column);
        auto window_interval = intervalKindAndUnits(window_interval_column);

        switch (std::get<0>(window_interval))
        {
            /// FIXME, TIME
            case IntervalKind::Nanosecond:
            case IntervalKind::Microsecond:
            case IntervalKind::Millisecond:
            case IntervalKind::Second:
                return executeHop<UInt32, IntervalKind::Second>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Minute:
                return executeHop<UInt32, IntervalKind::Minute>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Hour:
                return executeHop<UInt32, IntervalKind::Hour>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Day:
                return executeHop<UInt32, IntervalKind::Day>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Week:
                return executeHop<UInt16, IntervalKind::Week>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Month:
                return executeHop<UInt16, IntervalKind::Month>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Quarter:
                return executeHop<UInt16, IntervalKind::Quarter>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Year:
                return executeHop<UInt16, IntervalKind::Year>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
        }
        __builtin_unreachable();
    }

    template <typename ToType, IntervalKind::Kind unit>
    static ColumnPtr
    executeHop(const ColumnDateTime32 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();

        auto final_size = size * (window_num_units / hop_num_units);
        auto start = ColumnArray::create(ColumnVector<ToType>::create(0));
        start->reserve(final_size);
        auto end = ColumnArray::create(ColumnVector<ToType>::create(0));
        end->reserve(final_size);

        /// In order to avoid memory copy, we manipulate array and offsets by ourselves
        auto & start_data = start->getData();
        auto & start_offsets = start->getOffsets();

        auto & end_data = end->getData();
        auto & end_offsets = end->getOffsets();

        UInt64 offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            /// event_ts is a round down to the latest unit which will be its
            /// current second, minute, hour, day, week, month, quarter, year etc
            ToType event_ts = time_data[i];
            ToType wstart = ToStartOfTransform<unit>::execute(event_ts, hop_num_units, time_zone);
            ToType wend = AddTime<unit>::execute(wstart, window_num_units, time_zone);

            do
            {
                start_data.insert(Field(wstart));
                end_data.insert(Field(wend));
                ++offset;

                /// Slide to left until the right of the window passes (<) `event_ts`
                wstart = AddTime<unit>::execute(wstart, -1 * hop_num_units, time_zone);
                wend = AddTime<unit>::execute(wend, -1 * hop_num_units, time_zone);
            } while (wend > event_ts);

            start_offsets.push_back(offset);
            end_offsets.push_back(offset);
        }
        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }
};

template <>
struct WindowImpl<SESSION>
{
    static constexpr auto name = "__session";
    static constexpr auto external_name = "session";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        bool result_type_is_date = true;
        IntervalKind window_kind;
        Int64 window_size = 0;

        if (arguments.size() >= 2)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, window_kind, window_size, result_type_is_date);
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        size_t time_zone_arg_num_check = 0;
        DataTypePtr data_type = getReturnDataType(result_type_is_date, arguments, time_zone_arg_num_check);
        return std::make_shared<DataTypeTuple>(DataTypes{data_type, data_type});
    }

    static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & /*arguments*/, const String & /*function_name*/)
    {
        MutableColumns result;
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
