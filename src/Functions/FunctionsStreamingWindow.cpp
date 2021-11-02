#include <numeric>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsStreamingWindow.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{
    using ColumnDateTime64 = ColumnDecimal<DateTime64>;
    using ColumnDateTime32 = ColumnVector<UInt32>;
    using ColumnDate = ColumnVector<UInt16>;

    std::tuple<IntervalKind::Kind, Int64>
    dispatchForIntervalColumns(const ColumnWithTypeAndName & interval_column, const String & function_name)
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
        if (!interval_type)
            throw Exception(
                "Illegal column " + interval_column.name + " of argument of function " + function_name, ErrorCodes::ILLEGAL_COLUMN);
        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
        if (!interval_column_const_int64)
            throw Exception(
                "Illegal column " + interval_column.name + " of argument of function " + function_name, ErrorCodes::ILLEGAL_COLUMN);
        Int64 num_units = interval_column_const_int64->getValue<Int64>();
        if (num_units <= 0)
            throw Exception(
                "Value for column " + interval_column.name + " of function " + function_name + " must be positive",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        return {interval_type->getKind(), num_units};
    }

    ColumnPtr executeWindowBound(const ColumnPtr & column, int index, const String & function_name)
    {
        if (const ColumnTuple * col_tuple = checkAndGetColumn<ColumnTuple>(column.get()))
        {
            if (!checkColumn<ColumnDate>(*col_tuple->getColumnPtr(index)) && !checkColumn<ColumnDateTime32>(*col_tuple->getColumnPtr(index))
                && !checkColumn<ColumnDateTime64>(*col_tuple->getColumnPtr(index)))
                throw Exception(
                    "Illegal column for first argument of function " + function_name
                        + ". Must be a Tuple(DataTime64, DataTime64) or a Tuple(ColumnVectorUInt16, ColumnVectorUInt16)",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return col_tuple->getColumnPtr(index);
        }
        else
        {
            throw Exception(
                "Illegal column for first argument of function " + function_name + ". Must be Tuple", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    void checkFirstArgument(const ColumnWithTypeAndName & argument, const String & function_name)
    {
        if (!isDateTime64(argument.type) && !isDateTime(argument.type))
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name + ". Should be a date with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void checkIntervalArgument(
        const ColumnWithTypeAndName & argument, const String & function_name, IntervalKind & interval_kind, bool & result_type_is_date)
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(argument.type.get());
        if (!interval_type)
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name
                    + ". Should be an interval of time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        interval_kind = interval_type->getKind();
        result_type_is_date = (interval_kind == IntervalKind::Year) || (interval_kind == IntervalKind::Quarter)
            || (interval_kind == IntervalKind::Month) || (interval_kind == IntervalKind::Week);
    }

    void checkIntervalArgument(const ColumnWithTypeAndName & argument, const String & function_name, bool & result_type_is_date)
    {
        IntervalKind interval_kind;
        checkIntervalArgument(argument, function_name, interval_kind, result_type_is_date);
    }

    void checkTimeZoneArgument(const ColumnWithTypeAndName & argument, const String & function_name)
    {
        if (!isString(argument.type))
            throw Exception(
                "Illegal type " + argument.type->getName() + " of argument of function " + function_name
                    + ". This argument is optional and must be a constant string with timezone name",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    bool checkIntervalOrTimeZoneArgument(
        const ColumnWithTypeAndName & argument, const String & function_name, IntervalKind & interval_kind, bool & result_type_is_date)
    {
        if (isString(argument.type))
        {
            checkTimeZoneArgument(argument, function_name);
            return false;
        }
        checkIntervalArgument(argument, function_name, interval_kind, result_type_is_date);
        return true;
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
    static constexpr auto name = "__TUMBLE";

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
            return dispatchForColumnsDateTime64(arguments, function_name);
        }
        else if (isDateTime(from_datatype))
        {
            return dispatchForColumnsDateTime32(arguments, function_name);
        }
        else
        {
            throw Exception(
                "Illegal column " + time_column.name + " argument of function " + function_name + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    static ColumnPtr dispatchForColumnsDateTime64(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);

        auto interval = dispatchForIntervalColumns(interval_column, function_name);

        switch (std::get<0>(interval))
        {
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

    static ColumnPtr dispatchForColumnsDateTime32(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime32>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);

        auto interval = dispatchForIntervalColumns(interval_column, function_name);

        switch (std::get<0>(interval))
        {
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
struct WindowImpl<TUMBLE_START>
{
    static constexpr auto name = "__TUMBLE_START";

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        /// FIXME: window ID
        if (arguments.size() == 1)
        {
            /// Referencing a window ID which points a tuple
            if (isTuple(arguments[0].type))
            {
                auto tuple_type = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
                if (tuple_type)
                    return tuple_type->getElements()[0];
            }
            throw Exception(
                "Illegal type of first argument of function " + function_name + " should be a window ID", ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            auto return_type = WindowImpl<TUMBLE>::getReturnType(arguments, function_name);
            auto tuple_type = checkAndGetDataType<DataTypeTuple>(return_type.get());
            return tuple_type->getElements()[0];
        }
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto first_arg_type = WhichDataType(arguments[0].type);
        ColumnPtr result_column_;
        if (first_arg_type.isDateTime64() || first_arg_type.isDateTime())
            result_column_ = WindowImpl<TUMBLE>::dispatchForColumns(arguments, function_name);
        else
            result_column_ = arguments[0].column;
        return executeWindowBound(result_column_, 0, function_name);
    }
};

template <>
struct WindowImpl<TUMBLE_END>
{
    static constexpr auto name = "__TUMBLE_END";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        return WindowImpl<TUMBLE_START>::getReturnType(arguments, function_name);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto first_arg_type = WhichDataType(arguments[0].type);
        ColumnPtr result_column_;
        if (first_arg_type.isDateTime64() || first_arg_type.isDateTime())
            result_column_ = WindowImpl<TUMBLE>::dispatchForColumns(arguments, function_name);
        else
            result_column_ = arguments[0].column;
        return executeWindowBound(result_column_, 1, function_name);
    }
};

template <>
struct WindowImpl<HOP>
{
    static constexpr auto name = "__HOP";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        bool result_type_is_date;
        IntervalKind interval_kind_1;
        IntervalKind interval_kind_2;

        if (arguments.size() == 3)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, interval_kind_1, result_type_is_date);
            checkIntervalArgument(arguments[2], function_name, interval_kind_2, result_type_is_date);
        }
        else if (arguments.size() == 4)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, interval_kind_1, result_type_is_date);
            checkIntervalArgument(arguments[2], function_name, interval_kind_2, result_type_is_date);
            checkTimeZoneArgument(arguments[3], function_name);
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 3 or 4",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (interval_kind_1 != interval_kind_2)
            throw Exception(
                "Illegal type of window and hop column of function " + function_name + ", must be same", ErrorCodes::ILLEGAL_COLUMN);

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
            return dispatchForColumnsDateTime64(arguments, function_name);
        }
        else if (time_col_type.isDateTime())
        {
            return dispatchForColumnsDateTime32(arguments, function_name);
        }
        else
        {
            throw Exception(
                "Illegal column " + time_column.name + " argument of function " + function_name + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    static ColumnPtr dispatchForColumnsDateTime64(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & hop_interval_column = arguments[1];
        const auto & window_interval_column = arguments[2];
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);

        auto hop_interval = dispatchForIntervalColumns(hop_interval_column, function_name);
        auto window_interval = dispatchForIntervalColumns(window_interval_column, function_name);

        if (std::get<1>(hop_interval) > std::get<1>(window_interval))
            throw Exception(
                "Value for hop interval of function " + function_name + " must not larger than window interval",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        switch (std::get<0>(window_interval))
        {
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

        /// FIXME, size
        auto start = ColumnArray::create(ColumnDateTime64::create(0, scale));
        auto end = ColumnArray::create(ColumnDateTime64::create(0, scale));

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
            UInt32 wstart = ToStartOfTransform<unit>::execute(event_ts, window_num_units, time_zone);
            UInt32 wend = AddTime<unit>::execute(wstart, window_num_units, time_zone);

            UInt32 wstart_l = wstart;
            UInt32 wend_l = wend;

            do
            {
                components.whole = wstart_l;
                start_data.insert(DecimalUtils::decimalFromComponents(components, scale));

                components.whole = wend_l;
                end_data.insert(DecimalUtils::decimalFromComponents(components, scale));
                ++offset;

                /// Slide to left until the right of the window passes (<) `component.whole`
                wstart_l = AddTime<unit>::execute(wstart_l, -1 * hop_num_units, time_zone);
                wend_l = AddTime<unit>::execute(wend_l, -1 * hop_num_units, time_zone);
            } while (wend_l > event_ts);

            UInt32 wstart_r = AddTime<unit>::execute(wstart, hop_num_units, time_zone);
            UInt32 wend_r = AddTime<unit>::execute(wend, hop_num_units, time_zone);

            while (wstart_r <= event_ts)
            {
                components.whole = wstart_r;
                start_data.insert(DecimalUtils::decimalFromComponents(components, scale));

                components.whole = wend_r;
                end_data.insert(DecimalUtils::decimalFromComponents(components, scale));
                ++offset;

                /// Slide to right until the left of the window passes (>) `component.whole`
                wstart_r = AddTime<unit>::execute(wstart_r, hop_num_units, time_zone);
                wend_r = AddTime<unit>::execute(wend_r, hop_num_units, time_zone);
            }

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

        /// FIXME, size to avoid reallocation as much as possible
        auto start = ColumnArray::create(ColumnDate::create(0));
        auto end = ColumnArray::create(ColumnDate::create(0));

        auto & start_data = start->getData();
        auto & start_offsets = start->getOffsets();

        auto & end_data = end->getData();
        auto & end_offsets = end->getOffsets();

        UInt64 offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            UInt32 whole = DecimalUtils::getWholePart(time_data[i], scale);
            UInt16 event_ts = ToStartOfTransform<unit>::execute(whole, 1, time_zone);
            UInt16 wstart = ToStartOfTransform<unit>::execute(whole, window_num_units, time_zone);
            UInt16 wend = AddTime<unit>::execute(wstart, window_num_units, time_zone);

            UInt16 wstart_l = wstart;
            UInt16 wend_l = wend;

            do
            {
                start_data.insert(Field(wstart_l));
                end_data.insert(Field(wend_l));
                ++offset;

                /// Slide to left until the right of the window passes (<) `component.whole`
                wstart_l = AddTime<unit>::execute(wstart_l, -1 * hop_num_units, time_zone);
                wend_l = AddTime<unit>::execute(wend_l, -1 * hop_num_units, time_zone);
            } while (wend_l > event_ts);

            UInt16 wstart_r = AddTime<unit>::execute(wstart, hop_num_units, time_zone);
            UInt16 wend_r = AddTime<unit>::execute(wend, hop_num_units, time_zone);

            while (wstart_r <= event_ts)
            {
                start_data.insert(Field(wstart_r));
                end_data.insert(Field(wend_r));
                ++offset;

                /// Slide to right until the left of the window passes (>) `component.whole`
                wstart_r = AddTime<unit>::execute(wstart_r, hop_num_units, time_zone);
                wend_r = AddTime<unit>::execute(wend_r, hop_num_units, time_zone);
            }

            start_offsets.push_back(offset);
            end_offsets.push_back(offset);
        }
        MutableColumns result;
        result.emplace_back(std::move(start));
        result.emplace_back(std::move(end));
        return ColumnTuple::create(std::move(result));
    }

    static ColumnPtr dispatchForColumnsDateTime32(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & hop_interval_column = arguments[1];
        const auto & window_interval_column = arguments[2];
        const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);

        auto hop_interval = dispatchForIntervalColumns(hop_interval_column, function_name);
        auto window_interval = dispatchForIntervalColumns(window_interval_column, function_name);
        if (std::get<1>(hop_interval) > std::get<1>(window_interval))
            throw Exception(
                "Value for hop interval of function " + function_name + " must not larger than window interval",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        switch (std::get<0>(window_interval))
        {
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

        /// FIXME, size
        auto start = ColumnArray::create(ColumnVector<ToType>::create(0));
        auto end = ColumnArray::create(ColumnVector<ToType>::create(0));

        auto & start_data = start->getData();
        auto & start_offsets = start->getOffsets();

        auto & end_data = end->getData();
        auto & end_offsets = end->getOffsets();

        UInt64 offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            /// event_ts is a round down to the latest unit which will be its
            /// current second, minute, hour, day, week, month, quarter, year etc
            ToType event_ts = ToStartOfTransform<unit>::execute(time_data[i], 1, time_zone);
            ToType wstart = ToStartOfTransform<unit>::execute(time_data[i], window_num_units, time_zone);
            ToType wend = AddTime<unit>::execute(wstart, window_num_units, time_zone);

            ToType wstart_l = wstart;
            ToType wend_l = wend;

            do
            {
                start_data.insert(Field(wstart_l));
                end_data.insert(Field(wend_l));
                ++offset;

                /// Slide to left until the right of the window passes (<) `component.whole`
                wstart_l = AddTime<unit>::execute(wstart_l, -1 * hop_num_units, time_zone);
                wend_l = AddTime<unit>::execute(wend_l, -1 * hop_num_units, time_zone);
            } while (wend_l > event_ts);

            ToType wstart_r = AddTime<unit>::execute(wstart, hop_num_units, time_zone);
            ToType wend_r = AddTime<unit>::execute(wend, hop_num_units, time_zone);

            while (wstart_r <= event_ts)
            {
                start_data.insert(Field(wstart_r));
                end_data.insert(Field(wend_r));
                ++offset;

                /// Slide to right until the left of the window passes (>) `component.whole`
                wstart_r = AddTime<unit>::execute(wstart_r, hop_num_units, time_zone);
                wend_r = AddTime<unit>::execute(wend_r, hop_num_units, time_zone);
            }

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
struct WindowImpl<HOP_START>
{
    static constexpr auto name = "__HOP_START";

    static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        /// FIXME WINDOW_ID
        if (arguments.size() == 1)
        {
            if (isTuple(arguments[0].type))
            {
                auto tuple_type = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
                return tuple_type->getElements()[0];
            }
            else
            {
                throw Exception(
                    "Illegal type of first argument of function " + function_name + " should be a window ID", ErrorCodes::ILLEGAL_COLUMN);
            }
        }
        else
        {
            auto return_type = WindowImpl<HOP>::getReturnType(arguments, function_name);
            auto tuple_type = checkAndGetDataType<DataTypeTuple>(return_type.get());
            return tuple_type->getElements()[0];
        }
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto first_arg_type = WhichDataType(arguments[0].type);
        ColumnPtr result_column_;
        if (first_arg_type.isDateTime64() || first_arg_type.isDateTime())
            result_column_ = WindowImpl<HOP>::dispatchForColumns(arguments, function_name);
        else
            result_column_ = arguments[0].column;
        return executeWindowBound(result_column_, 0, function_name);
    }
};

template <>
struct WindowImpl<HOP_END>
{
    static constexpr auto name = "__HOP_END";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        return WindowImpl<HOP_START>::getReturnType(arguments, function_name);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto first_arg_type = WhichDataType(arguments[0].type);
        ColumnPtr result_column_;
        if (first_arg_type.isDateTime64() || first_arg_type.isDateTime())
            result_column_ = WindowImpl<HOP>::dispatchForColumns(arguments, function_name);
        else
            result_column_ = arguments[0].column;
        return executeWindowBound(result_column_, 1, function_name);
    }
};

template <WindowFunctionName type>
DataTypePtr FunctionWindow<type>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    return WindowImpl<type>::getReturnType(arguments, name);
}

template <WindowFunctionName type>
ColumnPtr FunctionWindow<type>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const
{
    return WindowImpl<type>::dispatchForColumns(arguments, name);
}

template <>
struct WindowImpl<WINDOW_ID>
{
    static constexpr auto name = "__WINDOW_ID";

    [[maybe_unused]] static DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        bool result_type_is_date;
        IntervalKind interval_kind_1;
        IntervalKind interval_kind_2;

        if (arguments.size() == 2)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, interval_kind_1, result_type_is_date);
        }
        else if (arguments.size() == 3)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, interval_kind_1, result_type_is_date);
            if (checkIntervalOrTimeZoneArgument(arguments[2], function_name, interval_kind_2, result_type_is_date))
            {
                if (interval_kind_1 != interval_kind_2)
                    throw Exception(
                        "Illegal type of window and hop column of function " + function_name + ", must be same",
                        ErrorCodes::ILLEGAL_COLUMN);
            }
        }
        else if (arguments.size() == 4)
        {
            checkFirstArgument(arguments[0], function_name);
            checkIntervalArgument(arguments[1], function_name, interval_kind_1, result_type_is_date);
            checkIntervalArgument(arguments[2], function_name, interval_kind_2, result_type_is_date);
            checkTimeZoneArgument(arguments[3], function_name);
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + function_name + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2, 3 or 4",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        size_t time_zone_arg_num_check = arguments.size() > 2 ? arguments.size() - 1 : 0;
        return getReturnDataType(result_type_is_date, arguments, time_zone_arg_num_check);
    }

    [[maybe_unused]] static ColumnPtr dispatchForHopColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & from_datatype = *time_column.type.get();
        if (isDateTime64(from_datatype))
        {
            return dispatchForHopColumnsDateTime64(arguments, function_name);
        }
        else if (isDateTime64(from_datatype))
        {
            return dispatchForHopColumnsDateTime32(arguments, function_name);
        }
        else
        {
            throw Exception(
                "Illegal column " + time_column.name + " argument of function " + function_name + ". Must contain dates or dates with time",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    static ColumnPtr dispatchForHopColumnsDateTime64(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & hop_interval_column = arguments[1];
        const auto & window_interval_column = arguments[2];
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);

        auto hop_interval = dispatchForIntervalColumns(hop_interval_column, function_name);
        auto window_interval = dispatchForIntervalColumns(window_interval_column, function_name);

        if (std::get<1>(hop_interval) > std::get<1>(window_interval))
            throw Exception(
                "Value for hop interval of function " + function_name + " must not larger than window interval",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        switch (std::get<0>(window_interval))
        {
            case IntervalKind::Second:
                return executeHopSliceDateTime64<IntervalKind::Second>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Minute:
                return executeHopSliceDateTime64<IntervalKind::Minute>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Hour:
                return executeHopSliceDateTime64<IntervalKind::Hour>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Day:
                return executeHopSliceDateTime64<IntervalKind::Day>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Week:
                return executeHopSliceDate<IntervalKind::Week>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Month:
                return executeHopSliceDate<IntervalKind::Month>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Quarter:
                return executeHopSliceDate<IntervalKind::Quarter>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Year:
                return executeHopSliceDate<IntervalKind::Year>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
        }
        __builtin_unreachable();
    }

    template <IntervalKind::Kind unit>
    static ColumnPtr executeHopSliceDateTime64(
        const ColumnDateTime64 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();

        auto scale = time_column.getScale();
        auto end = ColumnDateTime64::create(size, scale);
        auto & end_data = end->getData();

        Int64 gcd_num_units = std::gcd(hop_num_units, window_num_units);

        for (size_t i = 0; i < size; ++i)
        {
            auto components = DecimalUtils::split(time_data[i], scale);
            components.fractional = 0;

            auto wstart = ToStartOfTransform<unit>::execute(components.whole, hop_num_units, time_zone);
            auto wend = AddTime<unit>::execute(wstart, hop_num_units, time_zone);

            auto wend_current = wend;
            auto wend_latest = wend;

            do
            {
                wend_latest = wend_current;
                wend_current = AddTime<unit>::execute(wend_current, -1 * gcd_num_units, time_zone);
            } while (wend_current > components.whole);

            components.whole = wend_latest;
            end_data[i] = DecimalUtils::decimalFromComponents(components, scale);
        }
        return end;
    }

    template <IntervalKind::Kind unit>
    static ColumnPtr
    executeHopSliceDate(const ColumnDateTime64 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();

        auto scale = time_column.getScale();
        auto end = ColumnDate::create(size);
        auto & end_data = end->getData();

        Int64 gcd_num_units = std::gcd(hop_num_units, window_num_units);

        for (size_t i = 0; i < size; ++i)
        {
            auto whole = DecimalUtils::getWholePart(time_data[i], scale);
            auto wstart = ToStartOfTransform<unit>::execute(whole, hop_num_units, time_zone);
            auto wend = AddTime<unit>::execute(wstart, hop_num_units, time_zone);

            auto wend_current = wend;
            auto wend_latest = wend;

            do
            {
                wend_latest = wend_current;
                wend_current = AddTime<unit>::execute(wend_current, -1 * gcd_num_units, time_zone);
            } while (wend_current > whole);

            end_data[i] = wend_latest;
        }
        return end;
    }

    static ColumnPtr dispatchForHopColumnsDateTime32(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        const auto & time_column = arguments[0];
        const auto & hop_interval_column = arguments[1];
        const auto & window_interval_column = arguments[2];
        const auto * time_column_vec = checkAndGetColumn<ColumnDateTime32>(time_column.column.get());
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 3, 0);

        auto hop_interval = dispatchForIntervalColumns(hop_interval_column, function_name);
        auto window_interval = dispatchForIntervalColumns(window_interval_column, function_name);

        if (std::get<1>(hop_interval) > std::get<1>(window_interval))
            throw Exception(
                "Value for hop interval of function " + function_name + " must not larger than window interval",
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        switch (std::get<0>(window_interval))
        {
            case IntervalKind::Second:
                return executeHopSlice<UInt32, IntervalKind::Second>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Minute:
                return executeHopSlice<UInt32, IntervalKind::Minute>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Hour:
                return executeHopSlice<UInt32, IntervalKind::Hour>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Day:
                return executeHopSlice<UInt32, IntervalKind::Day>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Week:
                return executeHopSlice<UInt16, IntervalKind::Week>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Month:
                return executeHopSlice<UInt16, IntervalKind::Month>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Quarter:
                return executeHopSlice<UInt16, IntervalKind::Quarter>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
            case IntervalKind::Year:
                return executeHopSlice<UInt16, IntervalKind::Year>(
                    *time_column_vec, std::get<1>(hop_interval), std::get<1>(window_interval), time_zone);
        }
        __builtin_unreachable();
    }

    template <typename ToType, IntervalKind::Kind unit>
    static ColumnPtr
    executeHopSlice(const ColumnDateTime32 & time_column, UInt64 hop_num_units, UInt64 window_num_units, const DateLUTImpl & time_zone)
    {
        Int64 gcd_num_units = std::gcd(hop_num_units, window_num_units);

        const auto & time_data = time_column.getData();
        size_t size = time_column.size();

        auto end = ColumnVector<ToType>::create(size);
        auto & end_data = end->getData();

        for (size_t i = 0; i < size; ++i)
        {
            ToType wstart = ToStartOfTransform<unit>::execute(time_data[i], hop_num_units, time_zone);
            ToType wend = AddTime<unit>::execute(wstart, hop_num_units, time_zone);

            ToType wend_current = wend;
            ToType wend_latest = wend;

            do
            {
                wend_latest = wend_current;
                wend_current = AddTime<unit>::execute(wend_current, -1 * gcd_num_units, time_zone);
            } while (wend_current > time_data[i]);

            end_data[i] = wend_latest;
        }
        return end;
    }

    [[maybe_unused]] static ColumnPtr dispatchForTumbleColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        ColumnPtr column = WindowImpl<TUMBLE>::dispatchForColumns(arguments, function_name);
        return executeWindowBound(column, 1, function_name);
    }

    [[maybe_unused]] static ColumnPtr dispatchForColumns(const ColumnsWithTypeAndName & arguments, const String & function_name)
    {
        if (arguments.size() == 2)
            return dispatchForTumbleColumns(arguments, function_name);
        else
        {
            const auto & third_column = arguments[2];
            if (arguments.size() == 3 && isString(third_column.type))
                return dispatchForTumbleColumns(arguments, function_name);
            else
                return dispatchForHopColumns(arguments, function_name);
        }
    }
};

void registerFunctionsStreamingWindow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTumble>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTumbleStart>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTumbleEnd>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionHop>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionHopStart>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionHopEnd>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionWindowId>(FunctionFactory::CaseInsensitive);
}
}
