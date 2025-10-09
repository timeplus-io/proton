#pragma once

#include <Core/DecimalFunctions.h>
#include <base/Decimal.h>
#include <Common/LocalDateTime.h>

class LocalDateTime64
{
private:
    LocalDateTime local_datetime;

    Int64 fractional;
    UInt32 scale;

    Int64 getFractionalWithScale(UInt32 new_scale) const
    {
        if (new_scale == scale)
            return fractional;

        using T = typename DB::DateTime64::NativeType;
        if (new_scale > scale)
            return fractional * DB::DecimalUtils::scaleMultiplier<T>(new_scale - scale);
        else
            return fractional / DB::DecimalUtils::scaleMultiplier<T>(scale - new_scale);
    }

public:
    explicit LocalDateTime64(const DB::DateTime64 & datetime64, UInt32 scale_, const DateLUTImpl & time_zone = DateLUT::instance())
    {
        static constexpr UInt32 MaxScale = DB::DecimalUtils::max_precision<DB::DateTime64>;
        scale = scale_ > MaxScale ? MaxScale : scale_;

        auto components = DB::DecimalUtils::split(datetime64, scale);
        /// Case1:
        /// -127914467.877
        /// => whole = -127914467, fraction = 877(After DecimalUtils::split)
        /// => new whole = -127914468(1965-12-12 12:12:12), new fraction = 1000 - 877 = 123(.123)
        /// => 1965-12-12 12:12:12.123
        ///
        /// Case2:
        /// -0.877
        /// => whole = 0, fractional = -877(After DecimalUtils::split)
        /// => whole = -1(1969-12-31 23:59:59), fractional = 1000 + (-877) = 123(.123)
        using T = typename DB::DateTime64::NativeType;
        if (datetime64.value < 0 && components.fractional)
        {
            components.fractional = DB::DecimalUtils::scaleMultiplier<T>(scale) + (components.whole ? T(-1) : T(1)) * components.fractional;
            --components.whole;
        }

        local_datetime = LocalDateTime(components.whole, time_zone);

        fractional = components.fractional;
    }

    LocalDateTime64(
        unsigned short year_,
        unsigned char month_,
        unsigned char day_,
        unsigned char hour_,
        unsigned char minute_,
        unsigned char second_,
        Int64 microsecond_)
        : local_datetime(year_, month_, day_, hour_, minute_, second_), fractional(microsecond_), scale(6)
    {
    }

    LocalDateTime64(const LocalDateTime64 &) noexcept = default;
    LocalDateTime64 & operator=(const LocalDateTime64 &) noexcept = default;

    unsigned short year() const { return local_datetime.year(); }
    unsigned char month() const { return local_datetime.month(); }
    unsigned char day() const { return local_datetime.day(); }
    unsigned char hour() const { return local_datetime.hour(); }
    unsigned char minute() const { return local_datetime.minute(); }
    unsigned char second() const { return local_datetime.second(); }
    Int64 millisecond() const { return getFractionalWithScale(3); }
    Int64 microsecond() const { return getFractionalWithScale(6); }
    Int64 nanosecond() const { return getFractionalWithScale(9); }

    LocalDate toDate() const { return local_datetime.toDate(); }
    LocalDateTime toStartOfDate() const { return local_datetime.toStartOfDate(); }

    time_t to_time_t(const DateLUTImpl & time_zone = DateLUT::instance()) const { return local_datetime.to_time_t(time_zone); }

    DB::DateTime64 toDateTime64(UInt32 new_scale, const DateLUTImpl & time_zone = DateLUT::instance()) const
    {
        return DB::DecimalUtils::decimalFromComponents<DB::DateTime64>(
            {local_datetime.to_time_t(time_zone), getFractionalWithScale(new_scale)}, new_scale);
    }
};
