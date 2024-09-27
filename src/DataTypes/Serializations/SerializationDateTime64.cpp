#include <DataTypes/Serializations/SerializationDateTime64.h>

#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <Common/DateLUT.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>

/// proton : starts
#include <base/ClockUtils.h>
/// proton : ends

namespace DB
{

SerializationDateTime64::SerializationDateTime64(
    UInt32 scale_, const TimezoneMixin & time_zone_)
    : SerializationDecimalBase<DateTime64>(DecimalUtils::max_precision<DateTime64>, scale_)
    , TimezoneMixin(time_zone_)
{
}

void SerializationDateTime64::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto value = assert_cast<const ColumnType &>(column).getData()[row_num];
    switch (settings.date_time_output_format)
    {
        case FormatSettings::DateTimeOutputFormat::Simple:
            if (has_explicit_time_zone)
                writeDateTimeTextWIthZone(value, scale, ostr, time_zone);
            else
                writeDateTimeText(value, scale, ostr, time_zone);
            return;
        case FormatSettings::DateTimeOutputFormat::UnixTimestamp:
            writeDateTimeUnixTimestamp(value, scale, ostr);
            return;
        case FormatSettings::DateTimeOutputFormat::ISO:
            writeDateTimeTextISO(value, scale, ostr, utc_time_zone);
            return;
    }
}

void SerializationDateTime64::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    DateTime64 result = 0;
    readDateTime64Text(result, scale, istr, time_zone);
    assert_cast<ColumnType &>(column).getData().push_back(result);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "datetime64");
}

void SerializationDateTime64::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    if (!istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "datetime64");
}

void SerializationDateTime64::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeText(column, row_num, ostr, settings);
}

static inline void readText(DateTime64 & x, UInt32 scale, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
{
    switch (settings.date_time_input_format)
    {
        case FormatSettings::DateTimeInputFormat::Basic:
            readDateTime64Text(x, scale, istr, time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffort:
            parseDateTime64BestEffort(x, scale, istr, time_zone, utc_time_zone);
            return;
        case FormatSettings::DateTimeInputFormat::BestEffortUS:
            parseDateTime64BestEffortUS(x, scale, istr, time_zone, utc_time_zone);
            return;
    }
}

void SerializationDateTime64::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    readText(x, scale, istr, settings, time_zone, utc_time_zone);
    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void SerializationDateTime64::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationDateTime64::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }

    /// proton : starts
    rescale(x.value);
    /// proton : ends

    assert_cast<ColumnType &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

void SerializationDateTime64::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime64::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;
    if (checkChar('"', istr))
    {
        readText(x, scale, istr, settings, time_zone, utc_time_zone);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }

    /// proton : starts
    rescale(x.value);
    /// proton : ends

    assert_cast<ColumnType &>(column).getData().push_back(x);
}

void SerializationDateTime64::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationDateTime64::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    DateTime64 x = 0;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++istr.position();

    readText(x, scale, istr, settings, time_zone, utc_time_zone);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    /// proton : starts
    rescale(x.value);
    /// proton : ends

    assert_cast<ColumnType &>(column).getData().push_back(x);
}

/// proton : starts. When real scale is different the expected scale of the datetime64 type
/// the timestamp will be interpreted wrongly. The following logic is trying to fix it
/// by rescaling. This rescaling is wrong in theory since it is possible
/// users insert a long future datetime in theory, but in practice, it shall be fine.
/// FIXME, we don't support rescale other scales other than ms/us/ns
void SerializationDateTime64::rescale(Int64 & x) const
{
    if (scale == 0 || scale == 3 || scale == 6 || scale == 9)
    {
        UInt64 real_scale = static_cast<UInt64>(x / UTCSeconds::now());
        if (real_scale > 100ul && real_scale < 10'000ul)
            real_scale = 3;
        else if (real_scale > 100'000ul && real_scale < 10'000'000ul)
            real_scale = 6;
        else if (real_scale > 100'000'000ul && real_scale < 10'000'000'000ul)
            real_scale = 9;
        else
            real_scale = 0;

        if (scale > real_scale)
            x *= common::exp10_i64(static_cast<int>(scale - real_scale));
        else if (scale < real_scale)
            x /= common::exp10_i64(static_cast<int>(real_scale - scale));
    }
}
/// proton : ends
}
