#include "SeekToInfo.h"

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <NativeLog/Record/Record.h>
#include <base/ClockUtils.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/parseIntStrict.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED;
}

namespace
{
String toString(SeekToType type)
{
    switch (type)
    {
        case SeekToType::ABSOLUTE_TIME:
            return "absolute time";
        case SeekToType::RELATIVE_TIME:
            return "relative time";
        case SeekToType::SEQUENCE_NUMBER:
            return "sequence number";
    }
}

std::pair<int64_t, bool> tryParseRelativeTimeSeek(const String & seek_to, bool utc)
{
    int64_t multiplier = 0;
    bool is_month = false, is_quarter = false, is_year = false;
    switch (seek_to.back())
    {
        case 's':
            multiplier = 1000;
            break;
        case 'm':
            multiplier = 60 * 1000;
            break;
        case 'h':
            multiplier = 60 * 60 * 1000;
            break;
        case 'd':
            multiplier = 24 * 60 * 60 * 1000;
            break;
        case 'M':
            multiplier = 1000;
            is_month = true;
            break;
        case 'q':
            multiplier = 1000;
            is_quarter = true;
            break;
        case 'y':
            multiplier = 1000;
            is_year = true;
            break;
        default:
            return {-1, false};
    }

    String timezone{utc ? "UTC" : ""};

    Int64 now_ms = 0;
    if (utc)
        now_ms = UTCMilliseconds::now();
    else
        now_ms = local_now_ms();

    Int32 relative_time;
    try
    {
        relative_time = parseIntStrict<Int32>(seek_to, 0, seek_to.size() - 1);
    }
    catch (const Exception &)
    {
        return {-1, false};
    }

    if (relative_time > 0)
        return {-1, false}; /// Relative seek time shall be negative

    int64_t seek_point;
    if (is_month)
    {
        seek_point = DateLUT::instance(timezone).addMonths(now_ms / multiplier, relative_time) * multiplier + (now_ms % multiplier);
    }
    else if (is_quarter)
    {
        seek_point = DateLUT::instance(timezone).addQuarters(now_ms / multiplier, relative_time) * multiplier + (now_ms % multiplier);
    }
    else if (is_year)
    {
        seek_point = DateLUT::instance(timezone).addYears(now_ms / multiplier, relative_time) * multiplier + (now_ms % multiplier);
    }
    else
        seek_point = now_ms + relative_time * multiplier;

    return {std::move(seek_point), true};
}

std::pair<int64_t, bool> tryParseSequenceBasedSeek(const String & seek_to)
{
    try
    {
        int64_t sn = parseIntStrict<int64_t>(seek_to, 0, seek_to.size());
        if (sn < 0)
            return {-1, false}; /// Sequence number shall be greater than 0

        return {sn, true};
    }
    catch (const Exception &)
    {
        return {-1, false};
    }
}

std::pair<SeekToType, int64_t> doParse(const String & seek_to, bool utc)
{
    assert(!seek_to.empty());

    /// -1 latest, -2 earliest
    if (seek_to == "latest")
        return {SeekToType::SEQUENCE_NUMBER, nlog::LATEST_SN};
    else if (seek_to == "earliest")
        return {SeekToType::SEQUENCE_NUMBER, nlog::EARLIEST_SN};

    if (auto [relative_seek_point, relative_valid] = tryParseRelativeTimeSeek(seek_to, utc); relative_valid)
        return {SeekToType::RELATIVE_TIME, relative_seek_point};
    else if (auto [absolute_seek_point, absolute_valid] = tryParseAbsoluteTimeSeek(seek_to); absolute_valid)
        return {SeekToType::ABSOLUTE_TIME, absolute_seek_point};
    else if (auto [sequence_seek_point, sequence_valid] = tryParseSequenceBasedSeek(seek_to); sequence_valid)
        return {SeekToType::SEQUENCE_NUMBER, sequence_seek_point};
    else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid seek_to '{}'. 'seek_to' supports to seek to a time based or a sequence number based. For example: time based "
            "supports to ISO8601 format or negative relative time (e.g. 2020-01-01T01:12:45Z or 2020-01-01T01:12:45.123+08:00 or -10s or "
            "-6m or "
            "-2h or -1d), sequence number based supports to 'earliest' or 'latest' or integer greater than or equal to 0",
            seek_to);
}
}

std::pair<int64_t, bool> tryParseAbsoluteTimeSeek(const String & seek_to)
{
    /// Simple check
    if (seek_to.find('-') == String::npos)
        return {-1, false};

    try
    {
        DateTime64 res;
        ReadBufferFromString in(seek_to);
        parseDateTime64BestEffort(res, 3, in, DateLUT::instance(), DateLUT::instance("UTC"));
        return {static_cast<int64_t>(res), true};
    }
    catch (...)
    {
        return {-1, false};
    }
}

SeekToInfo::SeekToInfo(String seek_to_) : seek_to(std::move(seek_to_))
{
    std::tie(type, seek_points) = parse(seek_to, /*default utc*/ true);
    assert(!seek_points.empty());
}

void SeekToInfo::replicateForShards(Int32 shards)
{
    if (seek_points.size() != 1 && seek_points.size() != static_cast<size_t>(shards))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid seek to. 'seek_to' supports to seek to a timestamp or a sequence number for each shard. "
            "For examples: time based seek_to='-1h', seek_to='-1h,-2h,-3h', seek_to='2020-01-01T01:12:45.123+08:00', "
            "sequence number based seek_to='earliest', seek_to='1023,23045,689'. Users either specify a common timestamp or sequence "
            "number to "
            "seek to for all shards of a stream or specify a separate timestamp or sequence number separated by ',' to seek to for each "
            "shard of a stream (if the `shards` setting is used, it needs to match that value). It expected {} timestamps or sequence "
            "numbers, but only found {} was specified",
            shards,
            seek_points.size());

    while (seek_points.size() < static_cast<size_t>(shards))
        seek_points.push_back(seek_points.back());

    assert(seek_points.size() == static_cast<size_t>(shards));
}

std::pair<SeekToType, std::vector<int64_t>> SeekToInfo::parse(const String & seek_to_str, bool utc)
{
    if (seek_to_str.empty())
        return {SeekToType::SEQUENCE_NUMBER, std::vector<int64_t>(1, nlog::LATEST_SN)};

    std::vector<String> seeks_to;
    boost::algorithm::split(seeks_to, seek_to_str, boost::is_any_of(","));

    assert(!seeks_to.empty());

    std::optional<SeekToType> last_seek_type;
    std::vector<int64_t> seek_points;
    seek_points.reserve(seeks_to.size());
    for (const auto & seek : seeks_to)
    {
        auto [seek_type, seek_point] = doParse(seek, utc);

        if (!last_seek_type)
            last_seek_type = seek_type;
        else if (*last_seek_type != seek_type)
            throw Exception(
                ErrorCodes::UNSUPPORTED, "Mixing {} and {} seek to is not supported", toString(*last_seek_type), toString(seek_type));

        seek_points.emplace_back(seek_point);
    }

    assert(last_seek_type);
    return {*last_seek_type, std::move(seek_points)};
}

String SeekToInfo::getSeekToForSettings() const
{
    if (isTimeBased())
    {
        /// Timestamp formats
        /// 1) absolute time string: '2020-01-01T01:12:45.123+08:00', now()-1d
        /// 2) relative time: -1m
        /// Transform seek_point (time based) to ISO timestamp string
        WriteBufferFromOwnString wb;
        for (size_t i = 0; const auto & seek_point : seek_points)
        {
            if (i != 0)
                wb.write(',');
            writeDateTimeTextISO(seek_point, 3, wb, DateLUT::instance("UTC"));
            ++i;
        }
        return wb.str();
    }
    else
    {
        /// Sequence ID formats:
        /// 1) absolute sn: 1000,2000,3000,
        /// 2) relative sn: latest,earliest
        return seek_to;
    }
}
}
