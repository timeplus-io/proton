#include "storageUtil.h"

#include <IO/ReadBufferFromString.h>
#include <IO/parseDateTimeBestEffort.h>
#include <NativeLog/Record/Record.h>
#include <Storages/IStorage.h>
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
bool isTimeBasedSeek(const String & seek_to)
{
    switch (seek_to.back())
    {
        case 's':
        case 'm':
        case 'h':
        case 'd':
        case 'M':
        case 'q':
        case 'y':
            return true;
        default:
            break;
    }

    return seek_to.find('-') != String::npos;
}

int64_t parseTimebaseSeek(const String & seek_to, bool utc)
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
    }

    String timezone{utc ? "UTC" : ""};

    Int64 now_ms = 0;
    if (utc)
        now_ms = UTCMilliseconds::now();
    else
        now_ms = local_now_ms();

    if (multiplier > 0)
    {
        Int32 relative_time;
        try
        {
            relative_time = parseIntStrict<Int32>(seek_to, 0, seek_to.size() - 1);
        }
        catch (const Exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid relative seek time '{}'. An integer number is expected.", seek_to);
        }

        if (relative_time > 0)
            throw Exception("Relative seek time shall be negative", ErrorCodes::BAD_ARGUMENTS);

        if (is_month)
        {
            now_ms = DateLUT::instance(timezone).addMonths(now_ms / multiplier, relative_time) * multiplier + (now_ms % multiplier);
        }
        else if (is_quarter)
        {
            now_ms = DateLUT::instance(timezone).addQuarters(now_ms / multiplier, relative_time) * multiplier + (now_ms % multiplier);
        }
        else if (is_year)
        {
            now_ms = DateLUT::instance(timezone).addYears(now_ms / multiplier, relative_time) * multiplier + (now_ms % multiplier);
        }
        else
            now_ms += relative_time * multiplier;
    }
    else
    {
        DateTime64 res;
        ReadBufferFromString in(seek_to);
        try
        {
            parseDateTime64BestEffort(res, 3, in, DateLUT::instance(), DateLUT::instance("UTC"));
        }
        catch (...)
        {
            throw Exception(
                "Invalid seek timestamp, ISO8601 format or relative time are supported. Example: 2020-01-01T01:12:45Z or "
                "2020-01-01T01:12:45.123+08:00 or -10s or -6m or -2h or -1d",
                ErrorCodes::BAD_ARGUMENTS);
        }

        now_ms = res;
    }
    return now_ms;
}

int64_t parseSequenceBasedSeek(const String & seek_to)
{
    int64_t sn;
    try
    {
        sn = parseIntStrict<int64_t>(seek_to, 0, seek_to.size());
    }
    catch (const Exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid seek sequence number '{}'. Sequence number shall be integer.", seek_to);
    }

    if (sn < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid seek sequence number '{}'. Sequence number shall be greater than 0.", seek_to);

    return sn;
}

std::pair<bool, int64_t> doParseSeekTo(const String & seek_to, bool utc)
{
    if (seek_to.empty())
        return {false, nlog::LATEST_SN};

    /// -1 latest, -2 earliest
    if (seek_to == "latest")
        return {false, nlog::LATEST_SN};
    else if (seek_to == "earliest")
        return {false, nlog::EARLIEST_SN};

    if (isTimeBasedSeek(seek_to))
        return {true, parseTimebaseSeek(seek_to, utc)};
    else
        return {false, parseSequenceBasedSeek(seek_to)};
}
}

std::pair<bool, std::vector<int64_t>> parseSeekTo(const String & seek_to, Int32 shards, bool utc)
{
    if (seek_to.empty())
        return {false, std::vector<int64_t>(shards, nlog::LATEST_SN)};

    std::vector<String> seeks_to;
    seeks_to.reserve(shards);

    boost::algorithm::split(seeks_to, seek_to, boost::is_any_of(","));

    assert(!seeks_to.empty());

    if (seeks_to.size() != 1 && seeks_to.size() != static_cast<size_t>(shards))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid seek to setting '{}'. 'seek_to' supports to seek to a timestamp or a sequence number for each shard. "
            "For examples: time based seek_to='-1h', seek_to='-1h,-2h,-3h', seek_to='2020-01-01T01:12:45.123+08:00', "
            "sequence number based seek_to='earliest', seek_to='1023,23045,689'. Users either specify a common timestamp or sequence "
            "number to "
            "seek to for all shards of a stream or specify a separate timestamp or sequence number separated by ',' to seek to for each "
            "shard of a stream. It expected {} timestamps or sequence numbers, but only found {} was specified",
            seek_to,
            shards,
            seeks_to.size());

    std::vector<int64_t> seeks_to_parsed;
    seeks_to_parsed.reserve(shards);

    std::optional<bool> timestamp_based;
    for (const auto & seek : seeks_to)
    {
        auto [is_timestamp_based, ts_or_sn] = doParseSeekTo(seek, utc);
        if (!timestamp_based)
            timestamp_based = is_timestamp_based;
        else if (*timestamp_based != is_timestamp_based)
            throw Exception(ErrorCodes::UNSUPPORTED, "Mixing time based and sequence number based seek to is not supported");

        seeks_to_parsed.push_back(ts_or_sn);
    }
    assert(timestamp_based);

    while (seeks_to_parsed.size() != static_cast<size_t>(shards))
        seeks_to_parsed.push_back(seeks_to_parsed.back());

    assert(seeks_to_parsed.size() == static_cast<size_t>(shards));

    return {*timestamp_based, std::move(seeks_to_parsed)};
}

bool supportStreamingQuery(const StoragePtr & storage)
{
    const auto & name = storage->getName();
    return (name == "ProxyStream" || name == "Stream" || name == "View" || name == "MaterializedView" || name == "ExternalStream" || name == "Random");
}
}
