#include "storageUtil.h"

#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/parseIntStrict.h>
#include <IO/ReadBufferFromString.h>
#include <IO/parseDateTimeBestEffort.h>
#include <NativeLog/Record/Record.h>
#include <Storages/IStorage.h>
#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// return the start timestamp in milli-seconds in UTC timezone or local timezone
int64_t parseSeekTo(const String & seek_to, bool utc)
{
    /// -1h
    /// ISO8601
    if (seek_to.empty())
        return nlog::LATEST_SN;

    /// -1 latest, -2 earliest
    if (seek_to == "latest")
        return nlog::LATEST_SN;
    else if (seek_to == "earliest")
        return nlog::EARLIEST_SN;

    Int64 multiplier = 0;
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
        auto relative_time = parseIntStrict<Int32>(seek_to, 0, seek_to.size() - 1);

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
            now_ms +=  relative_time * multiplier;
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

bool supportStreamingQuery(const StoragePtr & storage)
{
    const auto & name = storage->getName();
    return (name == "ProxyStream" || name == "Stream" || name == "View" || name == "MaterializedView" || name == "ExternalStream");
}
}
