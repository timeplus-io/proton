#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Common/logger_useful.h>

namespace DB
{

class TelemetryCollector
{
private:
    Poco::Logger * log;

    BackgroundSchedulePool & pool;
    BackgroundSchedulePoolTaskHolder collector_task;
    std::atomic_flag is_shutdown;

    std::string started_on;
    bool new_session = true;
    Int64 started_on_in_minutes;

    static constexpr auto INTERVAL_MS = 2 * 60 * 1000; /// sending anonymous telemetry data every 2 minutes

public:
    static TelemetryCollector & instance(ContextPtr context_)
    {
        static TelemetryCollector inst(context_);
        return inst;
    }

    ~TelemetryCollector();
    void shutdown();

private:
    void collect();
    TelemetryCollector(ContextPtr context_);
};
}
