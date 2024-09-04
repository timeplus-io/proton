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
    std::atomic_bool is_enable;

    std::string started_on;
    bool new_session = true;
    Int64 started_on_in_minutes;
    std::atomic<UInt64> collect_interval_ms;

    Int64 prev_total_select_query = 0;
    Int64 prev_streaming_select_query = 0;
    Int64 prev_historical_select_query = 0;

    // static constexpr auto INTERVAL_MS = 5 * 60 * 1000; /// sending anonymous telemetry data every 5 minutes

public:
    static TelemetryCollector & instance(ContextPtr context_)
    {
        static TelemetryCollector inst(context_);
        return inst;
    }

    ~TelemetryCollector();

    void startup();
    void shutdown();

    void enable();
    void disable();

    bool isEnabled() const { return is_enable; }

    UInt64 getCollectIntervalMilliseconds() const { return collect_interval_ms.load(); }

    void setCollectIntervalMilliseconds(UInt64 interval_ms) { collect_interval_ms.store(interval_ms); }

private:
    void collect();
    TelemetryCollector(ContextPtr context_);
};
}
