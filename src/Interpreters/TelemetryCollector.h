#pragma once

#include <NativeLog/Base/Concurrent/BlockingQueue.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/TelemetryElement.h>
#include <Common/logger_useful.h>

namespace DB
{

class TelemetryCollector
{
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

    void add(std::shared_ptr<TelemetryElement> element);

    template <typename Builder>
    void add(std::function<void(Builder & builder)> f)
    {
        Builder builder;
        f(builder);
        add(builder.build());
    }

private:
    void collect();
    TelemetryCollector(ContextPtr context_);

    void upload();

private:
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

    ThreadFromGlobalPool upload_thread;
    nlog::BlockingQueue<std::shared_ptr<TelemetryElement>> queue;

    Poco::Logger * logger;
};
}
