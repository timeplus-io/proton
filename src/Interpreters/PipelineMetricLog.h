#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndAliases.h>

#include <atomic>
#include <ctime>
#include <vector>


namespace DB
{

/** PipelineMetricLog is a log of query pipeline metric values measured at regular time interval.
  */

struct PipelineMetricLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};
    UInt64 milliseconds{};
    String query_id;
    String query;
    String pipeline_metric;

    static std::string name() { return "PipelineMetricLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};


class PipelineMetricLog : public SystemLog<PipelineMetricLogElement>
{
    using SystemLog<PipelineMetricLogElement>::SystemLog;

public:
    void shutdown() override;

    /// Launches a background thread to collect metrics with interval
    void startCollectMetric(size_t collect_interval_milliseconds_);

    /// Stop background thread. Call before shutdown.
    void stopCollectMetric();

private:
    void metricThreadFunction();

    ThreadFromGlobalPool metric_flush_thread;
    size_t collect_interval_milliseconds;
    std::atomic<bool> stopped = false;
};

}
