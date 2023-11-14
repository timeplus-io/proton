#pragma once

#include <string>

#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/Context_fwd.h>

#include <IO/WriteBuffer.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

/// Write metrics in Prometheus format
class PrometheusMetricsWriter
{
public:
    PrometheusMetricsWriter(
        const Poco::Util::AbstractConfiguration & config, const std::string & config_name,
        const AsynchronousMetrics & async_metrics_, ContextPtr context);

    void write(WriteBuffer & wb) const;


private:
    const AsynchronousMetrics & async_metrics;
    ContextPtr context;

    const bool send_events;
    const bool send_metrics;
    const bool send_asynchronous_metrics;
    const bool send_status_info;
    const bool send_external_stream;

    static inline constexpr auto profile_events_prefix = "ProtonProfileEvents_";
    static inline constexpr auto current_metrics_prefix = "ProtonMetrics_";
    static inline constexpr auto asynchronous_metrics_prefix = "ProtonAsyncMetrics_";
    static inline constexpr auto current_status_prefix = "ProtonStatusInfo_";
    static inline constexpr auto external_stream_prefix = "ExternalStream_";
};

}
