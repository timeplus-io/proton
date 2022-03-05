#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <optional>
#include <Core/Types.h>
#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{

/** Automatically sends
  * - delta values of ProfileEvents;
  * - cumulative values of ProfileEvents;
  * - values of CurrentMetrics;
  *  to Graphite at beginning of every minute.
  */
class MetricsTransmitter
{
public:
    MetricsTransmitter(const Poco::Util::AbstractConfiguration & config, const std::string & config_name_);
    ~MetricsTransmitter();

private:
    void run();
    void transmit(std::vector<ProfileEvents::Count> & prev_counters);

    std::string config_name;
    UInt32 interval_seconds;
    bool send_events;
    bool send_events_cumulative;
    bool send_metrics;

    bool quit = false;
    std::mutex mutex;
    std::condition_variable cond;
    std::optional<ThreadFromGlobalPool> thread;

    static inline constexpr auto profile_events_path_prefix = "Proton.ProfileEvents.";
    static inline constexpr auto profile_events_cumulative_path_prefix = "Proton.ProfileEventsCumulative.";
    static inline constexpr auto current_metrics_path_prefix = "Proton.Metrics.";
};

}
