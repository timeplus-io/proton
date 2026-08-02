#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/CgroupsMemoryUsageObserver.h>
#include <Common/MemoryStatisticsOS.h>
#include <Common/MemoryWorker.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <IO/ReadBufferFromFile.h>

#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>


namespace Poco
{
    class Logger;
}

namespace DB
{

class ReadBuffer;
struct Settings;

struct AsynchronousMetricValue
{
    /// proton: starts
    double value = 0;
    const char * documentation = "";
    /// proton: ends

    template <typename T>
    AsynchronousMetricValue(T value_, const char * documentation_)
        : value(static_cast<double>(value_)), documentation(documentation_) {}
    AsynchronousMetricValue() = default; /// For std::unordered_map::operator[].
};

using AsynchronousMetricValues = std::unordered_map<std::string, AsynchronousMetricValue>;

struct ProtocolServerMetrics
{
    String port_name;
    size_t current_threads;
};

/** Periodically (by default, each minute, starting at 30 seconds offset)
  *  calculates and updates some metrics,
  *  that are not updated automatically (so, need to be asynchronously calculated).
  *
  * This includes both ClickHouse-related metrics (like memory usage of ClickHouse process)
  *  and common OS-related metrics (like total memory usage on the server).
  *
  * All the values are either gauge type (like the total number of tables, the current memory usage).
  * Or delta-counters representing some accumulation during the interval of time.
  */
class AsynchronousMetrics : WithContext
{
public:
    using ProtocolServerMetricsFunc = std::function<std::vector<ProtocolServerMetrics>()>;
    AsynchronousMetrics(
        ContextPtr global_context_,
        int update_period_seconds,
        const ProtocolServerMetricsFunc & protocol_server_metrics_func_,
        bool update_jemalloc_epoch_,
        bool update_rss_);

    ~AsynchronousMetrics();

    /// Separate method allows to initialize the `servers` variable beforehand.
    void start();

    void stop();

    /// Returns copy of all values.
    AsynchronousMetricValues getValues() const;

#if defined(OS_LINUX)
    /// https://man7.org/linux/man-pages/man5/proc_pid_stat.5.html
    struct SelfProcStats
    {
        uint64_t utime;
        uint64_t stime;
        int64_t cutime;
        int64_t cstime;

        void read(ReadBuffer & in);
        SelfProcStats operator-(const SelfProcStats & other) const;
    };
#endif

private:
    const std::chrono::seconds update_period;
    ProtocolServerMetricsFunc protocol_server_metrics_func;

    std::unique_ptr<Settings> settings;

    mutable std::mutex mutex;
    std::condition_variable wait_cond;
    bool quit {false};
    AsynchronousMetricValues values;

    /// Some values are incremental and we have to calculate the difference.
    /// On first run we will only collect the values to subtract later.
    bool first_run = true;
    std::chrono::system_clock::time_point previous_update_time;

    [[maybe_unused]] const bool update_jemalloc_epoch;
    [[maybe_unused]] const bool update_rss;

#if defined(OS_LINUX)
    MemoryStatisticsOS memory_stat;

    std::optional<ReadBufferFromFilePRead> meminfo;
    std::optional<ReadBufferFromFilePRead> loadavg;
    std::optional<ReadBufferFromFilePRead> proc_stat;
    std::optional<ReadBufferFromFilePRead> proc_self_stat;
    std::optional<ReadBufferFromFilePRead> cpuinfo;
    std::optional<ReadBufferFromFilePRead> file_nr;
    std::optional<ReadBufferFromFilePRead> uptime;
    std::optional<ReadBufferFromFilePRead> net_dev;

    std::optional<ReadBufferFromFilePRead> cpu_pressure;
    std::optional<ReadBufferFromFilePRead> memory_pressure;
    std::optional<ReadBufferFromFilePRead> io_pressure;

    std::unordered_map<String /* PSI stall type */, uint64_t> prev_pressure_vals;

    std::optional<ReadBufferFromFilePRead> cgroupmem_limit_in_bytes;
    std::shared_ptr<ICgroupsReader> cgroupmem_reader;
    std::optional<ReadBufferFromFilePRead> cgroupcpu_cfs_period;
    std::optional<ReadBufferFromFilePRead> cgroupcpu_cfs_quota;
    std::optional<ReadBufferFromFilePRead> cgroupcpu_max;
    std::optional<ReadBufferFromFilePRead> cgroupcpu_stat;
    std::optional<ReadBufferFromFilePRead> cgroupcpuacct_stat;

    std::vector<std::unique_ptr<ReadBufferFromFilePRead>> thermal;

    std::unordered_map<String /* device name */,
        std::unordered_map<String /* label name */,
            std::unique_ptr<ReadBufferFromFilePRead>>> hwmon_devices;

    std::vector<std::pair<
        std::unique_ptr<ReadBufferFromFilePRead> /* correctable errors */,
        std::unique_ptr<ReadBufferFromFilePRead> /* uncorrectable errors */>> edac;

    std::unordered_map<String /* device name */, std::unique_ptr<ReadBufferFromFilePRead>> block_devs;

    /// TODO: socket statistics.

    struct ProcStatValuesCPU
    {
        uint64_t user;
        uint64_t nice;
        uint64_t system;
        uint64_t idle;
        uint64_t iowait;
        uint64_t irq;
        uint64_t softirq;
        uint64_t steal;
        uint64_t guest;
        uint64_t guest_nice;

        void read(ReadBuffer & in);
        ProcStatValuesCPU operator-(const ProcStatValuesCPU & other) const;
    };

    struct ProcStatValuesOther
    {
        uint64_t interrupts;
        uint64_t context_switches;
        uint64_t processes_created;

    ProcStatValuesOther operator-(const ProcStatValuesOther & other) const;
    };

    ProcStatValuesCPU cgroup_values_all_cpus{};
    ProcStatValuesCPU proc_stat_values_all_cpus{};
    ProcStatValuesOther proc_stat_values_other{};
    std::vector<ProcStatValuesCPU> proc_stat_values_per_cpu;

    /// proton: starts.
    SelfProcStats self_proc_stats{};
    /// proton: ends.

    /// https://www.kernel.org/doc/Documentation/block/stat.txt
    struct BlockDeviceStatValues
    {
        uint64_t read_ios;
        uint64_t read_merges;
        uint64_t read_sectors;
        uint64_t read_ticks;
        uint64_t write_ios;
        uint64_t write_merges;
        uint64_t write_sectors;
        uint64_t write_ticks;
        uint64_t in_flight_ios;
        uint64_t io_ticks;
        uint64_t time_in_queue;
        uint64_t discard_ops;
        uint64_t discard_merges;
        uint64_t discard_sectors;
        uint64_t discard_ticks;

        void read(ReadBuffer & in);
        BlockDeviceStatValues operator-(const BlockDeviceStatValues & other) const;
    };

    std::unordered_map<String /* device name */, BlockDeviceStatValues> block_device_stats;

    struct NetworkInterfaceStatValues
    {
        uint64_t recv_bytes;
        uint64_t recv_packets;
        uint64_t recv_errors;
        uint64_t recv_drop;
        uint64_t send_bytes;
        uint64_t send_packets;
        uint64_t send_errors;
        uint64_t send_drop;

        NetworkInterfaceStatValues operator-(const NetworkInterfaceStatValues & other) const;
    };

    std::unordered_map<String /* device name */, NetworkInterfaceStatValues> network_interface_stats;

    Stopwatch block_devices_rescan_delay;

    void openSensors();
    void openBlockDevices();
    void openSensorsChips();
    void openEDAC();

    std::unique_ptr<ReadBufferFromFilePRead> openFileIfExists(const std::string & filename);
    void openFileIfExists(const char * filename, std::optional<ReadBufferFromFilePRead> & out);
    void openCgroupv2MetricFile(const std::string & filename, std::optional<ReadBufferFromFilePRead> & out);

    void applyCgroupCPUMetricsUpdate(AsynchronousMetricValues & new_values, const ProcStatValuesCPU & delta_values, double multiplier);

    void applyCgroupNormalizedCPUMetricsUpdate(
        AsynchronousMetricValues & new_values,
        double num_cpus_to_normalize,
        const ProcStatValuesCPU & delta_values_all_cpus,
        double multiplier);

    void applyCPUMetricsUpdate(
        AsynchronousMetricValues & new_values, const std::string & cpu_suffix, const ProcStatValuesCPU & delta_values, double multiplier);

    void applyNormalizedCPUMetricsUpdate(
        AsynchronousMetricValues & new_values,
        double num_cpus_to_normalize,
        const ProcStatValuesCPU & delta_values_all_cpus,
        double multiplier);

#endif

/// proton: starts
#if defined(OS_DARWIN)
    struct MacDiskStatValues
    {
        uint64_t read_ops = 0;
        uint64_t write_ops = 0;
        uint64_t read_bytes = 0;
        uint64_t write_bytes = 0;
    };

    std::unordered_map<String /* device name */, MacDiskStatValues> mac_disk_stats;

    static std::unordered_map<String, MacDiskStatValues> collectMacDiskStats();
#endif
/// proton: ends

    std::unique_ptr<ThreadFromGlobalPool> thread;

    void run();
    void update(std::chrono::system_clock::time_point update_time);

    LoggerPtr log;
};

}
