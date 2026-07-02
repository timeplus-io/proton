#include <Common/MemoryWorker.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <base/cgroupsv2.h>
#include <Common/Jemalloc.h>
#include <Common/MemoryTracker.h>
#if defined(OS_LINUX)
#    include <Common/MemoryStatisticsOS.h>
#endif
#include <Common/OSThreadNiceValue.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <filesystem>
#include <optional>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event MemoryAllocatorPurge;
    extern const Event MemoryAllocatorPurgeTimeMicroseconds;
    extern const Event MemoryWorkerRun;
    extern const Event MemoryWorkerRunElapsedMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

#if defined(OS_LINUX)
namespace
{

/// Format is
///   kernel 5
///   rss 15
///   [...]
std::map<std::string, uint64_t> readAllMetricsFromStatFile(ReadBufferFromFile & buf)
{
    std::map<std::string, uint64_t> metrics;
    while (!buf.eof())
    {
        std::string current_key;
        readStringUntilWhitespace(current_key, buf);

        assertChar(' ', buf);

        uint64_t value = 0;
        readIntText(value, buf);
        assertChar('\n', buf);

        auto [_, inserted] = metrics.emplace(std::move(current_key), value);
        chassert(inserted, "Duplicate keys in stat file");
    }
    return metrics;
}

using Metrics = std::map<std::string_view, uint64_t>;

void readMetricsFromStatFile(
    ReadBufferFromFile & buf,
    Metrics & metrics,
    std::initializer_list<std::string_view> keys,
    std::initializer_list<std::string_view> optional_keys,
    bool * warnings_printed)
{
    /// Zero out existing values; keeps map nodes allocated for reuse.
    for (auto & [_, v] : metrics)
        v = 0;

    /// Track which keys were actually seen in this pass.
    uint64_t seen_mask = 0;

    bool print_warnings = !*warnings_printed;
    while (!buf.eof())
    {
        std::string current_key;
        readStringUntilWhitespace(current_key, buf);

        const auto * it = std::find(keys.begin(), keys.end(), current_key);
        if (it == keys.end())
        {
            std::string dummy;
            readStringUntilNewlineInto(dummy, buf);
            buf.tryIgnore(1);
            continue;
        }

        assertChar(' ', buf);
        uint64_t value = 0;
        readIntText(value, buf);
        buf.tryIgnore(1);

        uint64_t key_bit = 1ull << (it - keys.begin());
        if (seen_mask & key_bit)
        {
            if (print_warnings)
            {
                *warnings_printed = true;
                LOG_ERROR(getLogger("CgroupsReader"), "Duplicate key '{}' in '{}'", current_key, buf.getFileName());
            }
        }
        seen_mask |= key_bit;

        /// Use the string_view from keys (string literals) as map key.
        metrics[*it] = value;
    }

    if (print_warnings)
    {
        for (auto it = keys.begin(); it != keys.end(); ++it)
        {
            uint64_t key_bit = 1ull << (it - keys.begin());
            if (!(seen_mask & key_bit) && std::find(optional_keys.begin(), optional_keys.end(), *it) == optional_keys.end())
            {
                *warnings_printed = true;
                LOG_ERROR(getLogger("CgroupsReader"), "Cannot find '{}' in '{}'", *it, buf.getFileName());
            }
        }
    }
}

struct CgroupsV1Reader : ICgroupsReader
{
    explicit CgroupsV1Reader(const fs::path & stat_file_dir) : buf(stat_file_dir / "memory.stat") { }

    uint64_t readMemoryUsage() override
    {
        std::lock_guard lock(mutex);
        buf.rewind();
        readMetricsFromStatFile(buf, metrics, {"rss"}, {}, &warnings_printed);
        auto it = metrics.find("rss");
        return it != metrics.end() ? it->second : 0;
    }

    std::string dumpAllStats() override
    {
        std::lock_guard lock(mutex);
        buf.rewind();
        return fmt::format("{}", readAllMetricsFromStatFile(buf));
    }

private:
    std::mutex mutex;
    ReadBufferFromFile buf TSA_GUARDED_BY(mutex);
    Metrics metrics TSA_GUARDED_BY(mutex);
    bool warnings_printed TSA_GUARDED_BY(mutex) = false;
};

struct CgroupsV2Reader : ICgroupsReader
{
    explicit CgroupsV2Reader(const fs::path & stat_file_dir) : stat_buf(stat_file_dir / "memory.stat") { }

    uint64_t readMemoryUsage() override
    {
        std::lock_guard lock(mutex);
        stat_buf.rewind();
        readMetricsFromStatFile(
            stat_buf, metrics, {"anon", "sock", "kernel", "slab_reclaimable"}, {"kernel", "slab_reclaimable"}, &warnings_printed);

        auto get = [](const Metrics & m, std::string_view key) -> uint64_t
        {
            auto it = m.find(key);
            return it != m.end() ? it->second : 0;
        };

        /// anon + sock: actual process memory.
        /// kernel - slab_reclaimable: non-reclaimable kernel memory (pagetables, kernel_stack, slab_unreclaimable).
        /// slab_reclaimable is excluded because the kernel reclaims it synchronously under memory pressure
        /// before invoking the OOM killer, so it should not count against the application's memory budget.
        uint64_t usage = get(metrics, "anon") + get(metrics, "sock");
        uint64_t kernel = get(metrics, "kernel");
        uint64_t slab_reclaimable = get(metrics, "slab_reclaimable");
        if (kernel > slab_reclaimable)
            usage += kernel - slab_reclaimable;
        return usage;
    }

    std::string dumpAllStats() override
    {
        std::lock_guard lock(mutex);
        stat_buf.rewind();
        return fmt::format("{}", readAllMetricsFromStatFile(stat_buf));
    }

private:
    std::mutex mutex;
    ReadBufferFromFile stat_buf TSA_GUARDED_BY(mutex);
    Metrics metrics TSA_GUARDED_BY(mutex);
    bool warnings_printed TSA_GUARDED_BY(mutex) = false;
};

/// Caveats:
/// - All of the logic in this file assumes that the current process is the only process in the
///   containing cgroup (or more precisely: the only process with significant memory consumption).
///   If this is not the case, then other processe's memory consumption may affect the internal
///   memory tracker ...
/// - Cgroups v1 and v2 allow nested cgroup hierarchies. As v1 is deprecated for over half a
///   decade and will go away at some point, hierarchical detection is only implemented for v2.
/// - I did not test what happens if a host has v1 and v2 simultaneously enabled. I believe such
///   systems existed only for a short transition period.

std::optional<std::string> getCgroupsV1Path()
{
    auto path = default_cgroups_mount / "memory/memory.stat";
    if (!fs::exists(path))
        return {};
    return {default_cgroups_mount / "memory"};
}

}

std::pair<std::string, ICgroupsReader::CgroupsVersion> ICgroupsReader::getCgroupsPath()
{
    auto v2_path = getCgroupsV2PathContainingFile("memory.current");
    if (v2_path.has_value())
        return {*v2_path, ICgroupsReader::CgroupsVersion::V2};

    auto v1_path = getCgroupsV1Path();
    if (v1_path.has_value())
        return {*v1_path, ICgroupsReader::CgroupsVersion::V1};

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot find cgroups v1 or v2 current memory file");
}

std::shared_ptr<ICgroupsReader> ICgroupsReader::createCgroupsReader(ICgroupsReader::CgroupsVersion version, const std::filesystem::path & cgroup_path)
{
    if (version == CgroupsVersion::V2)
        return std::make_shared<CgroupsV2Reader>(cgroup_path);
    else
    {
        chassert(version == CgroupsVersion::V1);
        return std::make_shared<CgroupsV1Reader>(cgroup_path);
    }
}
#endif

namespace
{

std::string_view sourceToString(MemoryWorker::MemoryUsageSource source)
{
    switch (source)
    {
        case MemoryWorker::MemoryUsageSource::Cgroups: return "Cgroups";
        case MemoryWorker::MemoryUsageSource::Jemalloc: return "Jemalloc";
        case MemoryWorker::MemoryUsageSource::None: return "None";
    }
}

}

/// We try to pick the best possible supported source for reading memory usage.
/// Supported sources in order of priority
/// - reading from cgroups' pseudo-files (fastest and most accurate)
/// - reading jemalloc's resident stat (doesn't take into account allocations that didn't use jemalloc)
/// Also, different tick rates are used because not all options are equally fast
MemoryWorker::MemoryWorker(MemoryWorkerConfig config)
    : log(getLogger("MemoryWorker"))
    , rss_update_period_ms(config.rss_update_period_ms)
    , correct_tracker(config.correct_tracker)
    , purge_total_memory_threshold_ratio(config.purge_total_memory_threshold_ratio)
    , purge_dirty_pages_threshold_ratio(config.purge_dirty_pages_threshold_ratio)
{
#if USE_JEMALLOC
    page_size = pagesize_mib.getValue();
#endif

    if (config.use_cgroup)
    {
#if defined(OS_LINUX)
        try
        {
            static constexpr uint64_t cgroups_memory_usage_tick_ms{50};

            const auto [cgroup_path, version] = ICgroupsReader::getCgroupsPath();
            LOG_INFO(
                getLogger("CgroupsReader"),
                "Will create cgroup reader from '{}' (cgroups version: {})",
                cgroup_path,
                (version == ICgroupsReader::CgroupsVersion::V1) ? "v1" : "v2");

            cgroups_reader = ICgroupsReader::createCgroupsReader(version, cgroup_path);
            source = MemoryUsageSource::Cgroups;
            if (rss_update_period_ms == 0)
                rss_update_period_ms = cgroups_memory_usage_tick_ms;

            return;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Cannot use cgroups reader");
        }
#endif
    }

#if USE_JEMALLOC
    static constexpr uint64_t jemalloc_memory_usage_tick_ms{100};

    source = MemoryUsageSource::Jemalloc;
    if (rss_update_period_ms == 0)
        rss_update_period_ms = jemalloc_memory_usage_tick_ms;
#endif
}

MemoryWorker::MemoryUsageSource MemoryWorker::getSource()
{
    return source;
}

void MemoryWorker::start()
{
    if (source == MemoryUsageSource::None)
        return;

    const std::string purge_dirty_pages_info = purge_dirty_pages_threshold_ratio > 0 || purge_total_memory_threshold_ratio > 0
        ? fmt::format(
              "enabled (total memory threshold ratio: {}, dirty pages threshold ratio: {}, page size: {})",
              purge_total_memory_threshold_ratio,
              purge_dirty_pages_threshold_ratio,
              page_size)
        : "disabled";

    LOG_INFO(
        log,
        "Starting background memory thread with period of {}ms, using {} as source, purging dirty pages {}",
        rss_update_period_ms,
        sourceToString(source),
        purge_dirty_pages_info);

    update_resident_memory_thread = ThreadFromGlobalPool([this] { updateResidentMemoryThread(); });

#if USE_JEMALLOC
    purge_dirty_pages_thread = ThreadFromGlobalPool([this] { purgeDirtyPagesThread(); });
#endif
}

MemoryWorker::~MemoryWorker()
{
    {
        std::scoped_lock lock(rss_update_mutex, purge_dirty_pages_mutex);
        shutdown = true;
    }

    rss_update_cv.notify_all();
    purge_dirty_pages_cv.notify_all();

    if (update_resident_memory_thread.joinable())
        update_resident_memory_thread.join();

#if USE_JEMALLOC
    if (purge_dirty_pages_thread.joinable())
        purge_dirty_pages_thread.join();
#endif
}

uint64_t MemoryWorker::getMemoryUsage(bool log_error)
{
    switch (source)
    {
        case MemoryUsageSource::Cgroups:
        {
            if (cgroups_reader != nullptr)
                return cgroups_reader->readMemoryUsage();
            [[fallthrough]];
        }
        case MemoryUsageSource::Jemalloc:
#if USE_JEMALLOC
            epoch_mib.setValue(0);
            return resident_mib.getValue();
#else
            [[fallthrough]];
#endif
        case MemoryUsageSource::None:
        {
            if (log_error)
                LOG_ERROR(getLogger("MemoryWorker"), "Trying to fetch memory usage while no memory source can be used");
            return 0;
        }
    }
}

void MemoryWorker::updateResidentMemoryThread()
{
    setThreadName("MemoryWorker");

    /// Set the bigget priority for this thread to avoid drift
    /// under the CPU starvation.
    OSThreadNiceValue::set(-20);

    std::chrono::milliseconds chrono_period_ms{rss_update_period_ms};
    [[maybe_unused]] bool first_run = true;

#if defined(OS_LINUX)
    std::optional<MemoryStatisticsOS> os_memory_stat;
    bool os_memory_stat_failed_logged = false;

    if (source == MemoryUsageSource::Jemalloc)
    {
        try
        {
            os_memory_stat.emplace();
        }
        catch (...)
        {
            os_memory_stat_failed_logged = true;
            tryLogCurrentException(log, "Cannot initialize OS memory statistics reader; will rely on jemalloc resident value");
        }
    }
#endif

    std::unique_lock lock(rss_update_mutex);
    while (true)
    {
        try
        {
            rss_update_cv.wait_for(lock, chrono_period_ms, [this] { return shutdown; });
            if (shutdown)
                return;

            Stopwatch total_watch;

            Int64 resident = getMemoryUsage(first_run);
            Int64 rss_for_tracker = resident;

#if defined(OS_LINUX)
            /// proton: starts
            /// jemalloc's `stats.resident` does not include all non-jemalloc allocations (e.g. large mmaps, stacks),
            /// so it should not be used as the process RSS baseline for global hard-limit enforcement.
            /// See timeplus-io/proton-enterprise PR #11409 discussion (comment r2726280537).
            /// proton: ends
            if (source == MemoryUsageSource::Jemalloc && os_memory_stat)
            {
                try
                {
                    rss_for_tracker = static_cast<Int64>(os_memory_stat->get().resident);
                }
                catch (...)
                {
                    if (!os_memory_stat_failed_logged)
                    {
                        os_memory_stat_failed_logged = true;
                        tryLogCurrentException(log, "Cannot read OS memory statistics; will rely on jemalloc resident value");
                    }
                }
            }
#endif

            MemoryTracker::updateRSS(rss_for_tracker);

#if USE_JEMALLOC
            const auto memory_tracker_limit = total_memory_tracker.getHardLimit();

            const bool needs_purge
                = (purge_total_memory_threshold_ratio > 0 && resident > memory_tracker_limit * purge_total_memory_threshold_ratio)
                || (purge_dirty_pages_threshold_ratio > 0
                    && static_cast<Int64>(pdirty_mib.getValue() * page_size) > memory_tracker_limit * purge_dirty_pages_threshold_ratio);

            if (needs_purge)
            {
                bool expected = false;
                if (purge_dirty_pages.compare_exchange_strong(expected, true, std::memory_order_relaxed))
                    purge_dirty_pages_cv.notify_all();
            }

            /// update MemoryTracker with `allocated` information from jemalloc when:
            ///  - it's a first run of MemoryWorker (MemoryTracker could've missed some allocation before its initialization)
            ///  - MemoryTracker stores a negative value
            ///  - `correct_tracker` is set to true
            if (unlikely(first_run || total_memory_tracker.get() < 0))
                MemoryTracker::updateAllocated(resident, /*log_change=*/true);
            else if (correct_tracker)
                MemoryTracker::updateAllocated(resident, /*log_change=*/false);
#else
            /// we don't update in the first run if we don't have jemalloc
            /// because we can only use resident memory information
            /// resident memory can be much larger than the actual allocated memory
            /// so we rather ignore the potential difference caused by allocated memory
            /// before MemoryTracker initialization
            if (unlikely(total_memory_tracker.get() < 0) || correct_tracker)
                MemoryTracker::updateAllocated(resident, /*log_change=*/false);
#endif

            ProfileEvents::increment(ProfileEvents::MemoryWorkerRun);
            ProfileEvents::increment(ProfileEvents::MemoryWorkerRunElapsedMicroseconds, total_watch.elapsedMicroseconds());
            first_run = false;
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to update resident memory");
        }
    }
}

#if USE_JEMALLOC
void MemoryWorker::purgeDirtyPagesThread()
{
    setThreadName("PurgeDirtyPages");

    std::unique_lock lock(purge_dirty_pages_mutex);
    while (true)
    {
        try
        {
            /// We add timeout of 1 second to protect against rare race condition where
            /// signal could be missed leading to this thread being stuck forever.
            /// We cannot use rss_update_mutex in RSS update thread because we want to keep them independent,
            /// i.e. purging dirty pages should not block RSS update.
            purge_dirty_pages_cv.wait_for(
                lock,
                std::chrono::seconds(1),
                [this] { return shutdown || purge_dirty_pages.load(std::memory_order_relaxed); });
            if (shutdown)
                return;

            bool expected = true;
            if (!purge_dirty_pages.compare_exchange_strong(expected, false, std::memory_order_relaxed))
                continue;

            Stopwatch purge_watch;
            purge_mib.run();
            ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
            ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, purge_watch.elapsedMicroseconds());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to purge dirty pages");
        }
    }
}
#endif

}
