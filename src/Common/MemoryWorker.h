#pragma once

#include <Common/CgroupsMemoryUsageObserver.h>
#include <Common/ThreadPool.h>
#include <Common/Jemalloc.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace DB
{

struct ICgroupsReader
{
    enum class CgroupsVersion : uint8_t
    {
        V1,
        V2
    };

#if defined(OS_LINUX)
    static std::pair<std::string, CgroupsVersion> getCgroupsPath();
    static std::shared_ptr<ICgroupsReader>
    createCgroupsReader(ICgroupsReader::CgroupsVersion version, const std::filesystem::path & cgroup_path);
#endif

    virtual ~ICgroupsReader() = default;

    virtual uint64_t readMemoryUsage() = 0;

    virtual std::string dumpAllStats() = 0;
};

struct MemoryWorkerConfig
{
    uint64_t rss_update_period_ms = 0;
    double purge_dirty_pages_threshold_ratio = 0.0;
    double purge_total_memory_threshold_ratio = 0.0;
    bool correct_tracker = false;
    bool use_cgroup = true;
};

/// Correct MemoryTracker based on external information (e.g. Cgroups or stats.resident from jemalloc)
/// The worker spawns a background thread which periodically reads current resident memory from the source,
/// whose value is sent to global MemoryTracker.
/// It can do additional things like purging jemalloc dirty pages if the current memory usage is higher than global hard limit.
class MemoryWorker
{
public:
    explicit MemoryWorker(MemoryWorkerConfig config);

    enum class MemoryUsageSource : uint8_t
    {
        None,
        Cgroups,
        Jemalloc
    };

    MemoryUsageSource getSource();

    void start();

    ~MemoryWorker();
private:
    uint64_t getMemoryUsage(bool log_error);

    void updateResidentMemoryThread();
#if USE_JEMALLOC
    void purgeDirtyPagesThread();
#endif

    ThreadFromGlobalPool update_resident_memory_thread;
#if USE_JEMALLOC
    ThreadFromGlobalPool purge_dirty_pages_thread;
#endif

    std::mutex rss_update_mutex;
    std::condition_variable rss_update_cv;

    std::mutex purge_dirty_pages_mutex;
    std::condition_variable purge_dirty_pages_cv;

    bool shutdown = false;

    LoggerPtr log;

    uint64_t rss_update_period_ms;

    bool correct_tracker = false;

    double purge_total_memory_threshold_ratio;
    double purge_dirty_pages_threshold_ratio;
    uint64_t page_size = 0;

    std::atomic<bool> purge_dirty_pages{false};

    MemoryUsageSource source{MemoryUsageSource::None};

    std::shared_ptr<ICgroupsReader> cgroups_reader;

#if USE_JEMALLOC
    JemallocMibCache<uint64_t> epoch_mib{"epoch"};
    JemallocMibCache<size_t> resident_mib{"stats.resident"};
    JemallocMibCache<size_t> pagesize_mib{"arenas.page"};

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
    JemallocMibCache<size_t> pdirty_mib{"stats.arenas." STRINGIFY(MALLCTL_ARENAS_ALL) ".pdirty"};
    JemallocMibCache<size_t> purge_mib{"arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge"};
#undef STRINGIFY
#undef STRINGIFY_HELPER
#endif
};

}
