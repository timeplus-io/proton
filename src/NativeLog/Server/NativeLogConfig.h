#pragma once

#include <NativeLog/Cache/TailCacheConfig.h>
#include <NativeLog/Log/FetchConfig.h>
#include <NativeLog/Log/LogCompactorConfig.h>
#include <NativeLog/Log/LogConfig.h>
#include <NativeLog/Log/LogManagerConfig.h>
#include <NativeLog/MetaStore/MetaStoreConfig.h>

#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

namespace nlog
{
struct NativeLogSettingDefaults
{
    static const int32_t MAX_SCHEDULE_THREAD = 10;
    static const int32_t MAX_ADHOC_SCHEDULE_THREAD = 8;

    inline static const fs::path META_DIR = "/var/lib/proton/nativelog/meta";
    inline static const fs::path LOG_DIR = "/var/lib/proton/nativelog/log";
};

struct NativeLogConfig
{
    MetaStoreConfig metaStoreConfig() const
    {
        MetaStoreConfig meta_config;
        meta_config.meta_dir = meta_dir;

        if (meta_config.meta_dir.empty())
            meta_config.meta_dir = NativeLogSettingDefaults::META_DIR;

        return meta_config;
    }

    LogConfigPtr logConfig() const { return std::make_shared<LogConfig>(log_config); }

    const LogManagerConfig & logManagerConfig() const { return log_mgr_config; }

    const LogCompactorConfig & logCompactorConfig() const { return log_compactor_config; }

    fs::path meta_dir;
    std::vector<fs::path> log_dirs;

    bool check_crcs = false;
    int32_t max_schedule_threads = NativeLogSettingDefaults::MAX_SCHEDULE_THREAD;
    int32_t max_adhoc_schedule_threads = NativeLogSettingDefaults::MAX_ADHOC_SCHEDULE_THREAD;

    TailCacheConfig cache_config;
    FetchConfig fetch_config;
    LogConfig log_config;
    LogManagerConfig log_mgr_config;
    LogCompactorConfig log_compactor_config;

    std::string string() const
    {
        auto log_dirs_str = [this]() {
            std::string log_dirs_s;
            for (const auto & p : log_dirs)
                log_dirs_s += p.string();
            return log_dirs_s;
        };

        return fmt::format(
            "meta_dir={} log_dirs={} check_crcs={} max_schedule_threads={} max_adhoc_schedule_threads={} {} {} {} {} {}",
            meta_dir.c_str(),
            log_dirs_str(),
            check_crcs,
            max_schedule_threads,
            max_adhoc_schedule_threads,
            cache_config.string(),
            fetch_config.string(),
            log_config.string(),
            log_mgr_config.string(),
            log_compactor_config.string());
    }
};

}
