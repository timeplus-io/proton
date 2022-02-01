#pragma once

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
    static const int32_t MAX_ADHOC_SCHEDULE_THREAD = 2;

    inline static const fs::path META_DIR = "/var/lib/proton/native_log/meta";
};

struct NativeLogConfig
{
    MetaStoreConfig metaStoreConfig() const
    {
        MetaStoreConfig meta_config;
        meta_config.meta_dir = meta_dir;
        return meta_config;
    }

    LogConfigPtr logConfig() const { return std::make_shared<LogConfig>(); }

    LogManagerConfig logManagerConfig() const { return LogManagerConfig{}; }

    LogCompactorConfig logCompactorConfig() const { return LogCompactorConfig{}; }

    int32_t max_schedule_threads = NativeLogSettingDefaults::MAX_SCHEDULE_THREAD;

    int32_t max_adhoc_schedule_threads = NativeLogSettingDefaults::MAX_ADHOC_SCHEDULE_THREAD;

    fs::path meta_dir;
    std::vector<fs::path> log_dirs;
};

}
