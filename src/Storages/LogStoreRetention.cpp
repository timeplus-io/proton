#include <Storages/LogStoreRetention.h>

#include <limits>

namespace DB
{
namespace
{
int64_t clampU64ToI64(uint64_t value)
{
    constexpr auto int64_max_as_u64 = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    return (value > int64_max_as_u64) ? std::numeric_limits<int64_t>::max() : static_cast<int64_t>(value);
}
}

void applyLogstoreRetentionSettingsToLogConfig(
    int64_t logstore_retention_bytes_setting, int64_t logstore_retention_ms_setting, cluster::nlog::LogConfig & log_config)
{
    if (logstore_retention_bytes_setting != 0)
    {
        if (logstore_retention_bytes_setting < 0)
            log_config.retention_size = 0;
        else
            log_config.retention_size = static_cast<uint64_t>(logstore_retention_bytes_setting);
    }

    if (logstore_retention_ms_setting != 0)
    {
        if (logstore_retention_ms_setting < 0)
            log_config.retention_ms = 0;
        else
            log_config.retention_ms = logstore_retention_ms_setting;
    }
}

std::unordered_map<std::string, int64_t> normalizeNativeLogRetentionSettingsForAlter(
    const std::unordered_map<std::string, int64_t> & retention_settings, const cluster::nlog::LogConfig & defaults)
{
    auto effective_retention_settings = retention_settings;

    if (auto it = effective_retention_settings.find("retention_bytes"); it != effective_retention_settings.end())
    {
        if (it->second == 0)
            it->second = clampU64ToI64(defaults.retention_size);
        else if (it->second < 0)
            it->second = 0;
    }

    if (auto it = effective_retention_settings.find("retention_ms"); it != effective_retention_settings.end())
    {
        if (it->second == 0)
            it->second = defaults.retention_ms;
        else if (it->second < 0)
            it->second = 0;
    }

    return effective_retention_settings;
}

}
