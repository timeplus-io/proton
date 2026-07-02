#pragma once

#include <Cluster/LocalLog/Log/LogConfig.h>

#include <cstdint>
#include <string>
#include <unordered_map>

namespace DB
{

/// Applies user-facing logstore retention settings (0 = inherit defaults, negative = no limit) to a NativeLog config.
void applyLogstoreRetentionSettingsToLogConfig(
    int64_t logstore_retention_bytes_setting, int64_t logstore_retention_ms_setting, cluster::nlog::LogConfig & log_config);

/// Normalizes NativeLog alter() retention map in-place semantics:
/// - value == 0 means "inherit defaults" (resolved to the current defaults from config)
/// - value < 0 means "no limit" (resolved to 0 for NativeLog)
std::unordered_map<std::string, int64_t> normalizeNativeLogRetentionSettingsForAlter(
    const std::unordered_map<std::string, int64_t> & retention_settings, const cluster::nlog::LogConfig & defaults);

}
