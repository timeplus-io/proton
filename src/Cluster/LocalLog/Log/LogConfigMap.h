#pragma once

#include <Cluster/Common/StreamIDShard.h>
#include <Cluster/LocalLog/Log/LogConfig.h>

#include <absl/container/flat_hash_map.h>

namespace cluster::nlog
{
struct LogConfigEntry
{
    LogConfigEntry(LogConfigPtr config_) : config(std::move(config_)) { }

    LogConfigPtr config;
};

using LogConfigMap = absl::flat_hash_map<Stream, LogConfigEntry>;
}
