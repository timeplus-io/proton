#pragma once

#include <Cluster/LocalLog/Log/FetchConfig.h>
#include <Cluster/LocalLog/Log/LogConfig.h>

#include <filesystem>

namespace cluster
{
struct StoreConfig
{
    explicit StoreConfig(std::vector<std::filesystem::path> data_dirs_)
        : data_dirs(std::move(data_dirs_))
        , log_config(std::make_shared<nlog::LogConfig>())
        , log_fetch_config(std::make_shared<nlog::FetchConfig>())
    {
    }

    void validate();

    std::string string() const;

    std::vector<std::filesystem::path> data_dirs;

    uint64_t metadata_keep_versions = 10; /// For meta store only

    /// Max number of execution failures for updater stop retry; 0 means retry failure forever.
    uint64_t metadata_max_retries = 10;

    bool check_crcs = false;

    uint64_t retention_check_ms = 5 * 60 * 1000;


    nlog::LogConfigPtr log_config;
    nlog::FetchConfigPtr log_fetch_config;
};

using StoreConfigPtr = std::shared_ptr<StoreConfig>;
}
