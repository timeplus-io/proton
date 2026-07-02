#include <Cluster/StoreConfig.h>

#include <Common/Exception.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

namespace cluster
{
void StoreConfig::validate()
{
    if (data_dirs.empty())
        throw DB::Exception(DB::ErrorCodes::INVALID_CONFIG_PARAMETER, "Data dirs are not configured");

    if (retention_check_ms == 0)
        retention_check_ms = 5 * 60 * 1000;


    if (metadata_keep_versions > 127)
        metadata_keep_versions = 127;



    log_config->validate();
    log_fetch_config->validate();
}

std::string StoreConfig::string() const
{
    std::vector<const char *> dirs;
    dirs.reserve(data_dirs.size());

    for (const auto & data_dir : data_dirs)
        dirs.push_back(data_dir.c_str());

    return fmt::format(
        "data_dirs=[{}] metadata_keep_versions={} check_crcs={} retention_check_ms={} "
        "log_config={{{}}} log_fetch_config={{{}}}",
        fmt::join(dirs, ", "),
        metadata_keep_versions,
        check_crcs,
        retention_check_ms,
        log_config->string(),
        log_fetch_config->string());
}
}
