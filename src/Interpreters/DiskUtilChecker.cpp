#include "DiskUtilChecker.h"

#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int DISK_USAGE_RATIO_THRESHOLD_EXCEEDED;
}

DiskUtilChecker::DiskUtilChecker(const ContextPtr & global_context_)
    : WithContext(global_context_), log(&Poco::Logger::get("AsynchronousMetrics"))
{
    max_local_disk_usage_ratio = global_context_->getConfigRef().getDouble("max_local_disk_usage_ratio", 0.9);
    updateUtils(global_context_->getDisksMap());
}

/// We don't need hold locks here since we assume local disk volumes can't be changed
/// during the lifecycle of proton. We don't even need make the util atomic since slightly
/// out of sync in different CPUs doesn't matter in this case in practice.
void DiskUtilChecker::updateUtils(const DisksMap & disks_map)
{
    for (const auto & [name, disk] : disks_map)
    {
        auto total = disk->getTotalSpace();
        auto available = disk->getAvailableSpace();
        disk_utils[name] = ((total - available) * 1.0) / total;
    }
}

void DiskUtilChecker::check(const std::string & disk_name) const
{
    if (disk_name.empty())
    {
        for (const auto & [name, util] : disk_utils)
        {
            if (util >= max_local_disk_usage_ratio)
            {
                LOG_ERROR(log, "Disk {} utilization is {}, exceeds the max_disk_util {}", name, util, max_local_disk_usage_ratio);
                throw Exception(
                    ErrorCodes::DISK_USAGE_RATIO_THRESHOLD_EXCEEDED,
                    "Disk {} utilization is {}, exceeds the max_disk_util {}",
                    name,
                    util,
                    max_local_disk_usage_ratio);
            }
        }
        return;
    }

    auto it = disk_utils.find(disk_name);
    if (it != disk_utils.end() && it->second >= max_local_disk_usage_ratio)
    {
        LOG_ERROR(log, "Disk {} utilization is {} bytes, exceeds the max_disk_util {}", disk_name, it->second, max_local_disk_usage_ratio);
        throw Exception(
            ErrorCodes::DISK_USAGE_RATIO_THRESHOLD_EXCEEDED,
            "Disk {} utilization is {} bytes, exceeds the max_disk_util {}",
            disk_name,
            it->second,
            max_local_disk_usage_ratio);
    }
}
}
