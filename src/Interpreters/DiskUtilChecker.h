#pragma once

#include <Interpreters/Context_fwd.h>

#include <unordered_map>

namespace Poco
{
class Logger;
}

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
using DisksMap = std::map<String, DiskPtr>;

class DiskUtilChecker final : WithContext
{
public:
    static DiskUtilChecker & instance(const ContextPtr & global_context)
    {
        static DiskUtilChecker checker{global_context};
        return checker;
    }

    /// Check if disk utilization reaches the max limit.
    /// If @param disk_name is empty,
    /// check all of local disks utilizations,
    /// if any of them reaches the maximum limit, throw Exception
    void check(const std::string & disk_name = "") const;

    void updateUtils(const DisksMap & disks_map);

private:
    explicit DiskUtilChecker(const ContextPtr & global_context_);

    Poco::Logger * log;

    /// Max utilization for each disk volume
    double max_local_disk_usage_ratio;
    std::unordered_map<std::string, double> disk_utils;
};

}
