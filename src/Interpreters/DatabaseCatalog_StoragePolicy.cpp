#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
///#include <Interpreters/TableNameHints.h>
#include <Storages/IStorage.h>
#include <Databases/IDatabase.h>
///#include <Databases/DatabaseMemory.h>
///#include <Disks/IDisk.h>
///#include <Core/BackgroundSchedulePool.h>
///#include <Parsers/formatAST.h>
///#include <IO/ReadHelpers.h>
///#include <Poco/DirectoryIterator.h>
///#include <Common/renameat2.h>
///#include <Common/noexcept_scope.h>

#include <Disks/IStoragePolicy.h>

namespace DB
{

void DatabaseCatalog::addStoragePolicyInUse(StorageID storage_id, const StoragePolicyPtr & storage_policy)
{
    String full_name = storage_id.getFullTableName();
    std::lock_guard lock{storage_policies_in_use.mutex};
    storage_policies_in_use.map[storage_policy->getName()].insert(full_name);
}

void DatabaseCatalog::removeStoragePolicyInUse(StorageID storage_id, const StoragePolicyPtr & storage_policy)
{
    String full_name = storage_id.getFullTableName();
    std::lock_guard lock{storage_policies_in_use.mutex};

    auto it = storage_policies_in_use.map.find(storage_policy->getName());
    if (it == storage_policies_in_use.map.end())
        return;

    auto & storage_set = it->second;
    storage_set.erase(full_name);

    if (storage_set.empty())
        storage_policies_in_use.map.erase(it);
}

std::vector<String> DatabaseCatalog::getStoragesByStoragePolicy(const String & policy_name) const
{
    std::lock_guard lock{storage_policies_in_use.mutex};
    auto it = storage_policies_in_use.map.find(policy_name);
    if (it == storage_policies_in_use.map.end())
        return {};

    std::vector<String> result;
    result.reserve(it->second.size());
    for (const auto & name : it->second)
        result.push_back(name);
    return result;
}

std::vector<String> DatabaseCatalog::getStoragesByDisk(const String & disk_name) const
{
    std::lock_guard lock{disks_in_use.mutex};
    auto it = disks_in_use.map.find(disk_name);
    if (it == disks_in_use.map.end())
        return {};

    std::vector<String> result;
    result.reserve(it->second.size());
    for (const auto & name : it->second)
        result.push_back(name);
    return result;
}

void DatabaseCatalog::addDisksInUse(const String & storage_policy_name, const std::vector<String> & disk_names)
{
    std::lock_guard lock{disks_in_use.mutex};

    for (const auto & disk_name : disk_names)
        disks_in_use.map[disk_name].insert(storage_policy_name);
}

void DatabaseCatalog::removeDisksInUse(const String & storage_policy_name, const std::vector<String> & disk_names)
{
    std::lock_guard lock{disks_in_use.mutex};

    for (auto & disk_name : disk_names)
    {
        auto it = disks_in_use.map.find(disk_name);
        if (it == disks_in_use.map.end())
            return;

        auto & storage_set = it->second;
        storage_set.erase(storage_policy_name);

        if (storage_set.empty())
            disks_in_use.map.erase(it);
    }
}
}
