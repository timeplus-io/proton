#include "DiskLocal.h"
#include "DiskSelector.h"

#include <IO/WriteHelpers.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

#include <set>

/// proton: starts
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListDisksRequest.h>
#include <Cluster/Requests/ListDisksResponse.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/getOrCreateDiskFromAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_DISK;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_LOAD_CONFIG;
    extern const int CANNOT_DELETE_DISK; /// proton: udpated
}


void DiskSelector::assertInitialized() const
{
    if (!is_initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskSelector not initialized");
}


void DiskSelector::initialize(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    auto & factory = DiskFactory::instance();

    constexpr auto default_disk_name = "default";

    /// proton: starts. load disks from MetaStore
    bool has_default_disk = false;
    has_default_disk = loadDisksFromMetaStore(context);
    /// proton: ends

    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Disk name can contain only alphanumeric and '_' ({})", disk_name);

        if (disk_name == default_disk_name)
            has_default_disk = true;

        auto disk_config_prefix = config_prefix + "." + disk_name;

        disks.emplace(disk_name, factory.create(disk_name, config, disk_config_prefix, context, disks));
    }
    if (!has_default_disk)
    {
        disks.emplace(
            default_disk_name,
            std::make_shared<DiskLocal>(
                default_disk_name, context->getPath(), 0, context, config, config_prefix));
    }

    is_initialized = true;
}


DiskSelectorPtr DiskSelector::updateFromConfig(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context) const
{
    assertInitialized();

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    auto & factory = DiskFactory::instance();

    std::shared_ptr<DiskSelector> result = std::make_shared<DiskSelector>(*this);

    constexpr auto default_disk_name = "default";
    DisksMap old_disks_minus_new_disks(result->getDisksMap());

    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Disk name can contain only alphanumeric and '_' ({})", disk_name);

        auto disk_config_prefix = config_prefix + "." + disk_name;
        if (!result->getDisksMap().contains(disk_name))
        {
            result->addToDiskMap(disk_name, factory.create(disk_name, config, disk_config_prefix, context, result->getDisksMap()));
        }
        else
        {
            auto disk = old_disks_minus_new_disks[disk_name];

            disk->applyNewSettings(config, context, disk_config_prefix, result->getDisksMap());

            old_disks_minus_new_disks.erase(disk_name);
        }
    }

    old_disks_minus_new_disks.erase(default_disk_name);

    if (!old_disks_minus_new_disks.empty())
    {
        WriteBufferFromOwnString warning;
        if (old_disks_minus_new_disks.size() == 1)
            writeString("Disk ", warning);
        else
            writeString("Disks ", warning);

        int num_disks_removed_from_config = 0;
        for (const auto & [name, disk] : old_disks_minus_new_disks)
        {
            /// Custom disks are not present in config.
            if (disk->isCustomDisk())
                continue;

            if (num_disks_removed_from_config++ > 0)
                writeString(", ", warning);

            writeBackQuotedString(name, warning);
        }

        if (num_disks_removed_from_config > 0)
        {
            LOG_WARNING(
                getLogger("DiskSelector"),
                "{} disappeared from configuration, this change will be applied after restart of ClickHouse",
                warning.str());
        }
    }

    return result;
}


DiskPtr DiskSelector::tryGet(const String & name) const
{
    assertInitialized();
    auto it = disks.find(name);
    if (it == disks.end())
        return nullptr;
    return it->second;
}

DiskPtr DiskSelector::get(const String & name) const
{
    auto disk = tryGet(name);
    if (!disk)
        throw Exception(ErrorCodes::UNKNOWN_DISK, "Unknown disk {}", name);
    return disk;
}

const DisksMap & DiskSelector::getDisksMap() const
{
    assertInitialized();
    return disks;
}


void DiskSelector::addToDiskMap(const String & name, DiskPtr disk)
{
    assertInitialized();
    auto [_, inserted] = disks.emplace(name, disk);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk with name `{}` is already in disks map", name);
}


void DiskSelector::shutdown()
{
    assertInitialized();
    for (auto & e : disks)
        e.second->shutdown();
}

/// proton: starts
bool DiskSelector::loadDisksFromMetaStore(ContextPtr context)
{
    auto & factory = DiskFactory::instance();
    constexpr auto default_disk_name = "default";
    bool has_default_disk = false;

    /// proton: starts
    /// Single-instance mode: create request with required parameters
    auto req = std::make_shared<cluster::ListDisksRequest>(
        "",      // disk_name (empty for listing all)
        0,       // initiator (single-instance: always 0)
        false,   // consistent_read
        30000,   // timeout_ms
        1        // request_version
    );
    auto & metastore = Globals::getMetaStore();
    auto resp = metastore.listDisks(req);
    /// proton: ends
    if (resp->hasError())
    {
        LOG_ERROR(getLogger("DiskSelector"), "Failed to get disks from MetaStore, reason={}", resp->data().err.string());
        throw Exception(resp->data().err.error_code, "Failed to get disks from MetaStore, reason={}", resp->data().err.error_message);
    }

    for (const auto & disk_desc : resp->data().disks)
    {
        if (!std::all_of(disk_desc->name.begin(), disk_desc->name.end(), isWordCharASCII))
            throw Exception(
                ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Disk name can contain only alphanumeric and '_' ({})", disk_desc->name);

        if (disk_desc->name == default_disk_name)
            has_default_disk = true;

        ASTPtr disk_func;
        ParserFunction disk_func_p;
        disk_func = parseQuery(disk_func_p, disk_desc->config, 0, 0);

        /// Parse config to disk function (AST)
        if (!disk_func || disk_func->as<ASTFunction>()->name != "disk")
            throw Exception(
                ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "disk {} with invalid config: [{}]", disk_desc->name, disk_desc->config);

        const auto & ast_function = assert_cast<const ASTFunction &>(*disk_func);
        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(ast_function.arguments.get());
        const auto & function_args = function_args_expr->children;
        auto disk_config = getDiskConfigurationFromAST(disk_desc->name, function_args, context);
        disks.emplace(disk_desc->name, factory.create(disk_desc->name, *disk_config, disk_desc->name, context, disks));
    }

    return has_default_disk;
}

bool DiskSelector::isBusy(const DB::String & name) const
{
    return (DatabaseCatalog::instance().getStoragesByDisk(name).size() > 0);
}

void DiskSelector::removeFromDiskMap(const DB::String & name)
{
    assertInitialized();
    auto it = disks.find(name);
    if (it == disks.end())
        throw Exception(ErrorCodes::UNKNOWN_DISK, "Unknown disk {}", name);

    if (isBusy(name))
        throw Exception(ErrorCodes::CANNOT_DELETE_DISK, "disk {} is used by other streams, cannot be removed", name);
    it->second->shutdown();
    disks.erase(it);
}
/// proton: ends

}
