#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Disks/StorageHelpers.h>
#include <Disks/getOrCreateDiskFromAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateDiskQuery.h>
#include <Parsers/ASTCreateDiskQuery.h>

/// proton: starts
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_DISK;
extern const int DISK_ALREADY_EXISTS;
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_DISK;
}

BlockIO InterpreterCreateDiskQuery::execute()
{
    auto & create_disk_query = query_ptr->as<ASTCreateDiskQuery &>();

    if (!create_disk_query.cluster.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ON CLUSTER is not implemented for CREATE Disk");

    /// Check Access
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_DISK);

    auto replace_if_exists = create_disk_query.or_replace;
    if (replace_if_exists)
        access_rights_elements.emplace_back(AccessType::DROP_DISK);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    /// check if disk exists
    try
    {
        if (current_context->getDisk(create_disk_query.name))
        {
            if (create_disk_query.if_not_exists)
                return {};
            else if (!replace_if_exists)
                throw Exception(ErrorCodes::DISK_ALREADY_EXISTS, "DISK '{}' already exists", create_disk_query.name);
        }
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_DISK)
            throw;
    }

    /// throw Exception if create_disk_query has errors
    auto timeout_ms = current_context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();

    auto disk_desc = getDiskDescriptorFromAST(create_disk_query, current_context);
    auto create_disk_req = std::make_shared<cluster::CreateDiskRequest>(
        std::move(disk_desc),
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/2);
    auto create_disk_resp = metastore.createDisk(std::move(create_disk_req));
    if (create_disk_resp->hasError())
    {
        const auto & err = create_disk_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_CREATE_DISK,
            "Failed to create disk {}: error_code={} error_message={}",
            create_disk_query.name,
            err.error_code,
            err.error_message);
    }

    LOG_INFO(getLogger("InterpreterCreateDiskQuery"), "Created custom disk {}", create_disk_query.name);

    return {};
}

}
