#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropDiskQuery.h>

#include <Access/ContextAccess.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Parsers/ASTDropDiskQuery.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_DELETE_DISK;
}

BlockIO InterpreterDropDiskQuery::execute()
{
    ASTDropDiskQuery & drop_disk_query = query_ptr->as<ASTDropDiskQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_DISK);

    auto current_context = getContext();

    if (!drop_disk_query.cluster.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ON CLUSTER is not implemented for DROP FORMAT SCHEMA");

    current_context->checkAccess(access_rights_elements);

    /// Check if the storage policy is still in use
    if (drop_disk_query.name == "default")
        throw Exception(ErrorCodes::CANNOT_DELETE_DISK, "Cannot drop 'default' disk");

    const auto used_by = DatabaseCatalog::instance().getStoragesByDisk(drop_disk_query.name);
    if (used_by.size() > 0)
        throw Exception(
            ErrorCodes::CANNOT_DELETE_DISK, "Cannot drop disk '{}', it used by: {}", drop_disk_query.name, fmt::join(used_by, ", "));

    /// Drop from metastore (this request also trigger to drop storage policy from memory in MetadataUpdater
    auto timeout_ms = current_context->getSettingsRef().query_timeout_sec * 1000;
    /// proton: starts
    /// Single-instance mode: use MetaStore directly
    auto & metastore = Globals::getMetaStore();
    /// proton: ends
    auto delete_disk_req = std::make_shared<cluster::DeleteDiskRequest>(
        std::string(drop_disk_query.name),
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/2);
    auto delete_disk_resp = metastore.deleteDisk(delete_disk_req);
    if (delete_disk_resp->hasError())
    {
        const auto & err = delete_disk_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_DELETE_DISK,
            "Failed to delete disk {}: error_code={} error_message={}",
            delete_disk_req->data().name,
            err.error_code,
            err.error_message);
    }

    return {};
}

}
