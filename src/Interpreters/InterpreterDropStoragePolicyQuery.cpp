#include <Interpreters/InterpreterDropStoragePolicyQuery.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTDropStoragePolicyQuery.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int CANNOT_DELETE_STORAGE_POLICY;
}

BlockIO InterpreterDropStoragePolicyQuery::execute()
{
    ASTDropStoragePolicyQuery & drop_policy_query = query_ptr->as<ASTDropStoragePolicyQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_STORAGE_POLICY);

    auto current_context = getContext();

    if (!drop_policy_query.cluster.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ON CLUSTER is not implemented for DROP STORAGE POLICY");

    current_context->checkAccess(access_rights_elements);

    /// Check if the storage policy is still in use
    if (drop_policy_query.name == "default")
        throw Exception(ErrorCodes::CANNOT_DELETE_STORAGE_POLICY, "Cannot drop 'default' storage policy");

    const auto used_by = DatabaseCatalog::instance().getStoragesByStoragePolicy(drop_policy_query.name);
    if (used_by.size() > 0)
        throw Exception(ErrorCodes::CANNOT_DELETE_STORAGE_POLICY, "Cannot drop storage policy '{}', it used by: {}", drop_policy_query.name, fmt::join(used_by, ", "));

    /// Drop from metastore (this request also trigger to drop storage policy from memory in MetadataUpdater
    auto timeout_ms = current_context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();

    auto delete_policy_req = std::make_shared<cluster::DeleteStoragePolicyRequest>(
        std::string(drop_policy_query.name),
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/2);
    auto delete_policy_resp = metastore.deleteStoragePolicy(delete_policy_req);
    if (delete_policy_resp->hasError())
    {
        const auto & err = delete_policy_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_DELETE_STORAGE_POLICY,
            "Failed to delete storage policy {}: error_code={} error_message={}",
            delete_policy_req->data().name,
            err.error_code,
            err.error_message);
    }

    return {};
}

}
