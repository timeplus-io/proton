#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Disks/StorageHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateStoragePolicyQuery.h>
#include <Parsers/ASTCreateStoragePolicyQuery.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_STORAGE_POLICY;
extern const int STORAGE_POLICY_ALREADY_EXISTS;
extern const int UNKNOWN_POLICY;
}

BlockIO InterpreterCreateStoragePolicyQuery::execute()
{
    auto & create_policy_query = query_ptr->as<ASTCreateStoragePolicyQuery &>();

    if (!create_policy_query.cluster.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ON CLUSTER is not implemented for CREATE STORAGE POLICY");

    /// Check Access
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_STORAGE_POLICY);

    auto replace_if_exists = create_policy_query.or_replace;
    if (replace_if_exists)
        access_rights_elements.emplace_back(AccessType::DROP_STORAGE_POLICY);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    auto policy_config = getStoragePolicyConfigurationFromYaml(create_policy_query.name, create_policy_query.yaml_config);

    /// Throws Exception if the configuration of storage policy is invalid
    auto policy_desc = getStoragePolicyDescriptorFromConfiguration(create_policy_query.name, policy_config, current_context);

    /// check if policy exists
    try
    {
        if (current_context->getStoragePolicy(create_policy_query.name))
        {
            if (create_policy_query.if_not_exists)
                return {};
            else if (!replace_if_exists)
                throw Exception(ErrorCodes::STORAGE_POLICY_ALREADY_EXISTS, "Storage policy '{}' already exists", create_policy_query.name);
        }
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::UNKNOWN_POLICY)
            throw;
    }

    auto timeout_ms = current_context->getSettingsRef().query_timeout_sec * 1000;
    /// proton: starts
    /// Single-instance mode: use MetaStore directly
    auto & metastore = Globals::getMetaStore();
    /// proton: ends
    auto create_policy_req = std::make_shared<cluster::CreateStoragePolicyRequest>(
        std::move(policy_desc),
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/2);
    auto create_policy_resp = metastore.createStoragePolicy(std::move(create_policy_req));
    if (create_policy_resp->hasError())
    {
        const auto & err = create_policy_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_CREATE_STORAGE_POLICY,
            "Failed to create storage policy {}: error_code={} error_message={}",
            create_policy_query.name,
            err.error_code,
            err.error_message);
    }

    LOG_INFO(getLogger("InterpreterCreateStoragePolicyQuery"), "Created storage policy {}", create_policy_query.name);

    return {};
}

}
