#include <Disks/StoragePolicy.h>

#include <Bootstrap/Globals.h>
/// proton: starts
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListStoragePoliciesRequest.h>
/// proton: ends
#include <Cluster/Requests/ListStoragePoliciesResponse.h>
#include <Disks/StorageHelpers.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

void StoragePolicySelector::loadStoragePoliciesFromMetaStore(DiskSelectorPtr disks)
{
    /// proton: starts
    /// Single-instance mode: use MetaStore directly
    auto & metastore = Globals::getMetaStore();
    auto req = std::make_shared<cluster::ListStoragePoliciesRequest>(
        /*name_*/ "",  // Empty to list all
        /*initiator_*/ 1,  // Single-instance node ID
        /*consistent_read_*/ false,
        /*timeout_ms_*/ 30000,
        /*request_version*/ 1);
    auto resp = metastore.listStoragePolicies(req);
    /// proton: ends
    if (resp->hasError())
    {
        LOG_ERROR(
            getLogger("StoragePolicySelector"),
            "Failed to get storage policies from MetaStore, reason={}",
            resp->data().err.string());

        throw Exception(
            resp->data().err.error_code, "Failed to get storage policies from MetaStore, reason={}", resp->data().err.error_message);
    }

    for (const auto & policy_desc : resp->data().policies)
    {
        if (!std::all_of(policy_desc->name.begin(), policy_desc->name.end(), isWordCharASCII))
            throw Exception(
                ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                "Storage policy name can contain only alphanumeric and '_' ({})",
                backQuote(policy_desc->name));

        policies.emplace(
            policy_desc->name,
            std::make_shared<StoragePolicy>(
                policy_desc->name, *getStoragePolicyConfigurationFromDescriptor(policy_desc), policy_desc->name, disks));
    }
}

}
