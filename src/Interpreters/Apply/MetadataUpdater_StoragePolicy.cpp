#include <Interpreters/Apply/MetadataUpdater.h>

#include <Access/AccessControl.h>
#include <Access/AccessEntityIO.h>
#include <Access/MetaStoreAccessStorage.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/CreateStoragePolicyRequestData.h>
#include <Cluster/Protocol/DeleteStoragePolicyRequestData.h>
#include <Cluster/Requests/CreateStoragePolicyRequest.h>
#include <Cluster/Requests/DeleteStoragePolicyRequest.h>
#include <Disks/StorageHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int BAD_REQUEST_PARAMETER;
}

void MetadataUpdater::handleCreateStoragePolicy(
    const cluster::CreateStoragePolicyRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    try
    {
        const auto & request_data = request.data();
        auto query_context = Context::createCopy(Globals::getGlobalContext().getGlobalContext());
        createStoragePolicyFromDescriptor(request_data.policy, query_context);

        /// Save/replace storage policy in MetaDB.
        auto & metastore = Globals::getMetaStore();
        auto & db = metastore.getMetaDB();
        auto res = db.saveStoragePolicy(*request_data.policy, cluster::AppliedSequence(sn));
        if (res.hasError())
            LOG_ERROR(logger, "Failed to create storage policy: {}", res.error_message);

        meta_store->ackProposal(request_header.correlationID(), sn, res.error_code, std::move(res.error_message));
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(logger, "Failed to create storage policy: error={}", ex.message());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, ex.code(), std::string{ex.message()});
    }
}

void MetadataUpdater::handleDeleteStoragePolicy(
    const cluster::DeleteStoragePolicyRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    try
    {
        const auto & request_data = request.data();

        /// Delete in memory
        global_context->deleteStoragePolicy(request_data.name);

        /// Delete in MetaDB
        auto & metastore = Globals::getMetaStore();
        auto & db = metastore.getMetaDB();
        auto res = db.deleteStoragePolicy(request_data.name, cluster::AppliedSequence(sn));
        if (res.hasError())
            LOG_ERROR(logger, "Failed to delete storage policy {}: {}", request_data.name, res.error_message);
        else
            LOG_INFO(logger, "Delete delete storage policy {} successfully", request_data.name);

        meta_store->ackProposal(request_header.correlationID(), sn, res.error_code, std::move(res.error_message));
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(logger, "Failed to delete storage policy, error={}", ex.message());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, ex.code(), std::string{ex.message()});
    }
}

}
