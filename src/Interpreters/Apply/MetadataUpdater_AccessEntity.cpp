#include <Interpreters/Apply/MetadataUpdater.h>

#include <Access/AccessControl.h>
#include <Access/AccessEntityIO.h>
#include <Access/MetaStoreAccessStorage.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/CreateAccessEntityRequestData.h>
#include <Cluster/Protocol/DeleteAccessEntityRequestData.h>
#include <Cluster/Requests/CreateAccessEntityRequest.h>
#include <Cluster/Requests/DeleteAccessEntityRequest.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int BAD_REQUEST_PARAMETER;
}

void MetadataUpdater::handleCreateAccessEntity(
    const cluster::CreateAccessEntityRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    try
    {
        const auto & request_data = request.data();
        const auto entity = request_data.desc.entity;
        auto metastore_storage = Globals::getGlobalContext().getAccessControl()->getMetaStoreStorage();
        auto res = metastore_storage->insertLocal(
            request_data.desc.id, request_data.desc.data_version, entity, request_data.replace_if_exists, sn);
        if (res.hasError())
            LOG_ERROR(logger, "Failed to create access entity: {}", res.error_message);

        meta_store->ackProposal(request_header.correlationID(), sn, res.error_code, std::move(res.error_message));
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(logger, "Failed to create access entity: error={}", ex.message());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, ex.code(), std::string{ex.message()});
    }
}

void MetadataUpdater::handleDeleteAccessEntity(
    const cluster::DeleteAccessEntityRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    try
    {
        const auto & request_data = request.data();
        auto metastore_storage = Globals::getGlobalContext().getAccessControl()->getMetaStoreStorage();
        auto res = metastore_storage->removeLocal(request_data.id, sn);
        if (res.hasError())
            LOG_ERROR(logger, "Failed to delete access entity: {}", res.error_message);

        meta_store->ackProposal(request_header.correlationID(), sn, res.error_code, std::move(res.error_message));
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(logger, "Failed to delete access entity: error={}", ex.message());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, ex.code(), std::string{ex.message()});
    }
}

void MetadataUpdater::handleUpdateAccessEntity(
    const cluster::UpdateAccessEntityRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    try
    {
        const auto & request_data = request.data();
        const auto id = request_data.new_entity.id;
        const auto new_entity = request_data.new_entity.entity;
        auto metastore_storage = Globals::getGlobalContext().getAccessControl()->getMetaStoreStorage();
        auto res = metastore_storage->updateLocal(id, request_data.version_before_update, new_entity, sn);
        if (res.hasError())
            LOG_ERROR(logger, "Failed to update access entity: {}", res.error_message);

        meta_store->ackProposal(request_header.correlationID(), sn, res.error_code, std::move(res.error_message));
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(logger, "Failed to update access entity: error={}", ex.message());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, ex.code(), std::string{ex.message()});
    }
}

}
