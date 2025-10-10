#include <Interpreters/Apply/MetadataUpdater.h>

#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/NamedCollectionDescriptor.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int RESOURCE_ALREADY_EXISTS;
}

cluster::CallResultV<cluster::protocol::NamedCollectionDescriptorPtr> MetadataUpdater::loadNamedCollection(const std::string & name) const
{
    cluster::CallResultV<cluster::protocol::NamedCollectionDescriptorPtr> value;
    /// Ignore the result as it will be handled by caller
    auto _ = executeWithRetry([this, &name, &value] {
        value = meta_store->getMetaDB().getNamedCollection(name);
        return value.err.error_code;
    });
    return value;
}

void MetadataUpdater::handleCreateNamedCollection(
    const cluster::CreateNamedCollectionRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto & collection_name = request_data.collection_name;

    Stopwatch stopwatch;
    LOG_INFO(logger, "Start CreateNamedCollection, name={}", collection_name);
    SCOPE_EXIT({ LOG_INFO(logger, "End CreateNamedCollection, name={}, took={}ms", collection_name, stopwatch.elapsedMilliseconds()); });

    auto current_value = loadNamedCollection(collection_name);
    if (current_value.hasError() && !current_value.err.isNotFound())
    {
        LOG_ERROR(logger, "Failed to get current named collection: name={} error={{{}}}", collection_name, current_value.err.string());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(
            request_header.correlationID(), sn, current_value.err.error_code, std::move(current_value.err.error_message));
        return;
    }

    const bool keys_exists = !current_value.err.hasError();
    if (keys_exists)
    {
        if (request_data.exists_op == cluster::protocol::ExistsOperation::Throw)
        {
            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(),
                sn,
                ErrorCodes::RESOURCE_ALREADY_EXISTS,
                fmt::format("Failed to create named collection '{}', already exists", collection_name));
            return;
        }

        if (request_data.exists_op == cluster::protocol::ExistsOperation::Ignore)
        {
            meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
            return;
        }
    }

    const auto & collection = request_data.collection;
    if (keys_exists)
    {
        collection->data_version = current_value.result->data_version + 1;
        collection->last_modified_by = request_data.requested_by;
        collection->last_modify_timestamp_ms = request_data.requested_ts;
    }
    else
    {
        collection->data_version = 1;
        collection->created_by = request_data.requested_by;
        collection->create_timestamp_ms = request_data.requested_ts;
        collection->last_modified_by = request_data.requested_by;
        collection->last_modify_timestamp_ms = request_data.requested_ts;
    }

    auto res = executeWithRetry([this, &collection_name, &collection, &sn] {
        return meta_store->getMetaDB().saveNamedCollection(collection_name, *collection, cluster::AppliedSequence(sn)).error_code;
    });
    if (res != ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(), sn, res, fmt::format("Failed to save named collection: name={}", collection_name));
        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

void MetadataUpdater::handleDeleteNamedCollection(
    const cluster::DeleteNamedCollectionRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();

    LOG_INFO(logger, "Start DeleteNamedCollection, name={}", request_data.collection_name);

    Stopwatch stopwatch;
    SCOPE_EXIT({
        LOG_INFO(logger, "End DeleteNamedCollection, name={} took={}ms", request_data.collection_name, stopwatch.elapsedMilliseconds());
    });

    auto res = executeWithRetry([this, &request_data, &sn] {
        return meta_store->getMetaDB().deleteNamedCollection(request_data.collection_name, cluster::AppliedSequence(sn)).error_code;
    });

    if (res != ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(),
            sn,
            res,
            fmt::format("Failed to commit delete named collection {}", request_data.collection_name));
        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

}
