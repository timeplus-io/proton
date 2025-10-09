#include <Storages/MatView/StorageMaterializedView.h>

#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterUtil.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/ReadInOrderOptimizer.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{
namespace ErrorCodes
{
extern const int RESOURCE_NOT_INITED;
extern const int UNSUPPORTED;
}

void StorageMaterializedView::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const size_t num_streams)
{
    /// The behavior of querying a MV is same as querying the underlying stream`

    /// Read remote storage
    if (isVirtualStorage())
    {
        readRemote(query_plan, column_names, storage_snapshot, query_info, local_context, processed_stage);
        return;
    }

    /// The Materialized View can still be queried even if its status is unhealthy.
    /// This is because querying an MV behaves the same as querying the underlying stream.
    /// pipeline_state.checkException("There is something wrong in the underlying streaming query of the materialized view. Last streaming query error:");
    if (!isReady())
        throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "The target stream of '{}' is not ready", getStorageID().getFullTableName());

    auto storage = getTargetTable();

    Block header;
    if (!column_names.empty())
        header = storage_snapshot->getSampleBlockForColumns(column_names);
    else
        header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_TIME});

    auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto target_metadata_snapshot = storage->getInMemoryMetadataPtr();
    auto target_storage_snapshot = storage->getStorageSnapshot(target_metadata_snapshot, local_context);

    if (query_info.order_optimizer)
        query_info.input_order_info = query_info.order_optimizer->getInputOrder(target_metadata_snapshot, local_context);

    storage->read(
        query_plan, column_names, target_storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);

    if (query_plan.isInitialized())
    {
        query_plan.addStorageHolder(storage);
        query_plan.addTableLock(std::move(lock));
    }
}

void StorageMaterializedView::readRemote(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage)
{
    if (!query_info.getCluster())
    {
        /// Single-instance: no distributed query support
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Remote queries are not supported in single-instance mode for materialized view {}",
            getStorageID().getFullTableName());
    }

    Block header;
    if (!column_names.empty())
        header = storage_snapshot->getSampleBlockForColumns(column_names);
    else
        header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_TIME});

    auto modified_query_info = query_info;

    ClusterProxy::SelectStreamFactory select_stream_factory
        = ClusterProxy::SelectStreamFactory(header, /*objects_by_shard_=*/{}, storage_snapshot, processed_stage);

    ClusterProxy::executeQuery(
        query_plan,
        header,
        processed_stage,
        getStorageID(),
        /*table_func_ptr=*/nullptr,
        select_stream_factory,
        logger,
        local_context,
        modified_query_info,
        /*sharding_key_expr=*/nullptr,
        /*sharding_key_column_name=*/{},
        DistributedSettings{},
        /*shard_filter_generator=*/{},
        /*is_remote_function=*/false,
        /*is_streaming=*/query_info.isStreaming(),
        /*read_concat=*/false);

    chassert(query_plan.isInitialized());
}

SinkToStoragePtr StorageMaterializedView::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_)
{
    if (!context_->getSettingsRef().allow_insert_into_materialized_view.value)
        throw Exception(ErrorCodes::UNSUPPORTED, "INSERT INTO materialized view {} is not supported", getStorageID().getFullTableName());

    /// If the MV has a target table, write to it
    if (target_table_id.has_value())
        throw Exception(
            ErrorCodes::UNSUPPORTED,
            "INSERT INTO materialized view {} with target stream is not supported",
            getStorageID().getFullTableName());

    return getTargetTable()->write(query, metadata_snapshot, context_);
}
}
