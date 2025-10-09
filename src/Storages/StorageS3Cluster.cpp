#include "Storages/StorageS3Cluster.h"

#include "config.h"

#if USE_AWS_S3

#include "Common/Exception.h"
#include "Client/Connection.h"
#include "Core/QueryProcessingStage.h"
#include <DataTypes/DataTypeString.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/narrowPipe.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/getVirtualsForStorage.h>
#include <Storages/StorageDictionary.h>
#include <Storages/addColumnsStructureToQueryWithClusterEngine.h>

#include <aws/core/auth/AWSCredentials.h>

#include <memory>
#include <string>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED;
}

StorageS3Cluster::StorageS3Cluster(
    const Configuration & configuration_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_)
    : IStorage(table_id_)
    , s3_configuration{configuration_}
    , cluster_name(configuration_.cluster_name)
    , format_name(configuration_.format)
    , compression_method(configuration_.compression_method)
{
    context_->getGlobalContext()->getRemoteHostFilter().checkURL(configuration_.url.uri);
    StorageInMemoryMetadata storage_metadata;
    updateConfigurationIfChanged(context_);

    if (columns_.empty())
    {
        /// `distributed_processing` is set to false, because this code is executed on the initiator, so there is no callback set
        /// for asking for the next tasks.
        /// `format_settings` is set to std::nullopt, because StorageS3Cluster is used only as table function
        auto columns = StorageS3::getTableStructureFromDataImpl(s3_configuration, /*format_settings=*/std::nullopt, context_);
        storage_metadata.setColumns(columns);
        add_columns_structure_to_query = true;
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto columns = storage_metadata.getSampleBlock().getNamesAndTypesList();
    virtual_columns = getVirtualsForStorage(columns, default_virtuals);
    for (const auto & column : virtual_columns)
        virtual_block.insert({column.type->createColumn(), column.type, column.name});
}

void StorageS3Cluster::updateConfigurationIfChanged(ContextPtr local_context)
{
    s3_configuration.update(local_context);
}

/// The code executes on initiator
Pipe StorageS3Cluster::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    throw Exception(ErrorCodes::UNSUPPORTED, "s3 cluster is not supported");

#if 0
    updateConfigurationIfChanged(context);

    auto cluster = context->getCluster(cluster_name)->getClusterWithReplicasAsShards(context->getSettingsRef());

    auto iterator = std::make_shared<StorageS3Source::DisclosedGlobIterator>(
        *s3_configuration.client, s3_configuration.url, query, virtual_block, context, nullptr, s3_configuration.request_settings, context->getFileProgressCallback());
    auto callback = std::make_shared<std::function<String()>>([iterator]() mutable -> String { return iterator->next().key; });

    /// Calculate the header. This is significant, because some columns could be thrown away in some cases like query with count(*)
    Block header =
        InterpreterSelectQuery(query_info.query, context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};

    Pipes pipes;

    const bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;

    ASTPtr query_to_send = query_info.original_query->clone();
    if (add_columns_structure_to_query)
        addColumnsStructureToQueryWithClusterEngine(
            query_to_send, storage_snapshot->metadata->getColumns().getAll().toNamesAndTypesDescription(), 5, getName());

    for (const auto & replicas : cluster->getShardsAddresses())
    {
        /// There will be only one replica, because we consider each replica as a shard
        for (const auto & node : replicas)
        {
            auto connection = std::make_shared<Connection>(
                node.host_name, node.port, context->getGlobalContext()->getCurrentDatabase(),
                node.user, node.password, node.quota_key, node.cluster, node.cluster_secret,
                "S3ClusterInititiator",
                node.compression,
                node.secure
            );


            /// For unknown reason global context is passed to IStorage::read() method
            /// So, task_identifier is passed as constructor argument. It is more obvious.
            auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                connection,
                queryToString(query_to_send),
                header,
                context,
                /*throttler=*/nullptr,
                scalars,
                Tables(),
                processed_stage,
                RemoteQueryExecutor::Extension{.task_iterator = callback, .parallel_reading_coordinator = {}, .replica_info = {}});

            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, add_agg_info, false, UUIDHelpers::Nil, /*is_streaming_=*/false));
        }
    }

    storage_snapshot->check(column_names);
    return Pipe::unitePipes(std::move(pipes));
#endif
}

QueryProcessingStage::Enum StorageS3Cluster::getQueryProcessingStage(
    ContextPtr context, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr &, SelectQueryInfo &) const
{
    /// Initiator executes query on remote node.
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
            return QueryProcessingStage::Enum::WithMergeableState;

    /// Follower just reads the data.
    return QueryProcessingStage::Enum::FetchColumns;
}


NamesAndTypesList StorageS3Cluster::getVirtuals() const
{
    return virtual_columns;
}


}

#endif
