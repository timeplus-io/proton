#include "StorageStream.h"
#include "StreamShard.h"
#include "StreamSink.h"
#include "StreamingBlockReaderNativeLog.h"
#include "StreamingStoreSource.h"
#include "parseHostShards.h"

#include <Columns/ColumnConst.h>
#include <DistributedMetadata/CatalogService.h>
#include <Functions/IFunction.h>
#include <Interpreters/ClusterProxy/DistributedSelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <NativeLog/Server/NativeLog.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageMergeTree.h>
#include <base/logger_useful.h>
#include <Common/ProtonCommon.h>
#include <Common/randomSeed.h>


namespace DB
{
namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
extern const int INVALID_CONFIG_PARAMETER;
extern const int NOT_IMPLEMENTED;
extern const int OK;
extern const int UNABLE_TO_SKIP_UNUSED_SHARDS;
extern const int TOO_MANY_ROWS;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int BAD_ARGUMENTS;
extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
extern const int UNKNOWN_EXCEPTION;
extern const int INTERNAL_ERROR;
extern const int UNSUPPORTED_PARAMETER;
}

namespace ActionLocks
{
    extern const StorageActionBlockType StreamConsume;
}

namespace
{
const UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY = 1;
const UInt64 FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS = 2;
const UInt64 DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION = 2;

ExpressionActionsPtr
buildShardingKeyExpression(const ASTPtr & sharding_key, ContextPtr context, const NamesAndTypesList & columns, bool project)
{
    ASTPtr query = sharding_key;
    auto syntax_result = TreeRewriter(context).analyze(query, columns);
    return ExpressionAnalyzer(query, syntax_result, context).getActions(project);
}

bool isExpressionActionsDeterministics(const ExpressionActionsPtr & actions)
{
    for (const auto & action : actions->getActions())
    {
        if (action.node->type != ActionsDAG::ActionType::FUNCTION)
            continue;

        if (!action.node->function_base->isDeterministic())
            return false;
    }
    return true;
}

class ReplacingConstantExpressionsMatcher
{
public:
    using Data = Block;

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & node, Block & block_with_constants)
    {
        if (!node->as<ASTFunction>())
            return;

        std::string name = node->getColumnName();
        if (block_with_constants.has(name))
        {
            auto result = block_with_constants.getByName(name);
            if (!isColumnConst(*result.column))
                return;

            node = std::make_shared<ASTLiteral>(assert_cast<const ColumnConst &>(*result.column).getField());
        }
    }
};

void replaceConstantExpressions(
    ASTPtr & node,
    ContextPtr context,
    const NamesAndTypesList & columns,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot)
{
    auto syntax_result = TreeRewriter(context).analyze(node, columns, storage, storage_snapshot);
    Block block_with_constants = KeyCondition::getBlockWithConstants(node, syntax_result, context);

    InDepthNodeVisitor<ReplacingConstantExpressionsMatcher, true> visitor(block_with_constants);
    visitor.visit(node);
}

/// Returns one of the following:
/// - QueryProcessingStage::Complete
/// - QueryProcessingStage::WithMergeableStateAfterAggregation
/// - none (in this case regular WithMergeableState should be used)
std::optional<QueryProcessingStage::Enum>
getOptimizedQueryProcessingStage(const ASTPtr & query_ptr, bool extremes, const Block & sharding_key_block)
{
    const auto & select = query_ptr->as<ASTSelectQuery &>();

    auto sharding_block_has = [&](const auto & exprs, size_t limit = SIZE_MAX) -> bool {
        size_t i = 0;
        for (auto & expr : exprs)
        {
            ++i;
            if (i > limit)
                break;

            auto id = expr->template as<ASTIdentifier>();
            if (!id)
                return false;
            /// TODO: if GROUP BY contains multiIf()/if() it should contain only columns from sharding_key
            if (!sharding_key_block.has(id->name()))
                return false;
        }
        return true;
    };

    // GROUP BY qualifiers
    // - TODO: WITH TOTALS can be implemented
    // - TODO: WITH ROLLUP can be implemented (I guess)
    if (select.group_by_with_totals || select.group_by_with_rollup || select.group_by_with_cube)
        return {};

    // TODO: extremes support can be implemented
    if (extremes)
        return {};

    // DISTINCT
    if (select.distinct)
    {
        if (!sharding_block_has(select.select()->children))
            return {};
    }

    // GROUP BY
    const ASTPtr group_by = select.groupBy();
    if (!group_by)
    {
        if (!select.distinct)
            return {};
    }
    else
    {
        if (!sharding_block_has(group_by->children, 1))
            return {};
    }

    // ORDER BY
    const ASTPtr order_by = select.orderBy();
    if (order_by)
        return QueryProcessingStage::WithMergeableStateAfterAggregation;

    // LIMIT BY
    // LIMIT
    // OFFSET
    if (select.limitBy() || select.limitLength() || select.limitOffset())
        return QueryProcessingStage::WithMergeableStateAfterAggregation;

    // Only simple SELECT FROM GROUP BY sharding_key can use Complete state.
    return QueryProcessingStage::Complete;
}

size_t getClusterQueriedNodes(const Settings & settings, const ClusterPtr & cluster)
{
    size_t num_local_shards = cluster->getLocalShardCount();
    size_t num_remote_shards = cluster->getRemoteShardCount();
    return (num_remote_shards * settings.max_parallel_replicas) + num_local_shards;
}

String makeFormattedListOfShards(const ClusterPtr & cluster)
{
    WriteBufferFromOwnString buf;

    bool head = true;
    buf << "[";
    for (const auto & shard_info : cluster->getShardsInfo())
    {
        (head ? buf : buf << ", ") << shard_info.shard_num;
        head = false;
    }
    buf << "]";

    return buf.str();
}
}

StorageStream::StorageStream(
    Int32 replication_factor_,
    Int32 shards_,
    const ASTPtr & sharding_key_,
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach_,
    ContextMutablePtr context_,
    const String & date_column_name_,
    const MergingParams & merging_params_,
    std::unique_ptr<StreamSettings> settings_,
    bool has_force_restore_data_flag_)
    : MergeTreeData(
        table_id_,
        relative_data_path_,
        metadata_,
        context_,
        date_column_name_,
        merging_params_,
        std::make_unique<MergeTreeSettings>(*settings_), /// make a copy
        false, /// require_part_metadata
        attach_)
    , replication_factor(replication_factor_)
    , shards(shards_)
    , rng(randomSeed())
    , max_outstanding_blocks(context_->getSettingsRef().aysnc_ingest_max_outstanding_blocks)
{
    cacheVirtualColumnNamesAndTypes();

    /// Init StreamShard
    {
        auto ssettings = storage_settings.get();
        auto host_shards = parseHostShards(ssettings->host_shards.value, shards);
        stream_shards.reserve(host_shards.size());
        for (auto shard : host_shards)
            stream_shards.push_back(std::make_shared<StreamShard>(
                replication_factor,
                shards,
                shard,
                table_id_,
                relative_data_path_,
                metadata_,
                attach_,
                context_,
                date_column_name_,
                merging_params_,
                std::make_unique<MergeTreeSettings>(*settings_),
                has_force_restore_data_flag_,
                this,
                log));
    }

    for (Int32 shard_id = 0; shard_id < shards; ++shard_id)
        slot_to_shard.push_back(shard_id);

    if (sharding_key_)
    {
        sharding_key_expr = buildShardingKeyExpression(sharding_key_, getContext(), metadata_.getColumns().getAllPhysical(), false);
        sharding_key_is_deterministic = isExpressionActionsDeterministics(sharding_key_expr);
        sharding_key_column_name = sharding_key_->getColumnName();

        if (auto * shard_func = sharding_key_->as<ASTFunction>())
            if (shard_func->name == "rand" || shard_func->name == "RAND")
                rand_sharding_key = true;
    }

    /// TODO: For proton on kafka, disable compression in kafka topic if the settings is set:
    /// 1) During creation, disable kafka topic compression and init
    /// 2) After altered, reinit kafka wal.
    /// For now, only works under nativelog
    updateLogStoreCodec(settings_->logstore_codec);
}

NamesAndTypesList StorageStream::getVirtuals() const
{
    return virtual_column_names_and_types;
}

NamesAndTypesList StorageStream::getVirtualsHistory() const
{
    return MergeTreeData::getVirtuals();
}

void StorageStream::readRemote(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage)
{
    Block header;
    if (!column_names.empty())
        header = storage_snapshot->getSampleBlockForColumns(column_names, /* use_extended_objects */ false);
    else
        header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_TIME}, /* use_extended_objects */ false);

    /// sometimes 'getQueryProcessingStage' has not been called before 'read', get cluster info first before creating pipes
    /// by calling 'getQueryProcessingStageRemote'
    if (!query_info.getCluster())
        getQueryProcessingStageRemote(context_, processed_stage, storage_snapshot, query_info);

    /// Return directly (with correct header) if no shard to query.
    if (query_info.getCluster()->getShardsInfo().empty())
    {
        Pipe pipe(std::make_shared<NullSource>(header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource (Distributed)");
        query_plan.addStep(std::move(read_from_pipe));
        return;
    }

    bool has_virtual_shard_num_column = std::find(column_names.begin(), column_names.end(), "_shard_num") != column_names.end();
    if (has_virtual_shard_num_column && !isVirtualColumn("_shard_num", storage_snapshot->getMetadataForQuery()))
        has_virtual_shard_num_column = false;

    ClusterProxy::DistributedSelectStreamFactory select_stream_factory
        = ClusterProxy::DistributedSelectStreamFactory(header, processed_stage, has_virtual_shard_num_column);

    ClusterProxy::executeQuery(
        query_plan,
        header,
        processed_stage,
        getStorageID(),
        nullptr,
        select_stream_factory,
        log,
        query_info.query,
        context_,
        query_info,
        sharding_key_expr,
        sharding_key_column_name,
        query_info.cluster);
}

void StorageStream::readConcat(
    QueryPlan & query_plan,
    SelectQueryInfo & query_info,
    Names column_names,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size)
{
    /// FIXME, we only support one shard processing for now. For multiple shards, we will need dispatch query to remote shard server
    assert(!requireDistributedQuery(context_));

    /// For queries like `SELECT count(*) FROM tumble(table, now(), 5s) GROUP BY window_end` don't have required column from table.
    /// We will need add one
    Block header;
    if (!column_names.empty())
        header = storage_snapshot->getSampleBlockForColumns(column_names, /* use_extended_objects */ false);
    else
        header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_TIME}, /* use_extended_objects */ false);

    for (auto & stream_shard : stream_shards)
    {
        auto create_streaming_source = [this, header, storage_snapshot, stream_shard, context_](Int64 & max_sn_in_parts) {
            if (max_sn_in_parts < 0)
            {
                /// Fallback to seek streaming store
                auto offsets = stream_shard->getOffsets(context_->getSettingsRef().seek_to.value);
                LOG_INFO(log, "Fused read fallbacks to seek stream for shard={} since there are no historical data", stream_shard->shard);

                return std::make_shared<StreamingStoreSource>(
                    stream_shard, header, storage_snapshot, context_, stream_shard->shard, offsets[stream_shard->shard], log);
            }

            auto committed = stream_shard->storage->inMemoryCommittedSN();
            if (committed < max_sn_in_parts)
            {
                /// This happens if there are sequence ID gaps in the parts and system is bootstrapping to fill the gap
                /// Please refer to SequenceInfo.h for more details.
                /// There is a small race window in which is parts have been committed to fs and parts summaries have already updated in memory
                /// but we didn't have chance to update in memory committed sn.
                /// We sleep a while to hope we will get out of this situation

                LOG_WARNING(
                    log,
                    "Fused read fallbacks to seek stream since sequence gaps are found for shard={}, max_sn_in_parts={}, "
                    "current_committed_sn={}",
                    stream_shard->shard,
                    max_sn_in_parts,
                    committed);

                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }

            if (committed >= max_sn_in_parts)
            {
                LOG_INFO(
                    log,
                    "Fused read for shard={}, read historical data up to sn={}, current_committed_sn={}",
                    stream_shard->shard,
                    max_sn_in_parts,
                    committed);

                return std::make_shared<StreamingStoreSource>(
                    stream_shard, header, storage_snapshot, context_, stream_shard->shard, max_sn_in_parts + 1, log);
            }
            else
            {
                /// Fallback to seek streaming store
                auto offsets = stream_shard->getOffsets(context_->getSettingsRef().seek_to.value);
                LOG_INFO(
                    log,
                    "Fused read fallbacks to seek stream since sequence gaps are found for shard={}, max_sn_in_parts={}, "
                    "current_committed_sn={}",
                    stream_shard->shard,
                    max_sn_in_parts,
                    committed);

                /// We need reset max_sn_in_parts to tell caller that we are seeking streaming store directly
                max_sn_in_parts = -1;
                return std::make_shared<StreamingStoreSource>(
                    stream_shard, header, storage_snapshot, context_, stream_shard->shard, offsets[stream_shard->shard], log);
            }
        };

        assert(stream_shard->storage);

        stream_shard->storage->readConcat(
            query_plan,
            column_names,
            storage_snapshot,
            query_info,
            context_,
            processed_stage,
            max_block_size,
            std::move(create_streaming_source));
    }
}

void StorageStream::readStreaming(
    QueryPlan & query_plan,
    SelectQueryInfo & /*query_info*/,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr context_)
{
    Pipes pipes;
    pipes.reserve(shards);

    const auto & settings_ref = context_->getSettingsRef();
    auto share_resource_group = (settings_ref.query_resource_group.value == "shared")
        && (settings_ref.seek_to.value == "latest" || settings_ref.seek_to.value.empty());

    std::vector<std::pair<std::shared_ptr<StreamShard>, Int32>> shard_info;
    if (requireDistributedQuery(context_))
    {
        /// This is a distributed query on multi-shards
        auto & shard = stream_shards.back();
        for (Int32 i = 0; i < shards; ++i)
            shard_info.emplace_back(shard, i);
    }
    else
    {
        /// multi-shards in single node for NativeLog
        for (auto & stream_shard : stream_shards)
            shard_info.emplace_back(stream_shard, stream_shard->shard);
    }

    if (share_resource_group)
    {
        for (auto & [stream_shard, shard] : shard_info)
        {
            if (!column_names.empty())
                pipes.emplace_back(stream_shard->source_multiplexers->createChannel(shard, column_names, storage_snapshot, context_));
            else
                pipes.emplace_back(stream_shard->source_multiplexers->createChannel(
                    shard, {ProtonConsts::RESERVED_EVENT_TIME}, storage_snapshot, context_));
        }
    }
    else
    {
        /// auto consumer = klog::KafkaWALPool::instance(context_->getGlobalContext()).getOrCreateStreaming(eamingStorageClusterId());

        /// For queries like `SELECT count(*) FROM tumble(table, now(), 5s) GROUP BY window_end` don't have required column from table.
        /// We will need add one
        Block header;
        if (!column_names.empty())
            header = storage_snapshot->getSampleBlockForColumns(column_names, /* use_extended_objects */ false);
        else
            header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_TIME}, /* use_extended_objects */ false);

        auto offsets = stream_shards.back()->getOffsets(settings_ref.seek_to.value);

        for (auto & [stream_shard, shard] : shard_info)
            pipes.emplace_back(std::make_shared<StreamingStoreSource>(
                stream_shard, header, storage_snapshot, context_, shard, offsets[stream_shard->shard], log));
    }

    LOG_INFO(
        log,
        "Starting reading {} streams by seeking to {} in {} resource group",
        pipes.size(),
        settings_ref.seek_to.value,
        share_resource_group ? "shared" : "dedicated");

    auto pipe = Pipe::unitePipes(std::move(pipes));
    /// In cluster deployment, if there are multiple souces, there will be multiple thread conducting aggregation which cannot finalize
    /// the aggregated result correctly. To avoid multi-thread aggregation, resize the source to 1 stream here.
    pipe.resize(1);
    auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), getName());
    query_plan.addStep(std::move(read_step));
}

void StorageStream::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    /// Non streaming window function: tail or global streaming aggr
    const auto & settings_ref = context_->getSettingsRef();

    if (query_info.syntax_analyzer_result->streaming)
    {
        /// FIXME, to support seek_to='-1h'
        bool back_fill_from_historical = isChangelogKvMode() || isVersionedKvMode()
            || (settings_ref.seek_to.value == "earliest" && settings_ref.enable_backfill_from_historical_store.value);
        if (back_fill_from_historical && !requireDistributedQuery(context_))
            readConcat(query_plan, query_info, column_names, storage_snapshot, std::move(context_), processed_stage, max_block_size);
        else
            readStreaming(query_plan, query_info, column_names, storage_snapshot, std::move(context_));
    }
    else
        readHistory(
            query_plan, column_names, storage_snapshot, query_info, std::move(context_), processed_stage, max_block_size, num_streams);
}

void StorageStream::readHistory(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    if (requireDistributedQuery(context_))
    {
        /// This is a distributed query
        readRemote(query_plan, column_names, storage_snapshot, query_info, context_, processed_stage);
    }
    else
    {
        if (stream_shards.size() == 1)
        {
            stream_shards.back()->storage->read(
                query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
        }
        else
        {
            auto shard_num_streams = num_streams / stream_shards.size();
            if (shard_num_streams == 0)
                shard_num_streams = 1;

            std::vector<QueryPlanPtr> plans;
            plans.reserve(stream_shards.size());

            for (auto & stream_shard : stream_shards)
            {
                auto plan = std::make_unique<QueryPlan>();

                assert(stream_shard->storage);
                stream_shard->storage->read(
                    *plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, shard_num_streams);

                plans.push_back(std::move(plan));
            }

            DataStreams input_streams;
            input_streams.reserve(plans.size());

            for (auto & plan : plans)
                input_streams.emplace_back(plan->getCurrentDataStream());

            auto union_step = std::make_unique<UnionStep>(std::move(input_streams));
            query_plan.unitePlans(std::move(union_step), std::move(plans));
        }
    }
}

Pipe StorageStream::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(QueryPlanOptimizationSettings::fromContext(context_), BuildQueryPipelineSettings::fromContext(context_));
}

void StorageStream::startup()
{
    if (inited.test_and_set())
        return;

    LOG_INFO(log, "Starting");

    for (auto & stream_shard : stream_shards)
        stream_shard->startup();

    if (stream_shards.back()->kafka)
    {
        /// Always use last Kafka instance for ingestion
        /// FIXME, per shard ingestion
        kafka_log = stream_shards.back()->kafka.get();
    }
    else
    {
        native_log = &nlog::NativeLog::instance(getContext());
        assert(native_log->enabled());
    }

    LOG_INFO(log, "Started");
}

StorageStream::~StorageStream()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to shutdown");
    }

    LOG_INFO(log, "Stopped with outstanding_blocks={}", outstanding_blocks);

    /// Wait for outstanding ingested blocks
    while (outstanding_blocks != 0)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        LOG_INFO(log, "Waiting for outstanding_blocks={}", outstanding_blocks);
    }

    LOG_INFO(log, "Completely dtored");
}

void StorageStream::shutdown()
{
    if (stopped.test_and_set())
        return;

    LOG_INFO(log, "Stopping");

    for (auto & stream_shard : stream_shards)
        stream_shard->shutdown();
}

String StorageStream::getName() const
{
    return "Stream";
}

bool StorageStream::isRemote() const
{
    /// If there is no backing storage, it is remote
    /// checking one shard is good enough
    return stream_shards.back()->storage == nullptr;
}

bool StorageStream::requireDistributedQuery(ContextPtr context_) const
{
    if (!stream_shards.back()->storage)
        return true;

    /// If it has backing storage and it is a single shard table
    if (shards == 1)
        return false;

    const auto & client_info = context_->getClientInfo();
    if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        /// If this query is a remote query already
        return false;

    if (stream_shards.size() == static_cast<size_t>(shards))
        /// Local host has all stream shards
        return false;

    /// If it has backing storage and it is a multiple shard stream and
    /// this query is an initial query, we need execute a distributed query
    return true;
}

bool StorageStream::supportsParallelInsert() const
{
    return true;
}

bool StorageStream::supportsIndexForIn() const
{
    return true;
}

bool StorageStream::supportsSubcolumns() const
{
    return true;
}

std::optional<UInt64> StorageStream::totalRows(const Settings & settings) const
{
    std::optional<UInt64> rows;
    for (const auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto shard_rows = stream_shard->storage->totalRows(settings);

            if (shard_rows.has_value())
            {
                if (rows.has_value())
                    *rows += *shard_rows;
                else
                    rows = shard_rows;
            }
        }
    }

    return rows;
}

std::optional<UInt64> StorageStream::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr context_) const
{
    std::optional<UInt64> rows;
    for (const auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto shard_rows = stream_shard->storage->totalRowsByPartitionPredicate(query_info, context_);
            if (shard_rows.has_value())
            {
                if (rows.has_value())
                    *rows += *shard_rows;
                else
                    rows = shard_rows;
            }
        }
    }

    return rows;
}

std::optional<UInt64> StorageStream::totalBytes(const Settings & settings) const
{
    std::optional<UInt64> bytes;

    for (const auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto shard_bytes = stream_shard->storage->totalBytes(settings);
            if (shard_bytes.has_value())
            {
                if (bytes.has_value())
                    *bytes += *shard_bytes;
                else
                    bytes = shard_bytes;
            }
        }
    }

    return bytes;
}

SinkToStoragePtr StorageStream::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_)
{
    return std::make_shared<StreamSink>(*this, metadata_snapshot, context_);
}

void StorageStream::checkTableCanBeDropped() const
{
    for (const auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->checkTableCanBeDropped();
}

void StorageStream::preDrop()
{
    shutdown();

    for (auto & stream_shard : stream_shards)
        stream_shard->deinitNativeLog();
}

void StorageStream::drop()
{
    for (const auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->drop();
}

void StorageStream::preRename(const StorageID & new_table_id)
{
    if (native_log)
    {
        const auto & storage_id = getStorageID();
        nlog::RenameStreamRequest request(storage_id.getTableName(), new_table_id.getTableName());
        auto response{native_log->renameStream(storage_id.getDatabaseName(), request)};
        if (response.hasError())
            throw DB::Exception(response.error_code, "Failed to rename stream, error={}", response.error_message);
    }
}

void StorageStream::truncate(
    const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, TableExclusiveLockHolder & holder)
{
    for (const auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->truncate(query, metadata_snapshot, context_, holder);
}

void StorageStream::alter(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder)
{
    for (auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            stream_shard->storage->alter(commands, context_, alter_lock_holder);
            setInMemoryMetadata(stream_shard->storage->getInMemoryMetadata());
        }
    }

    /// Update native_log codec, retention or flush settings
    if (commands.hasSettingsAlterCommand() && stream_shards.back()->storage)
    {
        auto & shard = stream_shards.back();
        const auto settings = shard->storage->getSettings();
        updateLogStoreCodec(settings->logstore_codec);

        if (!shard->isLogStoreKafka())
            shard->updateNativeLog();
    }
}

bool StorageStream::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & partition,
    bool finall,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    ContextPtr context_)
{
    bool result = true;
    for (auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto optimized = stream_shard->storage->optimize(
                query, metadata_snapshot, partition, finall, deduplicate, deduplicate_by_columns, context_);
            if (!optimized)
                result = false;
        }
    }
    return result;
}

void StorageStream::mutate(const MutationCommands & commands, ContextPtr context_)
{
    for (auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->mutate(commands, context_);
}

/// Return introspection information about currently processing or recently processed mutations.
std::vector<MergeTreeMutationStatus> StorageStream::getMutationsStatus() const
{
    std::vector<MergeTreeMutationStatus> results;
    for (const auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto statuses = stream_shard->storage->getMutationsStatus();
            for (auto & status : statuses)
                results.push_back(std::move(status));
        }
    }
    return results;
}

CancellationCode StorageStream::killMutation(const String & mutation_id)
{
    for (auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto code = stream_shard->storage->killMutation(mutation_id);
            if (code != CancellationCode::CancelSent)
                return code;
        }
    }
    return CancellationCode::CancelSent;
}

ActionLock StorageStream::getActionLock(StorageActionBlockType action_type)
{
    /// FIXME: Grap the first shard's action lock as the whole storage stream action lock.
    /// It is OK for now as for kafka log store, each stream only has one shard, therefore only lock the first shard
    ActionLock lock;
    if (action_type != ActionLocks::StreamConsume)
        return stream_shards.back()->storage->getActionLock(action_type);
    else
        return stream_shards.back()->consume_blocker.cancel();
}

void StorageStream::onActionLockRemove(StorageActionBlockType action_type)
{
    if (action_type != ActionLocks::StreamConsume)
    {
        for (auto & stream_shard : stream_shards)
            if (stream_shard->storage)
                stream_shard->storage->onActionLockRemove(action_type);
    }
}

CheckResults StorageStream::checkData(const ASTPtr & query, ContextPtr context_)
{
    CheckResults results;
    for (auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto check_results = stream_shard->storage->checkData(query, context_);
            for (auto & check_result : check_results)
                results.push_back(std::move(check_result));
        }
    }
    return results;
}

bool StorageStream::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    for (auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            if (!stream_shard->storage->scheduleDataProcessingJob(assignee))
                return false;
        }
    }
    return true;
}

QueryProcessingStage::Enum StorageStream::getQueryProcessingStage(
    ContextPtr context_,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    if (query_info.syntax_analyzer_result->streaming)
    {
        return QueryProcessingStage::Enum::FetchColumns;
    }
    else if (requireDistributedQuery(context_))
    {
        return getQueryProcessingStageRemote(context_, to_stage, storage_snapshot, query_info);
    }
    else
    {
        /// Use the last shard is good enough
        return stream_shards.back()->storage->getQueryProcessingStage(context_, to_stage, storage_snapshot, query_info);
    }
}

StorageSnapshotPtr StorageStream::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot) const
{
    auto & storage = stream_shards.back()->storage;

    std::shared_ptr<StorageSnapshot> storage_snapshot;
    if (!storage)
    {
        /// for virtual table
        storage_snapshot = IStorage::getStorageSnapshot(metadata_snapshot)->clone();
    }
    else
    {
        storage_snapshot = storage->getStorageSnapshot(metadata_snapshot)->clone();
    }
    /// Add virtuals, such as `_tp_append_time` and `_tp_process_time
    storage_snapshot->addVirtuals(getVirtuals());
    return storage_snapshot;
}

void StorageStream::dropPartNoWaitNoThrow(const String & part_name)
{
    for (auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->dropPartNoWaitNoThrow(part_name);
}

void StorageStream::dropPart(const String & part_name, bool detach, ContextPtr context_)
{
    for (auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->dropPart(part_name, detach, context_);
}

void StorageStream::dropPartition(const ASTPtr & partition, bool detach, ContextPtr context_)
{
    for (auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->dropPartition(partition, detach, context_);
}

PartitionCommandsResultInfo
StorageStream::attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context_)
{
    PartitionCommandsResultInfo results;
    for (auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto infos = stream_shard->storage->attachPartition(partition, metadata_snapshot, part, context_);
            for (auto & info : infos)
                results.push_back(std::move(info));
        }
    }
    return results;
}

void StorageStream::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context_)
{
    for (auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->replacePartitionFrom(source_table, partition, replace, context_);
}

void StorageStream::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context_)
{
    for (auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->movePartitionToTable(dest_table, partition, context_);
}

/// If part is assigned to merge or mutation (possibly replicated)
/// Should be overridden by children, because they can have different
/// mechanisms for parts locking
bool StorageStream::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    for (const auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            if (stream_shard->storage->partIsAssignedToBackgroundOperation(part))
                return true;
        }
    }
    return false;
}

/// Return most recent mutations commands for part which weren't applied
/// Used to receive AlterConversions for part and apply them on fly. This
/// method has different implementations for replicated and non replicated
/// MergeTree because they store mutations in different way.
MutationCommands StorageStream::getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    MutationCommands results;
    for (const auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto cmds = stream_shard->storage->getFirstAlterMutationCommandsForPart(part);
            for (auto & cmd : cmds)
                results.push_back(std::move(cmd));
        }
    }
    return results;
}

void StorageStream::startBackgroundMovesIfNeeded()
{
    for (const auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->startBackgroundMovesIfNeeded();
}

std::unique_ptr<StreamSettings> StorageStream::getDefaultSettings() const
{
    return std::make_unique<StreamSettings>(getContext()->getStreamSettings());
}

/// Distributed query related functions
ClusterPtr StorageStream::getCluster() const
{
    auto sid = getStorageID();
    return CatalogService::instance(getContext()).tableCluster(sid.database_name, sid.table_name, replication_factor, shards);
}

/// Returns a new cluster with fewer shards if constant folding for `sharding_key_expr` is possible
/// using constraints from "PREWHERE" and "WHERE" conditions, otherwise returns `nullptr`
ClusterPtr StorageStream::skipUnusedShards(
    ClusterPtr cluster, const ASTPtr & query_ptr, const StorageSnapshotPtr & storage_snapshot, ContextPtr context_) const
{
    const auto & select = query_ptr->as<ASTSelectQuery &>();

    if (!select.prewhere() && !select.where())
        return nullptr;

    ASTPtr condition_ast;
    if (select.prewhere() && select.where())
        condition_ast = makeASTFunction("and", select.prewhere()->clone(), select.where()->clone());
    else
        condition_ast = select.prewhere() ? select.prewhere()->clone() : select.where()->clone();

    replaceConstantExpressions(
        condition_ast,
        context_,
        storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns()),
        shared_from_this(),
        storage_snapshot);

    size_t limit = context_->getSettingsRef().optimize_skip_unused_shards_limit;
    if (!limit || limit > LONG_MAX)
        throw Exception("optimize_skip_unused_shards_limit out of range (0, {}]", ErrorCodes::ARGUMENT_OUT_OF_BOUND, LONG_MAX);

    ++limit;
    const auto blocks = evaluateExpressionOverConstantCondition(condition_ast, sharding_key_expr, limit);

    if (!limit)
    {
        LOG_TRACE(
            log,
            "Number of values for sharding key exceeds optimize_skip_unused_shards_limit={}, "
            "try to increase it, but note that this may increase query processing time.",
            context_->getSettingsRef().optimize_skip_unused_shards_limit);
        return nullptr;
    }

    /// Can't get definite answer if we can skip any shards
    if (!blocks)
        return nullptr;

    std::set<int> shard_ids;

    for (const auto & block : *blocks)
    {
        if (!block.has(sharding_key_column_name))
            throw Exception("sharding_key_expr should evaluate as a single row", ErrorCodes::TOO_MANY_ROWS);

        const ColumnWithTypeAndName & result = block.getByName(sharding_key_column_name);
        const auto selector = createSelector(result);

        shard_ids.insert(selector.begin(), selector.end());
    }

    return cluster->getClusterWithMultipleShards({shard_ids.begin(), shard_ids.end()});
}

ClusterPtr
StorageStream::getOptimizedCluster(ContextPtr context_, const StorageSnapshotPtr & storage_snapshot, const ASTPtr & query_ptr) const
{
    ClusterPtr cluster = getCluster();
    const Settings & settings = context_->getSettingsRef();

    bool sharding_key_is_usable = settings.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic;

    if (sharding_key_expr && sharding_key_is_usable)
    {
        ClusterPtr optimized = skipUnusedShards(cluster, query_ptr, storage_snapshot, context_);
        if (optimized)
            return optimized;
    }

    UInt64 force = settings.force_optimize_skip_unused_shards;
    if (force)
    {
        WriteBufferFromOwnString exception_message;
        if (!sharding_key_expr)
            exception_message << "No sharding key";
        else if (!sharding_key_is_usable)
            exception_message << "Sharding key is not deterministic";
        else
            exception_message << "Sharding key " << sharding_key_column_name << " is not used";

        if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS)
            throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);

        if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY && sharding_key_expr)
            throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);
    }

    return {};
}

QueryProcessingStage::Enum StorageStream::getQueryProcessingStageRemote(
    ContextPtr context_,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    const auto & settings = context_->getSettingsRef();

    ClusterPtr cluster = getCluster();
    query_info.cluster = cluster;

    /// Always calculate optimized cluster here to avoid conditions during read()
    if (settings.optimize_skip_unused_shards && getClusterQueriedNodes(settings, cluster) > 1)
    {
        ClusterPtr optimized_cluster = getOptimizedCluster(context_, storage_snapshot, query_info.query);
        if (optimized_cluster)
        {
            LOG_DEBUG(
                log,
                "Skipping irrelevant shards - the query will be sent to the following shards of the cluster (shard numbers): {}",
                makeFormattedListOfShards(optimized_cluster));
            cluster = optimized_cluster;
            query_info.optimized_cluster = cluster;
        }
        else
        {
            LOG_DEBUG(
                log,
                "Unable to figure out irrelevant shards from WHERE/PREWHERE clauses - the query will be sent to all shards of the "
                "cluster{}",
                sharding_key_expr ? "" : " (no sharding key)");
        }
    }

    if (settings.distributed_group_by_no_merge)
    {
        if (settings.distributed_group_by_no_merge == DISTRIBUTED_GROUP_BY_NO_MERGE_AFTER_AGGREGATION)
            return QueryProcessingStage::WithMergeableStateAfterAggregation;
        else
            return QueryProcessingStage::Complete;
    }

    /// Nested distributed query cannot return Complete stage,
    /// since the parent query need to aggregate the results after.
    if (to_stage == QueryProcessingStage::WithMergeableState)
        return QueryProcessingStage::FetchColumns;

    /// If there is only one node, the query can be fully processed by the
    /// shard, initiator will work as a proxy only.
    //    if (getClusterQueriedNodes(settings, cluster) == 1)
    //        return QueryProcessingStage::Complete;

    if (settings.optimize_skip_unused_shards && settings.optimize_distributed_group_by_sharding_key && sharding_key_expr
        && (settings.allow_nondeterministic_optimize_skip_unused_shards || sharding_key_is_deterministic))
    {
        Block sharding_key_block = sharding_key_expr->getSampleBlock();
        auto stage = getOptimizedQueryProcessingStage(query_info.query, settings.extremes, sharding_key_block);
        if (stage)
        {
            LOG_DEBUG(log, "Force processing stage to {}", QueryProcessingStage::toString(*stage));
            return *stage;
        }
    }

    return QueryProcessingStage::FetchColumns;
}

/// New functions
IColumn::Selector StorageStream::createSelector(const ColumnWithTypeAndName & result) const
{
/// If result.type is DataTypeLowCardinality, do shard according to its dictionaryType
#define CREATE_FOR_TYPE(TYPE) \
    if (typeid_cast<const DataType##TYPE *>(result.type.get())) \
        return createBlockSelector<TYPE>(*result.column, slot_to_shard); \
    else if (auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(result.type.get())) \
        if (typeid_cast<const DataType##TYPE *>(type_low_cardinality->getDictionaryType().get())) \
            return createBlockSelector<TYPE>(*result.column->convertToFullColumnIfLowCardinality(), slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}


IColumn::Selector StorageStream::createSelector(const Block & block) const
{
    Block current_block_with_sharding_key_expr = block;
    sharding_key_expr->execute(current_block_with_sharding_key_expr);

    const auto & key_column = current_block_with_sharding_key_expr.getByName(sharding_key_column_name);

    return createSelector(key_column);

#if 0
    auto selector = createSelector(key_column);

    for (size_t i = 0; i < key_column.column->size(); ++i)
    {
        std::cout << "key=" << key_column.column->getInt(i) << ", selector=" << selector[i] << "\n";
    }

    return selector;
#endif
}

const ExpressionActionsPtr & StorageStream::getShardingKeyExpr() const
{
    return sharding_key_expr;
}

size_t StorageStream::getRandomShardIndex() const
{
    std::lock_guard lock(rng_mutex);
    return std::uniform_int_distribution<size_t>(0, shards - 1)(rng);
}

size_t StorageStream::getNextShardIndex() const
{
    return next_shard++ % shards;
}

void StorageStream::append(
    nlog::RecordPtr & record, IngestMode ingest_mode, klog::AppendCallback callback, void * data, UInt64 base_block_id, UInt64 sub_block_id)
{
    if (native_log)
    {
        record->setCodec(logstore_codec);
        appendToNativeLog(record, ingest_mode);
    }
    else
        appendToKafka(record, ingest_mode, callback, data, base_block_id, sub_block_id);
}

inline void StorageStream::appendToNativeLog(nlog::RecordPtr & record, IngestMode /*ingest_mode*/)
{
    assert(native_log);

    const auto & storage_id = getStorageID();
    nlog::AppendRequest request(storage_id.getTableName(), storage_id.uuid, record->getShard(), record);

    auto resp{native_log->append(storage_id.getDatabaseName(), request)};
    if (resp.hasError())
    {
        LOG_ERROR(log, "Failed to append record to native log, error={}", resp.errString());
        throw DB::Exception(ErrorCodes::INTERNAL_ERROR, "Failed to append record to native log, error={}", resp.errString());
    }
}

inline void StorageStream::appendToKafka(
    nlog::RecordPtr & record, IngestMode ingest_mode, klog::AppendCallback callback, void * data, UInt64 base_block_id, UInt64 sub_block_id)
{
    assert(kafka_log);

    switch (ingest_mode)
    {
        case IngestMode::ASYNC: {
            //                LOG_TRACE(
            //                    storage.log,
            //                    "[async] write a block={} rows={} shard={} query_status_poll_id={} ...",
            //                    outstanding,
            //                    record.block.rows(),
            //                    current_block.shard,
            //                    query_context->getQueryStatusPollId());

            appendAsync(*record, base_block_id, sub_block_id);
            break;
        }
        case IngestMode::SYNC: {
            //                LOG_TRACE(
            //                    log,
            //                    "[sync] write a block={} rows={} shard={} committed={} ...",
            //                    outstanding,
            //                    record.block.rows(),
            //                    current_block.shard,
            //                    committed);

            auto ret = kafka_log->log->append(*record, callback, data, kafka_log->append_ctx);
            if (ret != 0)
                throw Exception("Failed to insert data sync", ret);

            break;
        }
        case IngestMode::FIRE_AND_FORGET: {
            //                LOG_TRACE(
            //                    log,
            //                    "[fire_and_forget] write a block={} rows={} shard={} ...",
            //                    outstanding,
            //                    record.block.rows(),
            //                    current_block.shard);

            auto ret = kafka_log->log->append(*record, nullptr, nullptr, kafka_log->append_ctx);
            if (ret != 0)
                throw Exception("Failed to insert data fire_and_forget", ret);

            break;
        }
        case IngestMode::ORDERED: {
            auto ret = kafka_log->log->append(*record, kafka_log->append_ctx);
            if (ret.err != ErrorCodes::OK)
                throw Exception("Failed to insert data ordered", ret.err);

            break;
        }
        case IngestMode::None:
            /// FALLTHROUGH
        case IngestMode::INVALID:
            throw Exception("Failed to insert data, ingest mode is not setup", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
}

inline void StorageStream::appendAsync(nlog::Record & record, UInt64 block_id, UInt64 sub_block_id)
{
    if (outstanding_blocks > max_outstanding_blocks)
        throw Exception("Too many request", ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS);

    [[maybe_unused]] auto added = kafka_log->ingesting_blocks.add(block_id, sub_block_id);
    assert(added);

    auto data = std::make_unique<WriteCallbackData>(block_id, sub_block_id, this);
    auto ret = kafka_log->log->append(record, &StorageStream::writeCallback, data.get(), kafka_log->append_ctx);
    if (ret == ErrorCodes::OK)
        /// The writeCallback takes over the ownership of callback data
        data.release();
    else
        throw Exception("Failed to insert data async", ret);
}

void StorageStream::writeCallback(const klog::AppendResult & result, UInt64 block_id, UInt64 sub_block_id)
{
    if (result.err)
    {
        kafka_log->ingesting_blocks.fail(block_id, result.err);
        LOG_ERROR(log, "[async] Failed to write sub_block_id={} in block_id={} error={}", sub_block_id, block_id, result.err);
    }
    else
    {
        kafka_log->ingesting_blocks.remove(block_id, sub_block_id);
        LOG_TRACE(log, "[async] Written sub_block_id={} in block_id={}", sub_block_id, block_id);
    }
}

void StorageStream::writeCallback(const klog::AppendResult & result, void * data)
{
    std::unique_ptr<StorageStream::WriteCallbackData> pdata(static_cast<WriteCallbackData *>(data));

    pdata->storage->writeCallback(result, pdata->block_id, pdata->sub_block_id);
}

IngestMode StorageStream::ingestMode() const
{
    assert(!stream_shards.empty());
    return stream_shards.back()->ingestMode();
}

bool StorageStream::isMaintain() const
{
    assert(!stream_shards.empty());
    return stream_shards.back()->isMaintain();
}

void StorageStream::reInit()
{
    assert(!stream_shards.empty());
    if (!isMaintain())
        return;

    for(auto & shard : stream_shards)
        if (shard->storage)
            shard->storage->reInit();
}

void StorageStream::getIngestionStatuses(const std::vector<UInt64> & block_ids, std::vector<IngestingBlocks::IngestStatus> & statuses) const
{
    if (kafka_log)
        kafka_log->ingesting_blocks.getStatuses(block_ids, statuses);
}

UInt64 StorageStream::nextBlockId() const
{
    /// FIXME, per shard block ID ?
    if (kafka_log)
        return kafka_log->ingesting_blocks.nextId();

    return 0;
}

void StorageStream::poll(Int32 timeout_ms)
{
    assert(kafka_log);
    kafka_log->log->poll(timeout_ms, kafka_log->append_ctx);
}

std::vector<std::pair<Int32, Int64>> StorageStream::lastCommittedSequences() const
{
    std::vector<std::pair<Int32, Int64>> committed;
    for (const auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            committed.emplace_back(stream_shard->shard, stream_shard->lastSN());

    return committed;
}

void StorageStream::cacheVirtualColumnNamesAndTypes()
{
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_INGEST_TIME, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeInt64>()));
}

void StorageStream::updateLogStoreCodec(const String & settings_codec)
{
    if (settings_codec == "lz4")
        logstore_codec = CompressionMethodByte::LZ4;
    else if (settings_codec == "zstd")
        logstore_codec = CompressionMethodByte::ZSTD;
    else
        logstore_codec = CompressionMethodByte::NONE;
}

}
