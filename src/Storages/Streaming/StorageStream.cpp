#include "StorageStream.h"
#include "StreamSink.h"
#include "StreamingBlockReaderKafka.h"
#include "StreamingBlockReaderNativeLog.h"
#include "StreamingStoreSource.h"
#include "storageUtil.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
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
#include <KafkaLog/KafkaWALCommon.h>
#include <KafkaLog/KafkaWALPool.h>
#include <NativeLog/Server/NativeLog.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/StorageMergeTree.h>
#include <base/logger_useful.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>
#include <Common/timeScale.h>


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

    inline void assignSequenceID(nlog::RecordPtr & record)
    {
        auto * col = record->getBlock().findByName(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);
        if (!col)
            return;

        auto * seq_id_col = typeid_cast<ColumnInt64 *>(col->column->assumeMutable().get());
        if (seq_id_col)
        {
            auto & data = seq_id_col->getData();
            for (auto & item : data)
                item = record->getSN();
        }
    }

    inline void assignIndexTime(ColumnWithTypeAndName * index_time_col)
    {
        if (!index_time_col)
            return;

        /// index_time_col->column
        ///    = index_time_col->type->createColumnConst(moved_block.rows(), nowSubsecond(3))->convertToFullColumnIfConst();

        const auto * type = typeid_cast<const DataTypeDateTime64 *>(index_time_col->type.get());
        if (type)
        {
            auto scale = type->getScale();
            auto * col = typeid_cast<ColumnDecimal<DateTime64> *>(index_time_col->column->assumeMutable().get());
            assert(col);

            auto now = nowSubsecond(scale).get<DateTime64>();
            auto & data = col->getData();
            for (auto & item : data)
                item = now;
        }
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
        std::make_unique<MergeTreeSettings>(*settings_.get()), /// make a copy
        false, /// require_part_metadata
        attach_)
    , replication_factor(replication_factor_)
    , shards(shards_)
    , part_commit_pool(context_->getPartCommitPool())
    , rng(randomSeed())
    , max_outstanding_blocks(context_->getSettingsRef().aysnc_ingest_max_outstanding_blocks)
{
    cacheVirtualColumnNamesAndTypes();

    if (!relative_data_path_.empty())
    {
        storage = StorageMergeTree::create(
            table_id_,
            relative_data_path_,
            metadata_,
            attach_,
            context_,
            date_column_name_,
            merging_params_,
            std::move(settings_),
            has_force_restore_data_flag_);

        buildIdempotentKeysIndex(storage->lastIdempotentKeys());
        auto sn = storage->committedSN();
        if (sn >= 0)
        {
            std::lock_guard lock(sns_mutex);
            last_sn = sn;
            local_sn = sn;
        }

        LOG_INFO(log, "Load committed sn={}", sn);
    }

    for (Int32 shardId = 0; shardId < shards; ++shardId)
        slot_to_shard.push_back(shardId);

    if (sharding_key_)
    {
        sharding_key_expr = buildShardingKeyExpression(sharding_key_, getContext(), metadata_.getColumns().getAllPhysical(), false);
        sharding_key_is_deterministic = isExpressionActionsDeterministics(sharding_key_expr);
        sharding_key_column_name = sharding_key_->getColumnName();

        if (auto shard_func = sharding_key_->as<ASTFunction>())
            if (shard_func->name == "rand" || shard_func->name == "RAND")
                rand_sharding_key = true;
    }
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
    Block header = InterpreterSelectQuery(query_info.query, context_, SelectQueryOptions(processed_stage)).getSampleBlock();

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

    auto create_streaming_source = [this, header, storage_snapshot, context_](Int64 & max_sn_in_parts) {
        auto committed = storage->committedSN();
        if (max_sn_in_parts < 0)
        {
            /// Fallback to seek streaming store
            auto offsets = getOffsets(context_->getSettingsRef().seek_to.value);
            LOG_INFO(log, "Fused read fallbacks to seek stream. shard={}", currentShard());
            return std::make_shared<StreamingStoreSource>(
                shared_from_this(), header, storage_snapshot, context_, currentShard(), offsets[currentShard()], log);
        }
        else if (committed < max_sn_in_parts)
        {
            /// This happens if there are sequence ID gaps in the parts and system is bootstrapping to fill the gap
            /// Please refer to SequenceInfo.h for more details
            /// Fallback to seek streaming store
            auto offsets = getOffsets(context_->getSettingsRef().seek_to.value);
            LOG_INFO(
                log,
                "Fused read fallbacks to seek stream since sequence gaps are found for shard={}, max_sn_in_parts={}, "
                "current_committed_sn={}",
                currentShard(),
                max_sn_in_parts,
                committed);

            /// We need reset max_sn_in_parts to tell caller that we are seeking streaming store directly
            max_sn_in_parts = -1;
            return std::make_shared<StreamingStoreSource>(
                shared_from_this(), header, storage_snapshot, context_, currentShard(), offsets[currentShard()], log);
        }
        else
        {
            LOG_INFO(
                log,
                "Fused read for shard={}, read historical data up to sn={}, current_committed_sn={}",
                currentShard(),
                max_sn_in_parts,
                committed);
            return std::make_shared<StreamingStoreSource>(
                shared_from_this(), header, storage_snapshot, context_, currentShard(), max_sn_in_parts + 1, log);
        }
    };

    storage->readConcat(
        query_plan,
        column_names,
        storage_snapshot,
        query_info,
        context_,
        processed_stage,
        max_block_size,
        std::move(create_streaming_source));
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
    auto share_resource_group = (settings_ref.query_resource_group.value == "shared") && (settings_ref.seek_to.value == "latest");
    if (share_resource_group)
    {
        for (Int32 i = 0; i < shards; ++i)
        {
            if (!column_names.empty())
                pipes.emplace_back(source_multiplexers->createChannel(shard, column_names, storage_snapshot, context_));
            else
                pipes.emplace_back(source_multiplexers->createChannel(shard, {ProtonConsts::RESERVED_EVENT_TIME}, storage_snapshot, context_));
        }
    }
    else
    {
        /// auto consumer = klog::KafkaWALPool::instance(context_->getGlobalContext()).getOrCreateStreaming(streamingStorageClusterId());

        /// For queries like `SELECT count(*) FROM tumble(table, now(), 5s) GROUP BY window_end` don't have required column from table.
        /// We will need add one
        Block header;
        if (!column_names.empty())
            header = storage_snapshot->getSampleBlockForColumns(column_names, /* use_extended_objects */ false);
        else
            header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_TIME}, /* use_extended_objects */ false);

        auto offsets = getOffsets(settings_ref.seek_to.value);

        for (Int32 i = 0; i < shards; ++i)
            pipes.emplace_back(
                std::make_shared<StreamingStoreSource>(shared_from_this(), header, storage_snapshot, context_, i, offsets[i], log));
    }

    LOG_INFO(
        log,
        "Starting reading {} streams by seeking to {} in {} resource group",
        pipes.size(),
        settings_ref.seek_to.value,
        share_resource_group ? "shared" : "dedicated");
    auto read_step = std::make_unique<ReadFromStorageStep>(Pipe::unitePipes(std::move(pipes)), getName());
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
        if (settings_ref.seek_to.value == "earliest" && settings_ref.enable_backfill_from_historical_store.value
            && !requireDistributedQuery(context_))
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
        storage->read(query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
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

    source_multiplexers.reset(new StreamingStoreSourceMultiplexers(shared_from_this(), getContext(), log));

    initLog();

    if (!storage)
        return;

    LOG_INFO(log, "Starting");
    storage->startup();

    if (storage_settings.get()->storage_type.value == "streaming")
    {
        LOG_INFO(log, "Pure streaming storage mode, skip indexing data to historical store");
    }
    else
    {
        if (kafka)
        {
            if (storage_settings.get()->logstore_subscription_mode.value == "shared")
            {
                /// Shared mode, register callback
                addSubscriptionKafka();
                LOG_INFO(log, "Tailing streaming store in shared subscription mode");
            }
            else
            {
                /// Dedicated mode has dedicated poll thread
                poller.emplace(1);
                poller->scheduleOrThrowOnError([this] { backgroundPollKafka(); });
                LOG_INFO(log, "Tailing streaming store in dedicated subscription mode");
            }
        }
        else
        {
            /// Dedicated mode has dedicated poll thread. nativelog only supports dedicate mode for now
            poller.emplace(1);
            poller->scheduleOrThrowOnError([this] { backgroundPollNativeLog(); });
            LOG_INFO(log, "Tailing streaming store in dedicated subscription mode");
        }
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

    /// Wait for outstanding blocks
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
    if (storage)
    {
        if (poller)
        {
            poller->wait();
            /// Force delete pool
            poller.reset();
        }
        else
        {
            removeSubscriptionKafka();
        }
        storage->shutdown();
    }

    if (kafka)
        kafka->shutdown();

    LOG_INFO(log, "Stopped with outstanding_blocks={}", outstanding_blocks);
}

String StorageStream::getName() const
{
    return "Stream";
}

bool StorageStream::isRemote() const
{
    return !storage;
}

bool StorageStream::requireDistributedQuery(ContextPtr context_) const
{
    if (!storage)
    {
        return true;
    }

    /// If it has backing storage and it is a single shard table
    if (shards == 1)
    {
        return false;
    }

    const auto & client_info = context_->getClientInfo();
    if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
        /// If this query is a remote query already
        return false;
    }

    /// If it has backing storage and it is a multple shard table and
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
    return storage && storage->supportsSubcolumns();
}

std::optional<UInt64> StorageStream::totalRows(const Settings & settings) const
{
    assert(storage);
    return storage->totalRows(settings);
}

std::optional<UInt64> StorageStream::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr context_) const
{
    assert(storage);
    return storage->totalRowsByPartitionPredicate(query_info, context_);
}

std::optional<UInt64> StorageStream::totalBytes(const Settings & settings) const
{
    assert(storage);
    return storage->totalBytes(settings);
}

SinkToStoragePtr StorageStream::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_)
{
    return std::make_shared<StreamSink>(*this, metadata_snapshot, context_);
}

void StorageStream::checkTableCanBeDropped() const
{
    if (storage)
        storage->checkTableCanBeDropped();
}

void StorageStream::preDrop()
{
    shutdown();
    deinitNativeLog();
}

void StorageStream::drop()
{
    if (storage)
        storage->drop();
}

void StorageStream::preRename(const StorageID & new_table_id)
{
    if (!kafka)
    {
        auto & native_log = nlog::NativeLog::instance(getContext());
        assert(native_log.enabled());

        const auto & storage_id = getStorageID();
        nlog::RenameStreamRequest request(storage_id.getTableName(), new_table_id.getTableName());
        auto response{native_log.renameStream(storage_id.getDatabaseName(), request)};
        if (response.hasError())
            throw DB::Exception(response.error_code, "Failed to rename stream, error={}", response.error_message);
    }
}

void StorageStream::truncate(
    const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, TableExclusiveLockHolder & holder)
{
    assert(storage);
    storage->truncate(query, metadata_snapshot, context_, holder);
}

void StorageStream::alter(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder)
{
    assert(storage);
    storage->alter(commands, context_, alter_lock_holder);
    setInMemoryMetadata(storage->getInMemoryMetadata());
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
    assert(storage);
    return storage->optimize(query, metadata_snapshot, partition, finall, deduplicate, deduplicate_by_columns, context_);
}

void StorageStream::mutate(const MutationCommands & commands, ContextPtr context_)
{
    assert(storage);
    storage->mutate(commands, context_);
}

/// Return introspection information about currently processing or recently processed mutations.
std::vector<MergeTreeMutationStatus> StorageStream::getMutationsStatus() const
{
    assert(storage);
    return storage->getMutationsStatus();
}

CancellationCode StorageStream::killMutation(const String & mutation_id)
{
    assert(storage);
    return storage->killMutation(mutation_id);
}

ActionLock StorageStream::getActionLock(StorageActionBlockType action_type)
{
    assert(storage);
    return storage->getActionLock(action_type);
}

void StorageStream::onActionLockRemove(StorageActionBlockType action_type)
{
    assert(storage);
    storage->onActionLockRemove(action_type);
}

CheckResults StorageStream::checkData(const ASTPtr & query, ContextPtr context_)
{
    assert(storage);
    return storage->checkData(query, context_);
}

bool StorageStream::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    assert(storage);
    return storage->scheduleDataProcessingJob(assignee);
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
        return storage->getQueryProcessingStage(context_, to_stage, storage_snapshot, query_info);
    }
}

StorageSnapshotPtr StorageStream::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot) const
{
    assert(storage);
    return storage->getStorageSnapshot(metadata_snapshot);
}

void StorageStream::dropPartNoWaitNoThrow(const String & part_name)
{
    assert(storage);
    storage->dropPartNoWaitNoThrow(part_name);
}

void StorageStream::dropPart(const String & part_name, bool detach, ContextPtr context_)
{
    assert(storage);
    storage->dropPart(part_name, detach, context_);
}

void StorageStream::dropPartition(const ASTPtr & partition, bool detach, ContextPtr context_)
{
    assert(storage);
    storage->dropPartition(partition, detach, context_);
}

PartitionCommandsResultInfo
StorageStream::attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context_)
{
    assert(storage);
    return storage->attachPartition(partition, metadata_snapshot, part, context_);
}

void StorageStream::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context_)
{
    assert(storage);
    storage->replacePartitionFrom(source_table, partition, replace, context_);
}

void StorageStream::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context_)
{
    assert(storage);
    storage->movePartitionToTable(dest_table, partition, context_);
}

/// If part is assigned to merge or mutation (possibly replicated)
/// Should be overridden by children, because they can have different
/// mechanisms for parts locking
bool StorageStream::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    assert(storage);
    return storage->partIsAssignedToBackgroundOperation(part);
}

/// Return most recent mutations commands for part which weren't applied
/// Used to receive AlterConversions for part and apply them on fly. This
/// method has different implementations for replicated and non replicated
/// MergeTree because they store mutations in different way.
MutationCommands StorageStream::getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    assert(storage);
    return storage->getFirstAlterMutationCommandsForPart(part);
}

void StorageStream::startBackgroundMovesIfNeeded()
{
    assert(storage);
    return storage->startBackgroundMovesIfNeeded();
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
    {
        return nullptr;
    }

    ASTPtr condition_ast;
    if (select.prewhere() && select.where())
    {
        condition_ast = makeASTFunction("and", select.prewhere()->clone(), select.where()->clone());
    }
    else
    {
        condition_ast = select.prewhere() ? select.prewhere()->clone() : select.where()->clone();
    }

    replaceConstantExpressions(
        condition_ast,
        context_,
        storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns()),
        shared_from_this(),
        storage_snapshot);

    size_t limit = context_->getSettingsRef().optimize_skip_unused_shards_limit;
    if (!limit || limit > LONG_MAX)
    {
        throw Exception("optimize_skip_unused_shards_limit out of range (0, {}]", ErrorCodes::ARGUMENT_OUT_OF_BOUND, LONG_MAX);
    }
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
    {
        return nullptr;
    }

    std::set<int> shard_ids;

    for (const auto & block : *blocks)
    {
        if (!block.has(sharding_key_column_name))
        {
            throw Exception("sharding_key_expr should evaluate as a single row", ErrorCodes::TOO_MANY_ROWS);
        }

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
        {
            return optimized;
        }
    }

    UInt64 force = settings.force_optimize_skip_unused_shards;
    if (force)
    {
        WriteBufferFromOwnString exception_message;
        if (!sharding_key_expr)
        {
            exception_message << "No sharding key";
        }
        else if (!sharding_key_is_usable)
        {
            exception_message << "Sharding key is not deterministic";
        }
        else
        {
            exception_message << "Sharding key " << sharding_key_column_name << " is not used";
        }

        if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_ALWAYS)
        {
            throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);
        }

        if (force == FORCE_OPTIMIZE_SKIP_UNUSED_SHARDS_HAS_SHARDING_KEY && sharding_key_expr)
        {
            throw Exception(exception_message.str(), ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS);
        }
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
        return QueryProcessingStage::WithMergeableState;

    /// If there is only one node, the query can be fully processed by the
    /// shard, initiator will work as a proxy only.
    if (getClusterQueriedNodes(settings, cluster) == 1)
        return QueryProcessingStage::Complete;

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

    return QueryProcessingStage::WithMergeableState;
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

size_t StorageStream::getRandomShardIndex()
{
    std::lock_guard lock(rng_mutex);
    return std::uniform_int_distribution<size_t>(0, shards - 1)(rng);
}

size_t StorageStream::getNextShardIndex() const
{
    return next_shard++ % shards;
}

nlog::RecordSN StorageStream::lastSN() const
{
    std::lock_guard lock(sns_mutex);
    return last_sn;
}

void StorageStream::appendAsync(nlog::Record & record, UInt64 block_id, UInt64 sub_block_id)
{
    assert(kafka && kafka->log);

    if (outstanding_blocks > max_outstanding_blocks)
        throw Exception("Too many request", ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS);

    [[maybe_unused]] auto added = kafka->ingesting_blocks.add(block_id, sub_block_id);
    assert(added);

    auto data = std::make_unique<WriteCallbackData>(block_id, sub_block_id, this);
    auto ret = kafka->log->append(record, &StorageStream::writeCallback, data.get(), kafka->append_ctx);
    if (ret == ErrorCodes::OK)
        /// The writeCallback takes over the ownership of callback data
        data.release();
    else
        throw Exception("Failed to insert data async", ret);
}

void StorageStream::writeCallback(const klog::AppendResult & result, UInt64 block_id, UInt64 sub_block_id)
{
    assert(kafka);

    if (result.err)
    {
        kafka->ingesting_blocks.fail(block_id, result.err);
        LOG_ERROR(log, "[async] Failed to write sub_block_id={} in block_id={} error={}", sub_block_id, block_id, result.err);
    }
    else
    {
        kafka->ingesting_blocks.remove(block_id, sub_block_id);
        LOG_TRACE(log, "[async] Written sub_block_id={} in block_id={}", sub_block_id, block_id);
    }
}

void StorageStream::writeCallback(const klog::AppendResult & result, void * data)
{
    std::unique_ptr<StorageStream::WriteCallbackData> pdata(static_cast<WriteCallbackData *>(data));

    pdata->storage->writeCallback(result, pdata->block_id, pdata->sub_block_id);
}

/// Merge `rhs` block to `lhs`
void StorageStream::mergeBlocks(Block & lhs, Block & rhs)
{
    /// FIXME, we are assuming schema is not changed
    assert(blocksHaveEqualStructure(lhs, rhs));

    /// auto lhs_rows = lhs.rows();

    for (size_t pos = 0; auto & rhs_col : rhs)
    {
        auto & lhs_col = lhs.getByPosition(pos);
        //        if (unlikely(lhs_col == nullptr))
        //        {
        //            /// lhs doesn't have this column
        //            ColumnWithTypeAndName new_col{rhs_col.cloneEmpty()};
        //
        //            /// what about column with default expression
        //            new_col.column->assumeMutable()->insertManyDefaults(lhs_rows);
        //            lhs.insert(std::move(new_col));
        //            lhs_col = lhs.findByName(rhs_col.name);
        //        }

        lhs_col.column->assumeMutable()->insertRangeFrom(*rhs_col.column.get(), 0, rhs_col.column->size());
        ++pos;
    }

    /// lhs.checkNumberOfRows();
}

Int64 StorageStream::maxCommittedSN() const
{
    assert(storage);
    return storage->maxCommittedSN();
}

void StorageStream::commitSNLocal(nlog::RecordSN commit_sn)
{
    try
    {
        storage->commitSN(commit_sn);
        last_commit_ts = MonotonicSeconds::now();

        LOG_INFO(log, "Committed sn={} for shard={} to local file system", commit_sn, shard);

        std::lock_guard lock(sns_mutex);
        local_sn = commit_sn;
    }
    catch (...)
    {
        /// It is ok as next commit will override this commit if it makes through
        LOG_ERROR(
            log,
            "Failed to commit sn={} for shard={} to local file system exception={}",
            commit_sn,
            shard,
            getCurrentExceptionMessage(true, true));
    }
}

void StorageStream::commitSNRemote(nlog::RecordSN commit_sn)
{
    if (!kafka)
        return;

    /// Commit sequence number to dwal
    try
    {
        Int32 err = 0;
        if (kafka->multiplexer)
        {
            klog::TopicPartitionOffset tpo{kafka->topic(), shard, commit_sn};
            err = kafka->multiplexer->commit(tpo);
        }
        else
        {
            err = kafka->log->commit(commit_sn, kafka->consume_ctx);
        }

        if (unlikely(err != 0))
            /// It is ok as next commit will override this commit if it makes through
            LOG_ERROR(log, "Failed to commit sequence={} for shard={} to dwal error={}", commit_sn, shard, err);
    }
    catch (...)
    {
        LOG_ERROR(
            log,
            "Failed to commit sequence={} for shard={} to dwal exception={}",
            commit_sn,
            shard,
            getCurrentExceptionMessage(true, true));
    }
}

void StorageStream::commitSN()
{
    size_t outstanding_sns_size = 0;
    size_t local_committed_sns_size = 0;

    nlog::RecordSN commit_sn = -1;
    Int64 outstanding_commits = 0;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != prev_sn)
        {
            outstanding_commits = last_sn - local_sn;
            commit_sn = last_sn;
            prev_sn = last_sn;
        }
        outstanding_sns_size = outstanding_sns.size();
        local_committed_sns_size = local_committed_sns.size();
    }

    LOG_DEBUG(
        log,
        "Sequence outstanding_sns_size={} local_committed_sns_size={} for shard={}",
        outstanding_sns_size,
        local_committed_sns_size,
        shard);

    if (commit_sn < 0)
    {
        return;
    }

    /// Commit sequence number to local file system every 100 records
    if (outstanding_commits >= 100)
    {
        commitSNLocal(commit_sn);
    }

    commitSNRemote(commit_sn);
}

inline void StorageStream::progressSequencesWithoutLock(const SequencePair & seq)
{
    assert(!outstanding_sns.empty());

    if (seq != outstanding_sns.front())
    {
        /// Out of order committed sn
        local_committed_sns.insert(seq);
        return;
    }

    last_sn = seq.second;

    outstanding_sns.pop_front();

    /// Find out the max offset we can commit
    while (!local_committed_sns.empty())
    {
        auto & p = outstanding_sns.front();
        if (*local_committed_sns.begin() == p)
        {
            /// sn shall be consecutive
            assert(p.first == last_sn + 1);

            last_sn = p.second;

            local_committed_sns.erase(local_committed_sns.begin());
            outstanding_sns.pop_front();
        }
        else
        {
            break;
        }
    }

    assert(outstanding_sns.size() >= local_committed_sns.size());
    assert(last_sn >= prev_sn);
}

inline void StorageStream::progressSequences(const SequencePair & seq)
{
    std::lock_guard lock(sns_mutex);
    progressSequencesWithoutLock(seq);
}

void StorageStream::doCommit(
    Block block, SequencePair seq_pair, std::shared_ptr<IdempotentKeys> keys, SequenceRanges missing_sequence_ranges)
{
    {
        std::lock_guard lock(sns_mutex);
        assert(seq_pair.first > last_sn);
        /// We are sequentially consuming records, so seq_pair is always increasing
        outstanding_sns.push_back(seq_pair);

        /// After deduplication, we may end up with empty block
        /// We still mark these deduped blocks committed and moving forward
        /// the offset checkpointing
        if (!block)
        {
            progressSequencesWithoutLock(seq_pair);
            return;
        }

        assert(outstanding_sns.size() >= local_committed_sns.size());
    }

    /// Commit blocks to file system async
    part_commit_pool.scheduleOrThrowOnError([&,
                                             moved_block = std::move(block),
                                             moved_seq = std::move(seq_pair),
                                             moved_keys = std::move(keys),
                                             moved_sequence_ranges = std::move(missing_sequence_ranges),
                                             this] {
        while (!stopped.test())
        {
            try
            {
                auto sink = storage->write(nullptr, storage->getInMemoryMetadataPtr(), getContext());

                /// Setup sequence numbers to persistent them to file system
                auto merge_tree_sink = static_cast<MergeTreeSink *>(sink.get());
                merge_tree_sink->setSequenceInfo(std::make_shared<SequenceInfo>(moved_seq.first, moved_seq.second, moved_keys));
                merge_tree_sink->setMissingSequenceRanges(std::move(moved_sequence_ranges));

                merge_tree_sink->onStart();

                /// Reset index time here
                assignIndexTime(const_cast<ColumnWithTypeAndName *>(moved_block.findByName(ProtonConsts::RESERVED_INDEX_TIME)));

                merge_tree_sink->consume(Chunk(moved_block.getColumns(), moved_block.rows()));
                break;
            }
            catch (...)
            {
                LOG_ERROR(
                    log,
                    "Failed to commit rows={} for shard={} exception={} to file system",
                    moved_block.rows(),
                    shard,
                    getCurrentExceptionMessage(true, true));
                /// FIXME : specific error handling. When we sleep here, it occupied the current thread
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }
        }

        progressSequences(moved_seq);
    });

    assert(!block);
    assert(!keys);

    commitSN();
}

void StorageStream::buildIdempotentKeysIndex(const std::deque<std::shared_ptr<String>> & idempotent_keys_)
{
    idempotent_keys = idempotent_keys_;
    for (const auto & key : idempotent_keys)
    {
        idempotent_keys_index.emplace(*key);
    }
}

/// Add with lock held
inline void StorageStream::addIdempotentKey(const String & key)
{
    if (idempotent_keys.size() >= getContext()->getSettingsRef().max_idempotent_ids)
    {
        auto removed = idempotent_keys_index.erase(*idempotent_keys.front());
        (void)removed;
        assert(removed == 1);

        idempotent_keys.pop_front();
    }

    auto shared_key = std::make_shared<String>(key);
    idempotent_keys.push_back(shared_key);

    auto [iter, inserted] = idempotent_keys_index.emplace(*shared_key);
    assert(inserted);
    (void)inserted;
    (void)iter;

    assert(idempotent_keys.size() == idempotent_keys_index.size());
}

bool StorageStream::dedupBlock(const nlog::RecordPtr & record)
{
    if (!record->hasIdempotentKey())
    {
        return false;
    }

    auto & idem_key = record->idempotentKey();
    auto key_exists = false;
    {
        std::lock_guard lock{sns_mutex};
        key_exists = idempotent_keys_index.contains(idem_key);
        if (!key_exists)
        {
            addIdempotentKey(idem_key);
        }
    }

    if (key_exists)
    {
        LOG_INFO(log, "Skipping duplicate block, idempotent_key={} sn={}", idem_key, record->getSN());
        return true;
    }
    return false;
}

void StorageStream::commit(nlog::RecordPtrs records, SequenceRanges missing_sequence_ranges)
{
    if (records.empty())
        return;

    Block block;
    auto keys = std::make_shared<IdempotentKeys>();

    for (auto & rec : records)
    {
        if (likely(rec->opcode() == nlog::OpCode::ADD_DATA_BLOCK))
        {
            if (dedupBlock(rec))
                continue;

            assignSequenceID(rec);

            if (likely(block))
            {
                /// Merge next block
                /// assign event sequence ID to block events
                mergeBlocks(block, rec->getBlock());
            }
            else
            {
                /// First block
                block.swap(rec->getBlock());
                assert(!rec->getBlock());
            }

            if (rec->hasIdempotentKey())
                keys->emplace_back(rec->getSN(), std::move(rec->idempotentKey()));
        }
        else if (rec->opcode() == nlog::OpCode::ALTER_DATA_BLOCK)
        {
            /// FIXME: execute the later before doing any ingestion
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Log opcode {} is not implemented", static_cast<uint8_t>(rec->opcode()));
        }
    }

    LOG_DEBUG(
        log, "Committing records={} rows={} bytes={} for shard={} to file system", records.size(), block.rows(), block.bytes(), shard);

    doCommit(
        std::move(block),
        std::make_pair(records.front()->getSN(), records.back()->getSN()),
        std::move(keys),
        std::move(missing_sequence_ranges));
    assert(!block);
    assert(!keys);
    assert(missing_sequence_ranges.empty());
}

nlog::RecordSN StorageStream::snLoaded() const
{
    std::lock_guard lock(sns_mutex);
    if (local_sn >= 0)
        /// Sequence number committed on disk is sequence of a record
        /// `plus one` is the next sequence expecting
        return local_sn + 1;

    if (kafka)
        return -1000; /// STORED
    else
        return nlog::toSN(storage_settings.get()->logstore_auto_offset_reset.value);
}

void StorageStream::backgroundPollNativeLog()
{
    setThreadName("StorageStream");

    const auto & missing_sequence_ranges = storage->missingSequenceRanges();

    auto ssettings = storage_settings.get();

    LOG_INFO(
        log,
        "Start consuming records from shard={} sn={} distributed_flush_threshold_ms={} "
        "distributed_flush_threshold_count={} "
        "distributed_flush_threshold_bytes={} with missing_sequence_ranges={}",
        shard,
        snLoaded(),
        ssettings->distributed_flush_threshold_ms.value,
        ssettings->distributed_flush_threshold_count,
        ssettings->distributed_flush_threshold_bytes,
        sequenceRangesToString(missing_sequence_ranges));

    StreamCallbackData stream_commit{this, missing_sequence_ranges};

    StreamingBlockReaderNativeLog block_reader(
        shared_from_this(), shard, snLoaded(), /*max_wait_ms*/ 1000, /*read_buf_size*/ 16 * 1024 * 1024, &stream_commit, nlog::ALL_SCHEMA, {}, log);

    std::vector<char> read_buf(16 * 1024 * 1024 + 8 * 1024, '\0');

    while (!stopped.test())
    {
        try
        {
            /// Check if we have something to commit
            /// Every 10 seconds, flush the local file system checkpoint
            if (MonotonicSeconds::now() - last_commit_ts >= 10)
                periodicallyCommit();

            auto records{block_reader.read()};
            if (!records.empty())
                stream_commit.commit(std::move(records));
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(log, "Failed to consume data for shard={}, error={}", shard, e.message());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to consume data for shard={}", shard));
        }
    }

    stream_commit.wait();

    /// When tearing down, commit whatever it has
    finalCommit();
}

void StorageStream::backgroundPollKafka()
{
    /// Sleep a while to let librdkafka to populate topic / partition metadata
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    setThreadName("StorageStream");

    const auto & missing_sequence_ranges = storage->missingSequenceRanges();

    LOG_INFO(
        log,
        "Start consuming records from shard={} sn={} distributed_flush_threshold_ms={} "
        "distributed_flush_threshold_count={} "
        "distributed_flush_threshold_bytes={} with missing_sequence_ranges={}",
        shard,
        kafka->consume_ctx.offset,
        kafka->consume_ctx.consume_callback_timeout_ms,
        kafka->consume_ctx.consume_callback_max_rows,
        kafka->consume_ctx.consume_callback_max_bytes,
        sequenceRangesToString(missing_sequence_ranges));

    callback_data = std::make_unique<StreamCallbackData>(this, missing_sequence_ranges);

    while (!stopped.test())
    {
        try
        {
            auto err = kafka->log->consume(&StorageStream::consumeCallback, callback_data.get(), kafka->consume_ctx);
            if (err != ErrorCodes::OK)
            {
                LOG_ERROR(log, "Failed to consume data for shard={}, error={}", shard, err);
                /// FIXME, more error code handling
                std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            }

            /// Check if we have something to commit
            /// Every 10 seconds, flush the local file system checkpoint
            if (MonotonicSeconds::now() - last_commit_ts >= 10)
                periodicallyCommit();
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(log, "Failed to consume data for shard={}, error={}", shard, e.message());
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to consume data for shard={}", shard));
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }

    kafka->log->stopConsume(kafka->consume_ctx);

    callback_data->wait();

    /// When tearing down, commit whatever it has
    finalCommit();
}

inline void StorageStream::finalCommit()
{
    commitSN();

    nlog::RecordSN commit_sn = -1;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != local_sn)
            commit_sn = last_sn;
    }

    if (commit_sn >= 0)
        commitSNLocal(commit_sn);
}

inline void StorageStream::periodicallyCommit()
{
    nlog::RecordSN remote_commit_sn = -1;
    nlog::RecordSN commit_sn = -1;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != local_sn)
            commit_sn = last_sn;

        if (prev_sn != last_sn)
        {
            remote_commit_sn = last_sn;
            prev_sn = last_sn;
        }
    }

    if (commit_sn >= 0)
        commitSNLocal(commit_sn);

    if (remote_commit_sn >= 0)
        commitSNRemote(remote_commit_sn);

    last_commit_ts = MonotonicSeconds::now();
}

void StorageStream::consumeCallback(nlog::RecordPtrs records, klog::ConsumeCallbackData * data)
{
    auto * cdata = dynamic_cast<StreamCallbackData *>(data);

    if (records.empty())
    {
        cdata->storage->periodicallyCommit();
        return;
    }

    cdata->commit(std::move(records));
}

void StorageStream::addSubscriptionKafka()
{
    const auto & missing_sequence_ranges = storage->missingSequenceRanges();
    callback_data = std::make_unique<StreamCallbackData>(this, missing_sequence_ranges);

    const auto & cluster_id = storage_settings.get()->logstore_cluster_id.value;
    kafka->multiplexer = klog::KafkaWALPool::instance(getContext()).getOrCreateConsumerMultiplexer(cluster_id);

    klog::TopicPartitionOffset tpo{kafka->topic(), shard, snLoaded()};
    auto res = kafka->multiplexer->addSubscription(tpo, &StorageStream::consumeCallback, callback_data.get());
    if (res.err != ErrorCodes::OK)
        throw Exception("Failed to add subscription for shard=" + std::to_string(shard), res.err);

    kafka->shared_subscription_ctx = res.ctx;

    LOG_INFO(
        log,
        "Start consuming records from shard={} sn={} in shared mode "
        "with missing_sequence_ranges={}",
        shard,
        tpo.offset,
        sequenceRangesToString(missing_sequence_ranges));
}

void StorageStream::removeSubscriptionKafka()
{
    if (!kafka || !kafka->multiplexer)
        /// It is possible that storage is called with `shutdown` without calling `startup`
        /// during system startup and partially deleted table gets cleaned up
        return;

    klog::TopicPartitionOffset tpo{kafka->topic(), shard, snLoaded()};
    auto res = kafka->multiplexer->removeSubscription(tpo);
    if (res != ErrorCodes::OK)
        throw Exception("Failed to remove subscription for shard=" + std::to_string(shard), res);

    callback_data->wait();

    while (!kafka->shared_subscription_ctx.expired())
    {
        LOG_INFO(log, "Waiting for subscription context to get away for shard={}", shard);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    LOG_INFO(log, "Removed subscription for shard={}", shard);

    finalCommit();
    kafka->multiplexer.reset();
}

String StorageStream::streamingStorageClusterId() const
{
    return storage_settings.get()->logstore_cluster_id.value;
}

void StorageStream::initLog()
{
    auto ssettings = storage_settings.get();
    shard = ssettings->shard.value;
    if (shard < 0)
        shard = 0;

    const auto & logstore = ssettings->logstore.value;

    if (logstore == ProtonConsts::LOGSTORE_NATIVE_LOG || nlog::NativeLog::instance(getContext()).enabled())
        initNativeLog();
    else if (logstore == ProtonConsts::LOGSTORE_KAFKA || klog::KafkaWALPool::instance(getContext()).enabled())
        initKafkaLog();
    else
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Logstore type {} is not supported", logstore);
}

void StorageStream::initKafkaLog()
{
    if (!klog::KafkaWALPool::instance(getContext()).enabled())
        return;

    auto ssettings = storage_settings.get();

    const auto & storage_id = getStorageID();
    auto kafka_log = std::make_unique<KafkaLog>(
        toString(storage_id.uuid),
        shards,
        replication_factor,
        getContext()->getSettingsRef().async_ingest_block_timeout_ms,
        log);

    kafka.swap(kafka_log);

    const auto & offset_reset = ssettings->logstore_auto_offset_reset.value;
    if (offset_reset != "earliest" && offset_reset != "latest")
        throw Exception(
            "Invalid logstore_auto_offset_reset, only 'earliest' and 'latest' are supported", ErrorCodes::INVALID_CONFIG_PARAMETER);

    Int32 dwal_request_required_acks = 1;
    auto acks = ssettings->logstore_request_required_acks.value;
    if (acks >= -1 && acks <= replication_factor)
        dwal_request_required_acks = acks;
    else
        throw Exception(
            "Invalid logstore_request_required_acks, shall be in [-1, " + std::to_string(replication_factor) + "] range",
            ErrorCodes::INVALID_CONFIG_PARAMETER);

    Int32 dwal_request_timeout_ms = 30000;
    auto timeout = ssettings->logstore_request_timeout_ms.value;
    if (timeout > 0)
        dwal_request_timeout_ms = timeout;

    default_ingest_mode = toIngestMode(ssettings->distributed_ingest_mode.value);

    kafka->log = klog::KafkaWALPool::instance(getContext()).get(ssettings->logstore_cluster_id.value);
    assert(kafka->log);

    if (storage)
    {
        /// Init consume context only when it has backing storage
        kafka->consume_ctx.partition = shard;
        kafka->consume_ctx.offset = snLoaded();
        kafka->consume_ctx.auto_offset_reset = ssettings->logstore_auto_offset_reset.value;
        kafka->consume_ctx.consume_callback_timeout_ms = ssettings->distributed_flush_threshold_ms.value;
        kafka->consume_ctx.consume_callback_max_rows = ssettings->distributed_flush_threshold_count;
        kafka->consume_ctx.consume_callback_max_bytes = ssettings->distributed_flush_threshold_bytes;
        kafka->log->initConsumerTopicHandle(kafka->consume_ctx);
    }

    /// Init produce context
    /// Cached ctx, reused by append. Multiple threads are accessing append context
    /// since librdkafka topic handle is thread safe, so we are good
    kafka->append_ctx.request_required_acks = dwal_request_required_acks;
    kafka->append_ctx.request_timeout_ms = dwal_request_timeout_ms;
    kafka->log->initProducerTopicHandle(kafka->append_ctx);
}

void StorageStream::initNativeLog()
{
    auto & native_log = nlog::NativeLog::instance(getContext());
    if (!native_log.enabled())
        return;

    auto ssettings = storage_settings.get();
    const auto & storage_id = getStorageID();

    nlog::CreateStreamRequest request{storage_id.getTableName(), storage_id.uuid, shards, static_cast<UInt8>(replication_factor)};
    request.flush_messages = ssettings->logstore_flush_messages.value;
    request.flush_ms = ssettings->logstore_flush_ms.value;
    request.retention_bytes = ssettings->logstore_retention_bytes;
    request.retention_ms = ssettings->logstore_retention_ms;

    auto list_resp{native_log.listStreams(storage_id.getDatabaseName(), nlog::ListStreamsRequest(storage_id.getTableName()))};
    if (list_resp.hasError() || list_resp.streams.empty())
    {
        auto create_resp{native_log.createStream(storage_id.getDatabaseName(), request)};
        if (create_resp.hasError())
            throw DB::Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to create stream, error={}", create_resp.errString());
        else
            LOG_INFO(log, "Log was provisioned successfully");
    }
}

void StorageStream::deinitNativeLog()
{
    auto & native_log = nlog::NativeLog::instance(getContext());
    if (!native_log.enabled())
        return;

    const auto & storage_id = getStorageID();
    nlog::DeleteStreamRequest request{storage_id.getTableName(), storage_id.uuid};
    for (Int32 i = 0; i < 3; ++i)
    {
        auto resp{native_log.deleteStream(storage_id.getDatabaseName(), request)};
        if (resp.hasError())
        {
            LOG_ERROR(log, "Failed to clean up log, error={}", resp.errString());
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        else
        {
            LOG_INFO(log, "Cleaned up log");
            break;
        }
    }
}

std::vector<Int64> StorageStream::getOffsets(const String & seek_to) const
{
    auto utc_ms = parseSeekTo(seek_to, true);

    if (utc_ms == nlog::LATEST_SN || utc_ms == nlog::EARLIEST_SN)
    {
        return std::vector<Int64>(shards, utc_ms);
    }
    else
    {
        if (kafka)
            return kafka->log->offsetsForTimestamps(kafka->topic(), utc_ms, shards);
        else
            return sequencesForTimestamps(utc_ms);
    }
}

std::vector<Int64> StorageStream::sequencesForTimestamps(Int64 ts, bool append_time) const
{
    const auto & storage_id = getStorageID();

    std::vector<int32_t> shard_ids;
    shard_ids.reserve(shards);

    for (Int32 i = 0; i < shards; ++i)
        shard_ids.push_back(i);

    nlog::TranslateTimestampsRequest request{nlog::Stream{storage_id.getTableName(), storage_id.uuid}, std::move(shard_ids), ts, append_time};
    auto & native_log = nlog::NativeLog::instance(getContext());
    auto response{native_log.translateTimestamps(storage_id.getDatabaseName(), request)};
    if (response.hasError())
        throw Exception(response.error_code, "Failed to translate timestamps to record sequences");

    return response.sequences;
}

void StorageStream::cacheVirtualColumnNamesAndTypes()
{
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_INGEST_TIME, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeInt64>()));
}
}
