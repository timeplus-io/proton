#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamShardStore.h>
#include <Storages/Stream/StreamSink.h>

#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/SchemaRecord/SchemaControl.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterUtil.h>
#include <Interpreters/Streaming/DDLHelper.h>
#include <Interpreters/executeSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/Streaming/ParserUtil.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Streaming/ChangelogConvertStep.h>
#include <Processors/QueryPlan/Streaming/ChangelogStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/DistributedQuery.h>
#include <Storages/PruneShards.h>
#include <Storages/StorageMergeTree.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>
#include <Common/randomSeed.h>

#include <span>
#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int OK;
extern const int UNABLE_TO_SKIP_UNUSED_SHARDS;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int UNKNOWN_EXCEPTION;
extern const int UNSUPPORTED;
extern const int UNSUPPORTED_PARAMETER;
extern const int RESOURCE_NOT_INITED;
extern const int RESOURCE_NOT_FOUND;
}

namespace
{

String makeFormattedShards(const StorageStream::ShardsToRead & shards_to_read)
{
    return fmt::format(
        "{}:{}",
        magic_enum::enum_name(shards_to_read.mode),
        fmt::join(
            shards_to_read.shards | std::views::transform([](const auto & shard) {
                return fmt::format("{}({})", shard->shard(), shard->isVirtualReplica() ? "remote" : "local");
            }),
            ","));
}

/// \return true if all replicas are virtual, otherwise false
bool areAllReplicasVirtual(std::span<const StreamShardStorePtr> shards)
{
    for (const auto & shard : shards)
        if (!shard->isVirtualReplica())
            return false;

    return true;
}

bool hasAnyVirtualReplica(std::span<const StreamShardStorePtr> shards)
{
    for (const auto & shard : shards)
        if (shard->isVirtualReplica())
            return true;

    return false;
}

}

StorageStream::StorageStream(
    UInt32 shards_,
    const ASTPtr & sharding_expr_,
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
          metadata_,
          context_,
          date_column_name_,
          merging_params_,
          std::make_unique<MergeTreeSettings>(*settings_), /// make a copy
          false, /// require_part_metadata
          attach_,
          /*shard_=*/cluster::Nulls::NullShardID)
    , is_local(settings_->local)
    , shards(shards_)
    , rng(randomSeed())
    , metrics(MonotonicMilliseconds::now())
{
    /// track the used StoragePolicy in DatabaseCatalog
    DatabaseCatalog::instance().addStoragePolicyInUse(getStorageID(), getStoragePolicy());

    initializeDirectoriesAndFormatVersion(relative_data_path_, attach_, date_column_name_);


    /// simplified initialization process
    /// 1) Validate table properties at InterpreterCreateQuery layers
    /// 2) Provision a table object and validate settings
    /// 3) Commit the table definition to file system
    /// 4) Startup the new table which then can service requests

    max_outstanding_blocks = context_->getSettingsRef().aysnc_ingest_max_outstanding_blocks;

    assert(sharding_expr_);
    {
        sharding_key_expr = buildShardingKeyExpression(sharding_expr_, getContext(), metadata_.getColumns().getAllPhysical(), false);
        sharding_key_is_deterministic = isExpressionActionsDeterministics(sharding_key_expr);
        sharding_key_column_name = sharding_expr_->getColumnName();

        if (auto * shard_func = sharding_expr_->as<ASTFunction>())
            if (shard_func->name == "rand" || shard_func->name == "RAND")
                rand_sharding_key = true;
    }

    /// Save the init params
    init_params = std::make_unique<InitParams>();
    init_params->attach = attach_;
    init_params->has_force_restore_data_flag = has_force_restore_data_flag_;
    init_params->date_column_name = date_column_name_;
    init_params->merging_params = merging_params_;
    init_params->metadata = metadata_;
    init_params->context = context_;
}

void StorageStream::init()
{
    cacheVirtualColumnNamesAndTypes();

    auto inmemory = isInmemory();
    auto table_id = getStorageID();
    auto ssettings = storage_settings.get();

    // create default shard IDs
    StreamShardStorePtrs stream_shards_init;
    stream_shards_init.reserve(shards);
    slot_to_shard.reserve(shards);

    std::vector<uint32_t> shard_ids;
    for (uint32_t i = 0; i < shards; ++i)
        shard_ids.push_back(i);
    
    for (auto shard_id : shard_ids)
    {
        stream_shards_init.push_back(std::make_shared<StreamShardStore>(
            shard_id,
            table_id,
            relative_data_path,
            init_params->metadata,
            init_params->attach,
            init_params->context,
            init_params->date_column_name,
            init_params->merging_params,
            std::make_unique<MergeTreeSettings>(*ssettings),
            init_params->has_force_restore_data_flag,
            inmemory,
            *this));

        slot_to_shard.push_back(shard_id);
    }

    stream_shards = std::move(stream_shards_init);

    /// TODO: For proton on kafka, disable compression in kafka topic if the settings is set:
    /// 1) During creation, disable kafka topic compression and init
    /// 2) After altered, reinit kafka wal.
    /// For now, only works under NativeLog
    updateLogStoreCodec(ssettings->logstore_codec);
}

NamesAndTypesList StorageStream::getVirtuals() const
{
    return virtual_column_names_and_types;
}

NamesAndTypesList StorageStream::getVirtualsHistorical() const
{
    return MergeTreeData::getVirtuals();
}

NamesAndTypesList StorageStream::getVirtualsOfAppendOnly()
{
    /// Copied from getVirtuals()
    return NamesAndTypesList{
        NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeInt64>()),
        NameAndTypePair(ProtonConsts::RESERVED_INGEST_TIME, std::make_shared<DataTypeInt64>()),
        NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeInt64>()),
        NameAndTypePair(ProtonConsts::RESERVED_SHARD, std::make_shared<DataTypeInt32>()),
        LightweightDeleteDescription::FILTER_COLUMN,
    };
}

void StorageStream::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    checkReady();

    auto shards_to_read = getShardsToRead(context_, storage_snapshot, query_info);
    if (shards_to_read.mode != QueryMode::Streaming && hasAnyVirtualReplica(shards_to_read.shards))
    {
        if (!query_info.getCluster())
            query_info.cluster = getCluster(context_);
    }

    if (query_info.left_input_tracking_changes)
        return doReadChangelog(
            shards_to_read,
            query_plan,
            column_names,
            storage_snapshot,
            query_info,
            std::move(context_),
            processed_stage,
            max_block_size,
            num_streams);

    return doRead(
        shards_to_read,
        query_plan,
        column_names,
        storage_snapshot,
        query_info,
        std::move(context_),
        processed_stage,
        max_block_size,
        num_streams);
}

void StorageStream::doRead(
    const ShardsToRead & shards_to_read,
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto description = makeFormattedShards(shards_to_read);
    LOG_DEBUG(log, "Read {}", description);

    /// Streaming read always uses the minimum number of threads unless the user specifies a different value with the setting \min_threads.
    size_t streaming_shard_num_streams = std::max<size_t>(
        1ul, (context_->getSettingsRef().min_threads.value + shards_to_read.shards.size() - 1) / shards_to_read.shards.size());
    size_t shard_num_streams = std::max<size_t>(1ul, num_streams / shards_to_read.shards.size());
    /// Specially, we always read by single thread for keyed storage
    if (Streaming::isKeyedStorage(dataStreamSemantic()))
        shard_num_streams = 1;

    size_t max_threads_for_streaming_read = 0;

    std::vector<QueryPlanPtr> plans;
    plans.reserve(shards);

    for (const auto & stream_shard : shards_to_read.shards)
    {
        auto plan = std::make_unique<QueryPlan>();

        switch (shards_to_read.mode)
        {
            case QueryMode::Streaming:
            {
                stream_shard->readStreaming(
                    *plan, column_names, storage_snapshot, query_info, context_, max_block_size, streaming_shard_num_streams);

                max_threads_for_streaming_read += streaming_shard_num_streams;
                break;
            }
            case QueryMode::StreamingConcat:
            {
                stream_shard->readConcat(
                    *plan,
                    column_names,
                    storage_snapshot,
                    query_info,
                    context_,
                    processed_stage,
                    max_block_size,
                    shard_num_streams,
                    streaming_shard_num_streams);

                max_threads_for_streaming_read
                    += std::max(plan->getMaxThreads() ? plan->getMaxThreads() : shard_num_streams, streaming_shard_num_streams);
                break;
            }
            case QueryMode::Historical:
            {
                stream_shard->readHistorical(
                    *plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, shard_num_streams);
                break;
            }
        }

        /// If shard has no data skip it or we can create emtpy source like
        /// InterpreterSelectQuery::addEmptySourceToQueryPlan(...) does
        if (plan->isInitialized())
            plans.push_back(std::move(plan));
    }

    if (plans.empty())
        return;

    if (plans.size() == 1)
    {
        query_plan = std::move(*plans.front());
    }
    else
    {
        DataStreams input_streams;
        input_streams.reserve(plans.size());
        for (auto & plan : plans)
            input_streams.emplace_back(plan->getCurrentDataStream());

        auto union_step = std::make_unique<UnionStep>(std::move(input_streams));
        union_step->setStepDescription(std::move(description));
        query_plan.unitePlans(std::move(union_step), std::move(plans));
    }

    /// Limit the number of threads for streaming read
    if (max_threads_for_streaming_read > 0)
        query_plan.setMaxThreads(max_threads_for_streaming_read);
}

void StorageStream::doReadChangelog(
    const ShardsToRead & shards_to_read,
    QueryPlan & query_plan,
    Names column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    Names original_required_columns = column_names;
    auto add_version_column = [&] {
        assert(!merging_params.version_column.empty());

        if (query_info.changelog_query_drop_late_rows && *query_info.changelog_query_drop_late_rows)
        {
            /// Add version column
            if (std::find(column_names.begin(), column_names.end(), merging_params.version_column) == column_names.end())
                column_names.push_back(merging_params.version_column);
        }
    };

    if (Streaming::isChangelogStorage(dataStreamSemantic()))
    {
        /// Add _tp_delta column if it is necessary
        if (std::find(column_names.begin(), column_names.end(), ProtonConsts::RESERVED_DELTA_FLAG) == column_names.end())
            column_names.push_back(ProtonConsts::RESERVED_DELTA_FLAG);
    }
    else if (Streaming::isChangelogKVStorage(dataStreamSemantic()))
    {
        assert(!merging_params.sign_column.empty() && merging_params.sign_column == ProtonConsts::RESERVED_DELTA_FLAG);

        /// Add _tp_delta column if it is necessary
        if (std::find(column_names.begin(), column_names.end(), ProtonConsts::RESERVED_DELTA_FLAG) == column_names.end())
            column_names.push_back(ProtonConsts::RESERVED_DELTA_FLAG);

        add_version_column();
    }
    else if (Streaming::isVersionedKVStorage(dataStreamSemantic()))
    {
        /// Drop _tp_delta since we will generate that on the fly
        if (auto iter = std::find(column_names.begin(), column_names.end(), ProtonConsts::RESERVED_DELTA_FLAG); iter != column_names.end())
            column_names.erase(iter);

        if (auto iter = std::find(original_required_columns.begin(), original_required_columns.end(), ProtonConsts::RESERVED_DELTA_FLAG);
            iter != original_required_columns.end())
            original_required_columns.erase(iter);

        /// For versioned-kv, we always read back primary key columns and version column
        auto primary_key_columns = storage_snapshot->metadata->getPrimaryKeyColumns();
        for (auto & primary_key_column : primary_key_columns)
        {
            if (std::find(column_names.begin(), column_names.end(), primary_key_column) == column_names.end())
                column_names.emplace_back(std::move(primary_key_column));
        }

        add_version_column();
    }
    else
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Storage semantic '{}' is not supported",
            magic_enum::enum_name(dataStreamSemantic().toStorageSemantic()));
    }

    if (shards_to_read.mode == QueryMode::Historical)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Historical query against key value stream is not supported to convert to changelog");

    doRead(shards_to_read, query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);

    if (Streaming::isChangelogKVStorage(dataStreamSemantic()) || Streaming::isChangelogStorage(dataStreamSemantic()))
    {
        auto output_header
            = storage_snapshot->getSampleBlockForColumns(original_required_columns.empty() ? column_names : original_required_columns);

        query_plan.addStep(std::make_unique<Streaming::ChangelogStep>(
            query_plan.getCurrentDataStream(),
            output_header,
            storage_snapshot->metadata->getPrimaryKeyColumns(),
            (query_info.changelog_query_drop_late_rows && *query_info.changelog_query_drop_late_rows) ? merging_params.version_column
                                                                                                      : ""));
    }
    else if (Streaming::isVersionedKVStorage(dataStreamSemantic()))
    {
        auto output_header
            = storage_snapshot->getSampleBlockForColumns(original_required_columns.empty() ? column_names : original_required_columns);

        const auto & settings_ref = context_->getSettingsRef();
        query_plan.addStep(std::make_unique<Streaming::ChangelogConvertStep>(
            query_plan.getCurrentDataStream(),
            std::move(output_header),
            storage_snapshot->metadata->getPrimaryKeyColumns(),
            (query_info.changelog_query_drop_late_rows && *query_info.changelog_query_drop_late_rows) ? merging_params.version_column : "",
            settings_ref.default_hash_table.value,
            context_->getSpillDirForCurrentQuery("changelog"),
            settings_ref.max_hot_keys.value,
            /*backfill_key_unique=*/true));
    }
    else
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Storage semantic '{}' is not supported",
            magic_enum::enum_name(dataStreamSemantic().toStorageSemantic()));
}

void StorageStream::startup()
{
    if (inited.test_and_set())
        return;

    init();

    size_t remote_shards = 0;

    {
        auto stream_shards_snapshot = stream_shards;
        for (const auto & stream_shard : stream_shards_snapshot)
        {
            remote_shards += stream_shard->isVirtualReplica();
            stream_shard->startup();
        }
    }

    physical_shards = static_cast<uint32_t>(shards - remote_shards);

    ready = true;

    LOG_INFO(log, "Started with {} local shards, {} remote shards", shards - remote_shards, remote_shards);
}

StorageStream::~StorageStream()
{
    try
    {
        shutdown(/*dropping=*/false);
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

void StorageStream::shutdown(bool dropping)
{
    if (stopped.test_and_set() || !inited.test())
        return;

    LOG_INFO(log, "Stopping");

    ready = false;

    dropping = is_dropped || dropping;

    std::unique_ptr<ThreadPool> shards_shutdown_pool(
        std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, shards));

    try
    {
        auto stream_shards_snapshot = stream_shards;
        for (auto & stream_shard : stream_shards_snapshot)
            shards_shutdown_pool->scheduleOrThrow(
                [stream_shard_moved = std::move(stream_shard), dropping]() { stream_shard_moved->shutdown(dropping); });

        shards_shutdown_pool->wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    /// untrack the used StoragePolicy
    DatabaseCatalog::instance().removeStoragePolicyInUse(getStorageID(), getStoragePolicy());

    LOG_INFO(log, "Stopped");
}

String StorageStream::getName() const
{
    return "Stream";
}

StorageStream::ShardsToRead StorageStream::getShardsToRead(
    const ContextPtr & local_context, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info) const
{
    if (!query_info.shards_to_query)
        query_info.shards_to_query = getPrunedShardsWithQueryMode(
            sharding_key_expr,
            sharding_key_is_deterministic,
            sharding_key_column_name,
            shared_from_this(),
            storage_snapshot,
            query_info,
            slot_to_shard,
            local_context,
            log.load());

    ShardsToRead result;

    /// Here we like to recalculate the query mode since for replay etc settings can be changed between
    /// query stage and read calculation which caused query mode change. Recalculating query mode is lightweight,
    /// so it is fine compared shards prune.
    result.mode = getQueryMode(shared_from_this(), query_info, local_context);
    result.shards.reserve(query_info.shards_to_query->shards.size());

    auto all_shards = stream_shards;
    for (auto shard : query_info.shards_to_query->shards)
    {
        chassert(shard < all_shards.size());
        result.shards.push_back(std::move(all_shards[shard]));
    }

    if (all_shards.size() > query_info.shards_to_query->shards.size())
        LOG_INFO(log, "Query shards=[{}] after pruning", fmt::join(query_info.shards_to_query->shards, ", "));

    return result;
}

std::optional<UInt64> StorageStream::totalRows(const Settings & settings) const
{
    /// For versioned-kv, we will need read on merge to get correct row count
    if (!isReady() || (!isAppendStorage(dataStreamSemantic()) && settings.compact_kv_stream.value))
        return {};

    if (isInmemory())
        return 0;

    auto stream_shards_snapshot = stream_shards;
    if (hasAnyVirtualReplica(stream_shards_snapshot))
        return {};

    std::optional<UInt64> rows;
    for (const auto & stream_shard : stream_shards_snapshot)
    {
        chassert(stream_shard->storage);
        auto shard_rows = stream_shard->storage->totalRows(settings);
        if (shard_rows.has_value())
        {
            if (rows.has_value())
                *rows += *shard_rows;
            else
                rows = shard_rows;
        }
    }

    return rows;
}

std::optional<UInt64> StorageStream::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr context_) const
{
    /// For versioned-kv, we will need read on merge to get correct row count
    if (!isReady() || (!isAppendStorage(dataStreamSemantic()) && context_->getSettingsRef().compact_kv_stream.value))
        return {};

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
    if (!isReady())
        return {};

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
    checkReady();

    /// Check if exceed the total storage quota and license violation if will interrupt any running INSERT query
    context_->checkIngest(stream_shards.front()->streamShard().ns);

    bool count_public_query_metrics = !context_->isQueryFromMaterializedView();

    auto sink = std::make_shared<StreamSink>(
        std::dynamic_pointer_cast<StorageStream>(shared_from_this()), metadata_snapshot, std::move(context_));

    sink->setProgressCallback([&, count_public_query_metrics](const Progress & progress) {
        const auto & value = progress.getValues();
        metrics.written_bytes += value.written_bytes;
        metrics.written_rows += value.written_rows;

        if (count_public_query_metrics)
        {
            metrics.public_written_bytes += value.written_bytes;
            metrics.public_written_rows += value.written_rows;
        }
    });

    return sink;
}

void StorageStream::checkTableCanBeDropped(ContextPtr context_) const
{
    IStorage::checkTableCanBeDropped(context_);

    for (const auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->checkTableCanBeDropped(context_);
}

void StorageStream::checkAlterPartitionIsPossible(
    const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings) const
{
    for (const auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->checkAlterPartitionIsPossible(commands, metadata_snapshot, settings);

    MergeTreeData::checkAlterPartitionIsPossible(commands, metadata_snapshot, settings);
}

/// alterPartition proposes alter partition command the each stream shard via Raft consensus and commit in the
/// underlying NativeLog which holds the data.
/// Once the command is committed and every shard replica will pick it up in background to do the real partition altering.
Pipe StorageStream::alterPartition(
    const StorageMetadataPtr & metadata_snapshot, const PartitionCommands & commands, ContextPtr query_context)
{
    for (const auto & command : commands)
    {
        if (command.type != PartitionCommand::DROP_PARTITION)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported {} type", magic_enum::enum_name(command.type));

        auto table_id = getStorageID();
        auto partition_command_str = PartitionCommand::partitionCommandToString(command, table_id.table_name, table_id.database_name);
        auto record
            = cluster::ctrl::createAlterPartitionRecord(partition_command_str, query_context->getIdempotentKey(), /*schema_version=*/1);

        /// Broadcast ALTER PARTITION command to all shards of the stream
        const auto append_timeout_ms = query_context->getSettingsRef().insert_timeout_ms.value;
        for (const auto & stream_shard : stream_shards)
        {
            record->setShard(stream_shard->shard());

            int32_t retries = 0;
            int32_t errcode = ErrorCodes::OK;
            static constexpr int32_t max_retries = 3;

            while (retries < max_retries)
            {
                errcode = append(record, IngestMode::Sync, /*callback=*/{}, /*callback_data=*/{}, append_timeout_ms);
                if (errcode == ErrorCodes::OK)
                    break;

                LOG_ERROR(
                    log,
                    "Failed to commit alter partition command={}. Error: {}. Retry attempt: {} of {}",
                    partition_command_str,
                    DB::ErrorCodes::getName(errcode),
                    retries + 1,
                    max_retries);

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                ++retries;
            }

            if (retries == max_retries)
            {
                throw DB::Exception(
                    errcode, "Failed to alter partition after {} attempts. Partition command: {}", max_retries, partition_command_str);
            }
        }
    }

    return {};
}

void StorageStream::drop()
{
    Stopwatch stopwatch;

    LOG_INFO(log, "Dropping stream");

    shutdown(/*dropping=*/true);

    for (const auto & stream_shard : stream_shards)
        stream_shard->drop();

    LOG_INFO(log, "Dropped stream, took={}ms", stopwatch.elapsedMilliseconds());
}

void StorageStream::preRename(const StorageID & new_table_id)
{
    for (const auto & stream_shard : stream_shards)
        stream_shard->renameInMemory(new_table_id);
}

void StorageStream::truncate(
    const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, TableExclusiveLockHolder & holder)
{
    auto table_id = getStorageID();
    auto truncate_command = DB::queryToString(query);
    auto record = cluster::ctrl::createTruncateRecord(truncate_command, context_->getIdempotentKey(), /*schema_version=*/1);

    /// Broadcast Truncate command to all shards of the stream
    for (const auto & stream_shard : stream_shards)
    {
        record->setShard(stream_shard->shard());

        int32_t retries = 0;
        int32_t errcode = ErrorCodes::OK;
        static constexpr int32_t max_retries = 3;

        while (retries < max_retries)
        {
            errcode = append(record, IngestMode::Sync, /*callback=*/{}, /*callback_data=*/{}, /*timeout_ms=*/3000);
            if (errcode == ErrorCodes::OK)
                break;

            LOG_ERROR(
                log,
                "Failed to commit truncate table, command={}. Error: {}. Retry attempt: {} of {}",
                truncate_command,
                DB::ErrorCodes::getName(errcode),
                retries + 1,
                max_retries);

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            ++retries;
        }

        if (retries == max_retries)
        {
            throw DB::Exception(errcode, "Failed to truncate table after {} attempts. command={}", max_retries, truncate_command);
        }
    }
}

void StorageStream::applyAlterCommandsToAllShards(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder)
{
    /// First apply the altering to shard storage
    for (const auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            if (commands.front().type == AlterCommand::Type::RENAME_COLUMN)
            {
                auto new_context = Context::createCopy(context_);
                /// always wait for the rename column mutation to finish
                new_context->setSetting("alter_sync", 2);
                stream_shard->storage->alter(commands, new_context, alter_lock_holder);
            }
            else
            {
                stream_shard->storage->alter(commands, context_, alter_lock_holder);
            }
        }
    }
}

void StorageStream::alter(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder)
{
    auto table_id = getStorageID();

    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    commands.apply(new_metadata, context_);

    /// Inner storage works like a local stream, we apply the necessary changes locally first.
    if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, context_); table && table->getName() == getName())
    {
        /// For DROP_INDEX with clear=true (CLEAR INDEX), we don't update metadata
        /// For all other operations (including regular DROP_INDEX), we do update metadata
        if (!(commands.front().type == AlterCommand::Type::DROP_INDEX && commands.front().clear))
        {
            DatabaseCatalog::instance()
                .getDatabase(table_id.database_name)
                ->alterTable(context_, table_id, new_metadata, commands.front().typeString(), commands.astCommands(table_id));
        }

        /// If the metadata change will touch data as well, for example, DROP INDEX etc, we like to append the control block
        /// to the data log to ask each shard to drop the index
        if (commands.front().type == AlterCommand::Type::DROP_INDEX)
        {
            for (const auto & command : commands)
            {
                auto alter_command_ast = command.toASTString(table_id.table_name, table_id.database_name);
                /// Broadcast drop index command to all shards of the stream
                auto record = cluster::ctrl::createDropIndexRecord(
                    alter_command_ast,
                    context_->getIdempotentKey(),
                    /*schema_version=*/1);
                for (const auto & stream_shard : stream_shards)
                {
                    record->setShard(stream_shard->shard());
                    append(record, IngestMode::Sync, /*callback=*/{}, /*callback_data=*/{}, /*timeout_ms=*/3000);
                }
            }
        }
    }
    else
    {
        updateSettingsAndVersionForInnerStorage(new_metadata);
    }
}

void StorageStream::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    chassert(!commands.empty());

    auto command_type = commands.front().type;
    auto same_alter_command = std::ranges::all_of(commands, [&](const auto & command) { return command.type == command_type; });

    if (!same_alter_command)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Multi different alter commands are not supported for {} storage engine. Alter commands=[{}]",
            getName(),
            fmt::join(commands.stringCommands(), ", "));

    static std::unordered_set<AlterCommand::Type> supported_multi_command_types = {AlterCommand::Type::ADD_COLUMN};

    if (commands.size() > 1 && !supported_multi_command_types.contains(command_type))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Multi {} alter commands are not supported for {} storage engine",
            commands.front().typeString(),
            getName());

    /// When renaming a column, we need to consider the following:
    /// 1. Existing streaming queries will continue to work correctly with the new column name
    /// 2. However, materialized views dependent on this table WILL FAIL after server restart
    ///    because they reference columns by their original names
    /// This check prevents column renaming if there are dependent materialized views
    if (command_type == AlterCommand::Type::RENAME_COLUMN)
    {
        stream_shards.back()->storage->checkTableCanBeRenamed(local_context);

        IStorage::checkTableCanBeRenamed(local_context);

        /// Block renaming of reserved columns
        for (const auto & command : commands)
        {
            if (std::find(ProtonConsts::RESERVED_COLUMN_NAMES.begin(), ProtonConsts::RESERVED_COLUMN_NAMES.end(), command.column_name)
                != ProtonConsts::RESERVED_COLUMN_NAMES.end())
            {
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED, "Cannot rename reserved column '{}' in {} storage engine", command.column_name, getName());
            }
        }
    }

    static std::unordered_set<AlterCommand::Type> supported_types
        = {AlterCommand::Type::ADD_COLUMN,
           AlterCommand::Type::RENAME_COLUMN,
           AlterCommand::Type::MODIFY_SETTING,
           AlterCommand::Type::MODIFY_TTL,
           AlterCommand::Type::REMOVE_TTL,
           AlterCommand::Type::ADD_INDEX,
           AlterCommand::Type::DROP_INDEX,
           AlterCommand::Type::COMMENT_TABLE};

    for (const auto & command : commands)
    {
        if (!supported_types.contains(command.type))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Alter command of type '{}' is not supported by storage {}", command.typeString(), getName());

        if (command.type == AlterCommand::Type::MODIFY_SETTING)
        {
            static const std::set<String> allowed_settings{
                "logstore_flush_messages", "logstore_flush_ms", "logstore_retention_bytes", "logstore_retention_ms"};
            for (const auto & [setting_name, _] : command.settings_changes)
            {
                if (!allowed_settings.contains(setting_name))
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Modify setting {} is not supported, only {} settings can be modified",
                        setting_name,
                        fmt::join(allowed_settings, ", "));
            }
        }
    }

    MergeTreeData::checkAlterIsPossible(commands, local_context);
}

void StorageStream::alterLogSettings(
    const std::unordered_map<std::string, int32_t> & flush_settings, const std::unordered_map<std::string, int64_t> & retention_settings)
{
    auto stream_shard_snapshot = stream_shards;
    if (stream_shard_snapshot.back()->isNativeLog())
    {
        for (auto & stream_shard : stream_shard_snapshot)
            stream_shard->alterLogSettings(flush_settings, retention_settings);
    }
}

bool StorageStream::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    ContextPtr query_context)
{
    /// Broadcast optimize command to all shards of the stream
    auto record = cluster::ctrl::createOptimizeRecord(
        query ? queryToString(query, true) : "",
        final,
        deduplicate,
        deduplicate_by_columns,
        query_context->getIdempotentKey(),
        /*schema_version=*/1);

    for (const auto & stream_shard : stream_shards)
    {
        record->setShard(stream_shard->shard());
        append(record, IngestMode::Sync, /*callback=*/{}, /*callback_data=*/{}, /*timeout_ms=*/3'000);
    }

    return true;
}

void StorageStream::mutate(const MutationCommands & commands, ContextPtr query_context)
{
    chassert(commands.size() == 1);
    const auto & mutate_command = commands[0];
    switch (mutate_command.type)
    {
        case MutationCommand::Type::UPDATE:
        {
            auto table_id = getStorageID();
            /// Broadcast update command to all shards of the stream
            auto record = cluster::ctrl::createDeleteRowsRecord(
                mutate_command.toAlterCommandString(table_id.table_name, table_id.database_name),
                query_context->getIdempotentKey(),
                /*schema_version=*/1);
            for (const auto & stream_shard : stream_shards)
            {
                record->setShard(stream_shard->shard());
                append(record, IngestMode::Sync, /*callback=*/{}, /*callback_data=*/{}, /*timeout_ms=*/3000);
            }
            break;
        }
        case MutationCommand::Type::MATERIALIZE_INDEX:
        {
            auto table_id = getStorageID();
            /// Broadcast materialize index command to all shards of the stream
            auto record = cluster::ctrl::createSecondaryIndexRebuildRecord(
                mutate_command.toAlterCommandString(table_id.table_name, table_id.database_name),
                /*truncate_index=*/false,
                /*wait_meta_change=*/false,
                query_context->getIdempotentKey(),
                /*schema_version=*/1);
            for (const auto & stream_shard : stream_shards)
            {
                record->setShard(stream_shard->shard());
                append(record, IngestMode::Sync, /*callback=*/{}, /*callback_data=*/{}, /*timeout_ms=*/3000);
            }
            break;
        }
        default:
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", mutate_command.type, getName());
        }
    }
}

void StorageStream::checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const
{
    if (commands.size() != 1)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Multi alter commands are not supported for {} storage engine. Alter commands={}",
            getName(),
            fmt::join(commands.stringCommands(), ", "));

    static const std::unordered_set<MutationCommand::Type> supported_mutations = {
        MutationCommand::Type::UPDATE,
        MutationCommand::Type::MATERIALIZE_INDEX,
        MutationCommand::Type::DROP_INDEX,
    };

    for (const auto & command : commands)
    {
        if (!supported_mutations.contains(command.type))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Mutation command={} is not supported by {} storage engine.", command.typeString(), getName());
    }

    MergeTreeData::checkMutationIsPossible(commands, settings);
}

void StorageStream::updateSettingsAndVersionForInnerStorage(const StorageInMemoryMetadata & new_metadata)
{
    /// inner_metadata.setVersion(inner_metadata.getVersion() + 1);
    setInMemoryMetadata(new_metadata);
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
    /// Grap the first shard's action lock as the whole storage stream action lock
    return stream_shards.back()->storage->getActionLock(action_type);
}

void StorageStream::onActionLockRemove(StorageActionBlockType action_type)
{
    for (auto & stream_shard : stream_shards)
        if (stream_shard->storage)
            stream_shard->storage->onActionLockRemove(action_type);
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
    auto shards_to_read = getShardsToRead(context_, storage_snapshot, query_info);
    if (shards_to_read.mode == QueryMode::Historical)
    {
        /// For now, we assume all shards of a stream will be co-located on the same node
        if (hasAnyVirtualReplica(shards_to_read.shards))
            return getHistoricalQueryProcessingStageRemote(
                query_info,
                to_stage,
                storage_snapshot,
                context_,
                shared_from_this(),
                sharding_key_expr,
                sharding_key_is_deterministic,
                sharding_key_column_name,
                slot_to_shard,
                log.load());
        else
            /// Use the last shard is good enough
            return shards_to_read.shards.back()->storage->getQueryProcessingStage(context_, to_stage, storage_snapshot, query_info);
    }
    else
    {
        /// For streaming and streaming concat, we only support fetching columns from remote for now
        return QueryProcessingStage::Enum::FetchColumns;
    }
}

StorageSnapshotPtr StorageStream::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    checkReady(); /// Make sure the stream shards are ready

    assert(!stream_shards.empty());

    auto & storage = stream_shards.back()->storage;

    std::shared_ptr<StorageSnapshot> storage_snapshot;
    if (!storage)
        /// for virtual table
        storage_snapshot = IStorage::getStorageSnapshot(metadata_snapshot, query_context)->clone();
    else
        storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, query_context)->clone();

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
MutationCommands StorageStream::getAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    MutationCommands results;
    for (const auto & stream_shard : stream_shards)
    {
        if (stream_shard->storage)
        {
            auto cmds = stream_shard->storage->getAlterMutationCommandsForPart(part);
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
ClusterPtr StorageStream::getCluster(ContextPtr context_) const
{
    // return empty cluster
    return nullptr;
}

IColumn::Selector StorageStream::createSelector(const ColumnWithTypeAndName & result) const
{
    return DB::createSelector(result, slot_to_shard);
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

size_t StorageStream::getRandomShardIndex() const noexcept
{
    std::lock_guard lock(rng_mutex);
    return std::uniform_int_distribution<size_t>(0, shards - 1)(rng);
}

UInt32 StorageStream::getNextShardIndex() const noexcept
{
    return next_shard++ % shards;
}

int StorageStream::append(
    cluster::SchemaRecordPtr & record,
    IngestMode ingest_mode,
    cluster::klog::AppendCallback callback,
    cluster::klog::CallbackData data,
    int64_t timeout_ms)
{
    record->setCompressionCodec(logstore_codec);
    return appendToNativeLog(record, ingest_mode, std::move(callback), std::move(data), timeout_ms);
}

inline int StorageStream::appendToNativeLog(
    cluster::SchemaRecordPtr & record,
    IngestMode ingest_mode,
    cluster::klog::AppendCallback callback,
    cluster::klog::CallbackData data,
    int64_t timeout_ms)
{
    record->setAppendTime(UTCMilliseconds::now());

    assert(record->getShard() < stream_shards.size());
    const auto & stream_shard = stream_shards[record->getShard()];

    try
    {
        auto [_, max_event_time] = record->minMaxEventTime();
        if (auto append_result
            = stream_shard->append(record->serialize(), max_event_time, ingestModeToAckSemantic(ingest_mode), timeout_ms);
            !append_result.hasError())
        {
            /// TODO, audit append total size etc
            /// TODO: NativeLog support more ingest mode
            if (callback)
            {
                callback(
                    cluster::klog::AppendResult{
                        .err = DB::ErrorCodes::OK, .sn = append_result.result, .partition = static_cast<int32_t>(record->getShard())},
                    data);
            }
            return DB::ErrorCodes::OK;
        }
        else
        {
            /// FIXME, throw ?
            if (callback)
            {
                callback(
                    cluster::klog::AppendResult{
                        .err = append_result.err.error_code, .sn = -1, .partition = static_cast<int32_t>(record->getShard())},
                    data);
            }
            return append_result.err.error_code;
        }
    }
    catch (...)
    {
        /// Catch exception:
        /// 1) Invoke callback first
        /// 2) Rethrow
        int32_t error_code = getCurrentExceptionCode();
        if (callback)
        {
            callback(cluster::klog::AppendResult{.err = error_code, .sn = -1, .partition = static_cast<int32_t>(record->getShard())}, data);
        }

        return error_code;
    }
}


IngestMode StorageStream::ingestMode() const
{
    checkReady();
    return stream_shards.back()->ingestMode();
}

void StorageStream::poll(Int32 timeout_ms)
{
}

void StorageStream::cacheVirtualColumnNamesAndTypes()
{
    const auto & type_factory = DataTypeFactory::instance();
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, type_factory.get("int64")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_INGEST_TIME, type_factory.get("int64")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, type_factory.get("int64")));

    if (!getInMemoryMetadataPtr()->columns.has(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID))
        virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID, type_factory.get("int64")));

    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_SHARD, type_factory.get("int32")));

    /// We may emit _tp_delta on the fly
    if (Streaming::isKeyValueStorage(dataStreamSemantic()))
        virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_DELTA_FLAG, type_factory.get("int8")));

    virtual_column_names_and_types.push_back(LightweightDeleteDescription::FILTER_COLUMN);
}

bool StorageStream::hasLightweightDeletedMask() const
{
    for (const auto & shard : stream_shards)
        if (shard->storage && shard->storage->hasLightweightDeletedMask())
            return true;

    return false;
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

void StorageStream::checkReady() const
{
    if (!isReady())
        throw Exception(
            ErrorCodes::RESOURCE_NOT_INITED,
            "Stream '{}' is still initializing or was already shutdown",
            getStorageID().getFullTableName());
}

std::vector<int64_t> StorageStream::getLastSNs() const
{
    std::vector<int64_t> last_sns;
    for (const auto & stream_shard : stream_shards)
        last_sns.emplace_back(stream_shard->lastSN());

    return last_sns;
}

/// \return true if all replicas are virtual, otherwise false
bool StorageStream::isVirtualStorage() const
{
    checkReady();
    return areAllReplicasVirtual(stream_shards);
}

UInt64 StorageStream::getLogStoreDiskSize() const
{
    if (!isReady())
        return 0;

    auto stream_shards_snapshot = stream_shards;
    return std::accumulate(
        stream_shards_snapshot.begin(), stream_shards_snapshot.end(), static_cast<uint64_t>(0), [](uint64_t sum, const auto & shard) {
            return sum + shard->getLogStoreDiskSize();
        });
}

UInt64 StorageStream::getStorageSize() const
{
    if (!isReady())
        return 0;

    auto stream_shards_snapshot = stream_shards;
    return std::accumulate(
        stream_shards_snapshot.begin(), stream_shards_snapshot.end(), static_cast<uint64_t>(0), [](uint64_t sum, const auto & shard) {
            return sum + shard->getStorageSize();
        });
}

UInt32 StorageStream::getPhysicalShards() const noexcept
{
    if (!isReady())
        return 0;

    return physical_shards;
}

Int64 StorageStream::getMetricTime(bool reset)
{
    if (reset)
        return metrics.metric_time.exchange(MonotonicMilliseconds::now(), std::memory_order_relaxed);
    else
        return metrics.metric_time.load(std::memory_order_relaxed);
}

UInt64 StorageStream::readBytes(bool reset, bool external_ingress)
{
    auto & read_bytes_ref = external_ingress ? metrics.public_read_bytes : metrics.read_bytes;

    if (reset)
        return read_bytes_ref.exchange(0, std::memory_order_relaxed);
    else
        return read_bytes_ref.load(std::memory_order_relaxed);
}

UInt64 StorageStream::readRows(bool reset, bool external_ingress)
{
    auto & read_rows_ref = external_ingress ? metrics.public_read_rows : metrics.read_rows;

    if (reset)
        return read_rows_ref.exchange(0, std::memory_order_relaxed);
    else
        return read_rows_ref.load(std::memory_order_relaxed);
}

UInt64 StorageStream::writtenBytes(bool reset, bool external_ingress)
{
    auto & written_bytes_ref = external_ingress ? metrics.public_written_bytes : metrics.written_bytes;

    if (reset)
        return written_bytes_ref.exchange(0, std::memory_order_relaxed);
    else
        return written_bytes_ref.load(std::memory_order_relaxed);
}

UInt64 StorageStream::writtenRows(bool reset, bool external_ingress)
{
    auto & written_rows_ref = external_ingress ? metrics.public_written_rows : metrics.written_rows;

    if (reset)
        return written_rows_ref.exchange(0, std::memory_order_relaxed);
    else
        return written_rows_ref.load(std::memory_order_relaxed);
}


UInt64 StorageStream::maxEntrySize() const
{
    assert(!stream_shards.empty());
    return stream_shards.back()->maxEntrySize();
}

std::vector<ShardCommittedSequence> StorageStream::committedSequences() const
{
    std::vector<ShardCommittedSequence> committed;
    for (const auto & shard : stream_shards)
    {
        if (!shard->isVirtualReplica())
            committed.emplace_back(shard->shard(), shard->lastSN());
    }

    return committed;
}

std::vector<ShardAppliedSequence> StorageStream::appliedSequences() const
{
    std::vector<ShardAppliedSequence> shard_applied_sns;
    auto all_shards = stream_shards;
    for (const auto & shard : all_shards)
    {
        if (!shard->isVirtualReplica())
            shard_applied_sns.emplace_back(shard->shard(), shard->appliedSequence().value_or(0));
    }

    return shard_applied_sns;
}

String StorageStream::getCurrentSNsSignature() const
{
    auto all_shards = stream_shards;
    return fmt::format(
        "{}",
        fmt::join(
            all_shards | std::views::transform([](auto & shard) { return std::to_string(shard->appliedSequence().value_or(0)); }), ","));
}

IStorage::SnapshotDataWithExpiration StorageStream::readSnapshot(const Names & required_columns, Int64 default_cache_expire_sec)
{
    QueryPlan query_plan;
    SelectQueryInfo query_info;
    query_info.original_query = query_info.query = buildSelectFromTable(getStorageID(), required_columns);
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();

    ShardsToRead shards_to_read;
    shards_to_read.mode = QueryMode::Historical;
    shards_to_read.shards = stream_shards;
    auto storage_snapshot = getStorageSnapshot(getInMemoryMetadataPtr(), query_context);
    doRead(
        shards_to_read,
        query_plan,
        required_columns,
        storage_snapshot,
        query_info,
        query_context,
        QueryProcessingStage::FetchColumns,
        DEFAULT_BLOCK_SIZE,
        1);

    if (!query_plan.isInitialized())
    {
        auto header = storage_snapshot->getSampleBlockForColumns(required_columns);
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info, query_context);
    }

    ChunkList result;
    executeSelectPipeline(std::move(query_plan), query_context, [&](Chunk && chunk) {
        if (chunk.hasRows())
            result.push_back(std::move(chunk));
    });

    std::function<bool()> snapshot_expired;
    if (hasAnyVirtualReplica(shards_to_read.shards))
    {
        /// By default, we consider the data expired after \default_cache_expire_sec.
        snapshot_expired = [stopwatch = Stopwatch(), expire_sec = default_cache_expire_sec]() mutable {
            if (stopwatch.elapsedSeconds() > expire_sec)
            {
                stopwatch.restart();
                return true;
            }
            return false;
        };
    }
    else
    {
        /// Check if the SNs signature has changed
        snapshot_expired = [old_sn_signature = getCurrentSNsSignature(), this]() { return old_sn_signature != getCurrentSNsSignature(); };
    }

    return {std::move(result), std::move(snapshot_expired)};
}
}
