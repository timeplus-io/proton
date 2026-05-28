#include <Storages/Stream/StreamCallbackData.h>
#include <Storages/Stream/StreamShardStore.h>
#include <Storages/Stream/StreamingBlockReaderNativeLog.h>
#include <Storages/assignSequenceID.h>

#include <Bootstrap/Globals.h>
#include <Cluster/Common/CallResult.h>
#include <Cluster/Common/Constants.h>
#include <Cluster/Common/Nulls.h> 
#include <Cluster/LocalLog/Log/LogConfigMap.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Streaming/ConcatStep.h>
#include <Processors/QueryPlan/Streaming/DelayStep.h>
#include <Processors/Sources/MarkSource.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamingStoreSource.h>
#include <base/sleep.h>
#include <Common/ProtonCommon.h>
#include <Common/setThreadName.h>

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int INVALID_CONFIG_PARAMETER;
extern const int UNKNOWN_EXCEPTION;
extern const int RESOURCE_NOT_INITED;
extern const int SEQUENCE_COMPACTED_AWAY;
}

StreamShardStore::StreamShardStore(
    UInt32 shard_,
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach_,
    ContextMutablePtr context_,
    const String & date_column_name_,
    const MergeTreeData::MergingParams & merging_params_,
    std::unique_ptr<StreamSettings> settings_,
    bool has_force_restore_data_flag_,
    bool inmemory_,
    StorageStream & storage_stream_)
    : stream_shard(table_id_.getDatabaseName(), table_id_.getTableName(), table_id_.uuid, shard_)
    , inmemory(inmemory_)
    , storage_stream(storage_stream_)
    , part_commit_pool(context_->getStorageCommitPool())
    , logger(getLogger(stream_shard.logName()))
{
    // no replica set setup needed
    assert(shard_ != cluster::Nulls::NullShardID);

    if (!inmemory) // always real replica
    {
        assert(relative_data_path_.ends_with('/'));
        auto shard_path = fmt::format("{}{}/", relative_data_path_, shard_);
        storage = StorageMergeTree::create(
            table_id_,
            shard_path,
            metadata_,
            attach_,
            context_,
            date_column_name_,
            merging_params_,
            std::move(settings_),
            has_force_restore_data_flag_,
            shard_); // use actual shard ID

        idempotent_keys = std::make_unique<InMemoryIdempotentKeys>(
            context_->getSettingsRef().max_idempotent_ids.value, storage->lastIdempotentKeys(), logger);

        auto sn = storage->committedSN();
        if (sn >= 0)
        {
            std::lock_guard lock(sns_mutex);
            last_sn = sn;
            local_sn = sn;
        }

        LOG_INFO(logger, "Load committed sn={} from path={}", sn, shard_path);
    }
}

StreamShardStore::~StreamShardStore()
{
    try
    {
        shutdown(/*dropping=*/false);
    }
    catch (...)
    {
        tryLogCurrentException(logger, __PRETTY_FUNCTION__);
    }
}

void StreamShardStore::startup()
{
    if (started.test_and_set())
        return;

    initLog();

    /// For virtual stream shard or in-memory storage type, there is no storage object
    if (!storage)
        return;

    source_multiplexers.reset(new StreamingStoreSourceMultiplexers(shared_from_this(), storage_stream.getContext(), logger));

    storage->startup();

    auto storage_settings = storage_stream.getSettings();
    auto only_streaming_store = (storage_settings->storage_type.value == "streaming");

    /// We like setup the applied sn callback since committing to historical store is async
    /// We provide this way for the nativelog layer to know the latest applied sn on historical
    /// store, then nativelog can make a decision when to garbage collect the logs.
    /// Please note that we shared the underlying historical storage to the callback. Be careful
    /// with this object lifetime since during tearing down, StorageShardStore will be done prior
    /// its log.
    // Kafka not used
    if (true)
    {
        if (!only_streaming_store)
        {
            std::weak_ptr<StorageMergeTree> weak_storage = storage;
            Globals::getNativeLog().setAppliedSequenceGetter(stream_shard.id_shard, [weak_storage]() -> int64_t {
                if (auto historical_storage = weak_storage.lock(); historical_storage)
                    return historical_storage->committedSN();

                return 0;
            });
        }
    }

    if (only_streaming_store)
    {
        LOG_INFO(logger, "Pure streaming storage mode, skip indexing data to historical store");
    }
    else
    {
        // Kafka removed, only NativeLog supported
        {
            /// Dedicated mode has dedicated poll thread. NativeLog only supports dedicate mode for now
            poller.emplace(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 1);
            poller->scheduleOrThrowOnError([this] { backgroundPollNativeLog(); });
            LOG_INFO(logger, "Tailing NativeLog streaming store in dedicated subscription mode");
        }
    }
}

void StreamShardStore::shutdown(bool dropping)
{
    if (stopped.test_and_set() || !started.test())
        return;

    LOG_INFO(logger, "Stopping");

    is_dropping = is_dropping.load() || dropping;

    /// First shutdown log, which shall notify readers who waits on new data
    {
        // simplified shutdown
        Globals::getNativeLog().shutdown(streamIDShard());
    }

    if (poller)
    {
        /// Release internal resources may be a heavy operation, it shall be done in background to avoid blocking drop or recreate request:
        /// 1) Wait for the background poller thread to finish
        /// 2) Wait for internal storage shutdown (Need wait in-progressing merge task)

        poller->wait();
        /// Force delete pool
        poller.reset();
    }

    if (storage)
        storage->shutdown(dropping);
    else
        LOG_DEBUG(logger, "Skipping storage shutdown: component not initialized");

    // nlog_client.reset(); // Not used in

    LOG_INFO(logger, "Stopped");
}

void StreamShardStore::readStreaming(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    size_t /*max_block_size*/,
    size_t num_streams)
{
    chassert(query_info.seek_to_info);
    const auto & settings_ref = context->getSettingsRef();
    /// 1) Checkpointed queries shall not be multiplexed
    /// 2) Queries which seek to a specific timestamp shall not be multiplexed
    auto share_resource_group = (settings_ref.query_resource_group.value == "shared")
        && (query_info.seek_to_info->getSeekTo().empty() || query_info.seek_to_info->getSeekTo() == "latest")
        && (settings_ref.exec_mode == ExecuteMode::Normal);

    Pipe pipe;
    if (share_resource_group)
    {
        if (!column_names.empty())
            pipe = Pipe{source_multiplexers->createChannel(shard(), column_names, storage_snapshot, context)};
        else
            pipe = Pipe{source_multiplexers->createChannel(shard(), {ProtonConsts::RESERVED_EVENT_TIME}, storage_snapshot, context)};

        LOG_INFO(logger, "Starting reading shard-{} stream in shared resource group", shard());
    }
    else
    {
        /// For queries like `SELECT count(*) FROM tumble(table, now(), 5s) GROUP BY window_end` don't have required column from table.
        /// We will need add one
        Block header;
        if (!column_names.empty())
            header = storage_snapshot->getSampleBlockForColumns(column_names);
        else
            header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_TIME});

        auto offset = getOffset(query_info.seek_to_info);
        bool count_public_query_metrics = !context->isQueryFromMaterializedView();

        auto source = std::make_shared<StreamingStoreSource>(shared_from_this(), header, storage_snapshot, context, offset, logger);

        source->setProgressCallback([&, count_public_query_metrics](const Progress & progress) {
            const auto & value = progress.getValues();
            storage_stream.metrics.read_bytes += value.read_bytes;
            storage_stream.metrics.read_rows += value.read_rows;

            if (count_public_query_metrics)
            {
                storage_stream.metrics.public_read_bytes += value.read_bytes;
                storage_stream.metrics.public_read_rows += value.read_rows;
            }
        });

        source->setStream(shard());

        pipe = Pipe{std::move(source)};

        LOG_INFO(
            logger,
            "Starting reading shard-{} streams by seeking to '{}' with corresponding offsets='{}' in dedicated resource group",
            shard(),
            query_info.seek_to_info->getSeekTo(),
            offset);
    }

    if (num_streams > pipe.numOutputPorts())
        pipe.resize(num_streams);

    auto read_step = std::make_unique<ReadFromStorageShardStep>(
        std::move(pipe), share_resource_group ? "Stream(Shared)" : "Stream(Dedicated)", query_info.storage_limits);
    query_plan.addStep(std::move(read_step));
}

Int64 StreamShardStore::readHistorical(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    // no remote historical reads

    if (inmemory)
    {
        auto header = storage_snapshot->getSampleBlockForColumns(column_names);
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info, context);
        return -1;
    }

    const auto & settings_ref = context->getSettingsRef();

    size_t max_num_streams = num_streams;
    if (query_info.has_javascript_uda)
    {
        auto controlled_num_streams = settings_ref.javascript_uda_max_concurrency.value;
        if (max_num_streams > controlled_num_streams)
        {
            max_num_streams = controlled_num_streams;
            LOG_INFO(logger, "Limit JavaScript UDA table query's concurrency to {}", max_num_streams);
        }
    }

    chassert(storage);

    auto underlying_storage_snapshot = storage->getStorageSnapshot(storage_snapshot->metadata, context);

    /// Get the max sequence number from current data parts snapshot
    chassert(underlying_storage_snapshot->data);
    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*underlying_storage_snapshot->data);
    const auto & parts = snapshot_data.parts;
    Int64 max_sn = -1;
    for (const auto & part : parts)
    {
        if (part->seq_info)
            max_sn = std::max(max_sn, part->seq_info->maxSequenceID());
    }

    const Names * required_columns_p = &column_names;

    Names required_columns;
    if (settings_ref.remote_fetch_concat.value && isKeyedStorage(storage->dataStreamSemantic()))
    {
        /// For remote concat fetch historical data, the source query is a streaming query, we need read all of the primary
        /// key columns for historical data
        required_columns = column_names;
        auto primary_key_columns = storage_snapshot->metadata->getPrimaryKeyColumns();
        for (const auto & primary_key_column : primary_key_columns)
        {
            if (std::ranges::find(required_columns, primary_key_column) == required_columns.end())
                required_columns.push_back(primary_key_column);
        }

        if (isVersionedKVStorage(storage->dataStreamSemantic()))
        {
            if (std::ranges::find(required_columns, storage_stream.merging_params.version_column) == required_columns.end())
                required_columns.push_back(storage_stream.merging_params.version_column);
        }

        required_columns_p = &required_columns;
    }

    /// underlying_storage_snapshot->data is reset in read() so called after max_sn calculated.
    storage->read(
        query_plan,
        *required_columns_p,
        underlying_storage_snapshot,
        query_info,
        context,
        processed_stage,
        max_block_size,
        max_num_streams);

    /// Tells the max sequence number of historical data for remote read
    if (settings_ref.remote_fetch.value && max_sn >= 0)
    {
        auto header = query_plan.getCurrentDataStream().header;
        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
        plans.emplace_back(std::make_unique<QueryPlan>());

        auto chunk_ctx = ChunkContext::create();
        chunk_ctx->setSN(max_sn);
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(Pipe{std::make_shared<MarkSource>(header, std::move(chunk_ctx))});
        read_from_pipe->setStepDescription("Remote fetch End");
        plans.back()->addStep(std::move(read_from_pipe));

        DataStreams input_streams;
        input_streams.reserve(plans.size());
        for (const auto & plan_ : plans)
            input_streams.emplace_back(plan_->getCurrentDataStream());

        query_plan = {};
        query_plan.unitePlans(std::make_unique<Streaming::DelayStep>(std::move(input_streams)), std::move(plans));
    }

    return max_sn;
}


void StreamShardStore::readConcat(
    QueryPlan & query_plan,
    Names column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams,
    size_t streaming_num_streams)
{
    /// If required backfill input in order, we will need read `_tp_time`.
    if (query_info.require_in_order_backfill
        && std::ranges::none_of(column_names, [](const auto & name) { return name == ProtonConsts::RESERVED_EVENT_TIME; }))
        column_names.emplace_back(ProtonConsts::RESERVED_EVENT_TIME);

    auto historical_plan = std::make_unique<QueryPlan>();
    auto max_sn = readHistorical(
        *historical_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);

    auto streaming_plan = std::make_unique<QueryPlan>();
    readStreaming(*streaming_plan, column_names, storage_snapshot, query_info, context, max_block_size, streaming_num_streams);
    chassert(streaming_plan->isInitialized());

    /// If there is no historical data, we will fallback to seek streaming store
    if (!historical_plan->isInitialized())
    {
        query_plan = std::move(*streaming_plan);
        return;
    }

    const auto & header = streaming_plan->getCurrentDataStream().header;

    std::vector<QueryPlanPtr> plans;
    plans.reserve(3);
    {
        /// 1) Mark Historical Data Start Plan
        auto plan = std::make_unique<QueryPlan>();
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(
            Pipe{std::make_shared<MarkSource>(header, ProtonConsts::HISTORICAL_DATA_START_FLAG)});
        read_from_pipe->setStepDescription("Historical Data Start");
        plan->addStep(std::move(read_from_pipe));
        plans.emplace_back(std::move(plan));

        /// 2) Historical Data Plan (with sorting / converting ?)
        if (query_info.require_in_order_backfill)
        {
            /// TODO: support optimized order for the stream with ordered by `_tp_time`
            /// Copy basic code from `SortingStep::fullSort`
            /// Sorting backfilled historical data by ascending event time
            SortDescription sort_desc;
            sort_desc.emplace_back(ProtonConsts::RESERVED_EVENT_TIME, /*ascending*/ 1);

            auto addtional_sorting = std::make_unique<DB::SortingStep>(
                historical_plan->getCurrentDataStream(),
                std::move(sort_desc),
                0 /* LIMIT */,
                DB::SortingStep::Settings(*context),
                context->getSettingsRef().optimize_sorting_by_input_stream_properties);
            addtional_sorting->setStepDescription("Sorting for backfill");
            historical_plan->addStep(std::move(addtional_sorting));
        }

        /// Sometimes, historical source is not compatible with streaming source header
        /// its layout is always `<real columns> + <virtual columns>`, for example:
        /// `select _tp_shard, i from t1`, historical header is `i, _tp_shard`, but streaming header is `_tp_shard, i`
        if (!blocksHaveEqualStructure(historical_plan->getCurrentDataStream().header, header))
        {
            auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                historical_plan->getCurrentDataStream().header.getColumnsWithTypeAndName(),
                header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                true);

            auto converting = std::make_unique<DB::ExpressionStep>(historical_plan->getCurrentDataStream(), convert_actions_dag);
            historical_plan->addStep(std::move(converting));
        }
        plans.emplace_back(std::move(historical_plan));

        /// 3) Mark Historical Data End Plan
        plan = std::make_unique<QueryPlan>();
        auto chunk_ctx = ChunkContext::create();
        chunk_ctx->setMark(ProtonConsts::HISTORICAL_DATA_END_FLAG);
        if (max_sn >= 0)
            chunk_ctx->setSN(max_sn);

        read_from_pipe = std::make_unique<ReadFromPreparedSource>(Pipe{std::make_shared<MarkSource>(header, std::move(chunk_ctx))});
        read_from_pipe->setStepDescription("Historical Data End");
        plan->addStep(std::move(read_from_pipe));
        plans.emplace_back(std::move(plan));

        /// Unite historical plans via Streaming::DelayStep
        DataStreams input_streams;
        input_streams.reserve(plans.size());
        for (const auto & plan_ : plans)
            input_streams.emplace_back(plan_->getCurrentDataStream());

        historical_plan = std::make_unique<QueryPlan>();
        historical_plan->unitePlans(std::make_unique<Streaming::DelayStep>(std::move(input_streams)), std::move(plans));
    }

    /// Concat historical plan and streaming plan via Streaming::ConcatStep
    plans.clear();
    plans.emplace_back(std::move(historical_plan));
    plans.emplace_back(std::move(streaming_plan));

    DataStreams input_streams;
    input_streams.reserve(plans.size());
    for (const auto & plan : plans)
        input_streams.emplace_back(plan->getCurrentDataStream());

    query_plan.unitePlans(
        std::make_unique<Streaming::ConcatStep>(
            std::move(input_streams),
            /*disable_concat_callback_=*/false,
            context->getSettingsRef().backfill_max_threads.value),
        std::move(plans));
}

cluster::CallResultV<int64_t>
StreamShardStore::append(cluster::ByteVector && data, int64_t max_event_time, cluster::AckSemantic ack, int64_t timeout_ms)
{
    // call NativeLog directly
    if (storage_stream.isReady())
    {
        auto & native_log = Globals::getNativeLog();
        auto append_result = native_log.append(stream_shard.id_shard, std::move(data), max_event_time, ack, timeout_ms);

        if (append_result.hasError())
            return cluster::CallResultV<int64_t>{append_result.error_code, std::string{DB::ErrorCodes::getName(append_result.error_code)}};

        // Extract sequence number from AppendResult ( mode)
        if (append_result.result)
        {
            // In  mode, AppendResult is immediate (no wait needed)
            if (append_result.result->error_code != 0)
                return cluster::CallResultV<int64_t>{append_result.result->error_code, std::string{DB::ErrorCodes::getName(append_result.result->error_code)}};
            return cluster::CallResultV<int64_t>{append_result.result->sn};
        }
        else
            return cluster::CallResultV<int64_t>{DB::ErrorCodes::UNKNOWN_EXCEPTION, "Append returned null result"};
    }

    return cluster::CallResultV<int64_t>{DB::ErrorCodes::RESOURCE_NOT_INITED, "Stream was shutdown already or not initialized yet"};
}

void StreamShardStore::drop()
{
    if (isVirtualReplica())
        return;

    /// Wait for all resources shutdown before drop
    shutdown(/*dropping=*/true);

    auto remove_log = [this](auto do_remove, std::string_view log_type) {
        for (Int32 i = 0; i < 3; ++i)
        {
            try
            {
                if (auto err = do_remove(); err == DB::ErrorCodes::OK)
                {
                    break;
                }
                else
                {
                    LOG_ERROR(logger, "Failed to delete stream shard {} from {}, error_code={}", stream_shard.string(), log_type, err);
                    continue;
                }
            }
            catch (...)
            {
                tryLogCurrentException(logger, fmt::format("Failed to delete stream shard {} from {}", stream_shard.string(), log_type));
                sleepForMilliseconds(500);
            }
        }
    };

    {
        /// Remove NativeLog from file system
        auto & native_log = Globals::getNativeLog();

        remove_log([&]() { return native_log.remove({stream_shard}); }, "NativeLog");
    }

    if (storage)
        storage->drop();
}

/// void StreamShardStore::alter(const AlterCommands & commands, ContextPtr context_, std::unique_lock<std::timed_mutex> & alter_lock_holder)
/// {
///    if (storage)
///        storage->alter(commands, context_, alter_lock_holder);
/// }

void StreamShardStore::alterLogSettings(
    const std::unordered_map<std::string, int32_t> & flush_settings, const std::unordered_map<std::string, int64_t> & retention_settings)
{
    if (isVirtualReplica())
        return;

    auto & native_log = Globals::getNativeLog();
    if (auto err = native_log.alter({stream_shard}, flush_settings, retention_settings); err != DB::ErrorCodes::OK)
        throw DB::Exception(err, "Failed to update settings for {}", stream_shard.string());
}

void StreamShardStore::backgroundPollNativeLog()
{
    setThreadName("StreamShard");

    const auto & missing_sequence_ranges = storage->missingSequenceRanges();

    auto ssettings = storage_stream.getSettings();

    size_t rows_threshold = ssettings->flush_threshold_count.value;
    size_t bytes_threshold = ssettings->flush_threshold_bytes.value;
    Int64 interval_threshold = ssettings->flush_threshold_ms.value;

    LOG_INFO(
        logger,
        "Start consuming records from shard={} sn={} flush_threshold_ms={} flush_threshold_count={} "
        "flush_threshold_bytes={} with missing_sequence_ranges={}",
        shard(),
        snLoaded(),
        interval_threshold,
        rows_threshold,
        bytes_threshold,
        sequenceRangesToString(missing_sequence_ranges));

    StreamCallbackData stream_commit{this, missing_sequence_ranges};

    StreamingBlockReaderNativeLog block_reader(
        /*sn=*/
        snLoaded(),
        /*max_wait_ms_=*/std::min<Int64>(1000, interval_threshold),
        /*max_bytes_=*/std::min<Int64>(16 * 1024 * 1024, bytes_threshold),
        /*queued_max_bytes_=*/48 * 1024 * 1024,
        /*stream_shard_=*/stream_shard,
        /*storage=*/&storageStream(),
        /*sample_block=*/storageStream().getInMemoryMetadataPtr()->getSampleBlock(),
        /*query_schema_version=*/cluster::SchemaRecord::ANY_SCHEMA_VERSION,
        /*column_positions_=*/{},
        /*is_stopped_=*/[this] { return isStopped(); },
        /*is_local=*/isLocal(),
        logger);

    /// We like to retain control block for data mutation like drop partition, delete data etc
    block_reader.setEmitControlBlock(true);
    block_reader.setAllowFallbackToHistoricalStore(false);

    cluster::SchemaRecordPtrs batch;
    size_t batch_size = 100;
    batch.reserve(batch_size);

    size_t current_bytes_in_batch = 0;
    size_t current_rows_in_batch = 0;
    auto last_batch_commit = MonotonicMilliseconds::now();

    SCOPE_EXIT({
        if (is_dropping)
            return;

        /// Final batch
        if (!batch.empty())
            stream_commit.commit(std::move(batch));

        stream_commit.wait();

        /// When tearing down, commit whatever it has
        finalCommit();
    });

    while (!isStopped())
    {
        try
        {
            /// Check if we have something to commit
            /// Every 10 seconds, flush the local file system checkpoint
            if (MonotonicSeconds::now() - last_commit_ts >= 10)
                periodicallyCommit();

            auto records{block_reader.read()};
            for (auto & record : records)
            {
                current_bytes_in_batch += record->approximateSerializedSize();
                current_rows_in_batch += record->getBlock().rows();
                batch.push_back(std::move(record));
            }

            if ((current_bytes_in_batch >= bytes_threshold) || (current_rows_in_batch >= rows_threshold)
                || (MonotonicMilliseconds::now() - last_batch_commit >= interval_threshold))
            {
                if (!batch.empty())
                {
                    stream_commit.commit(std::move(batch));

                    /// We like to re-init `batch` as after move, its state is undefined
                    batch = cluster::SchemaRecordPtrs{};
                    batch.reserve(batch_size);

                    /// Reset
                    last_batch_commit = MonotonicMilliseconds::now();
                    current_bytes_in_batch = 0;
                    current_rows_in_batch = 0;
                }
            }
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::SEQUENCE_COMPACTED_AWAY)
            {
                /// The only case for now we will run into this case is snapshot case, in which scenario the current
                /// shard replica receives historical snapshot from its peer and the data in local log gets truncated
                /// to the snapshot sn which is committed in local file system.
                auto next_sn = storage->committedSN() + 1;
                block_reader.resetSequenceNumber(next_sn);
                LOG_INFO(logger, "Resetting block reader to next sn={} because of error '{}'", next_sn, e.message());
            }
            else
            {
                tryLogCurrentException(logger, fmt::format("Failed to consume data next_sn={}", storage->committedSN() + 1));
            }
            sleepForMilliseconds(2000);
        }
        catch (...)
        {
            tryLogCurrentException(logger, fmt::format("Failed to consume data next_sn={}", storage->committedSN() + 1));
            sleepForMilliseconds(2000);
        }
    }
}

void StreamShardStore::commitSNLocal(int64_t commit_sn)
{
    try
    {
        storage->commitSN(commit_sn);
        last_commit_ts = MonotonicSeconds::now();

        LOG_INFO(logger, "Committed sn={} to local file system", commit_sn);

        std::lock_guard lock(sns_mutex);
        local_sn = commit_sn;
    }
    catch (...)
    {
        /// It is ok as next commit will override this commit if it makes through
        LOG_ERROR(logger, "Failed to commit sn={} to local file system, exception={}", commit_sn, getCurrentExceptionMessage(true, true));
    }
}

void StreamShardStore::commitSNRemote(int64_t /*commit_sn*/)
{
}

void StreamShardStore::commitSN()
{
    size_t outstanding_sns_size = 0;
    size_t local_committed_sns_size = 0;

    int64_t commit_sn = -1;
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
        logger,
        "Sequence outstanding_sns_size={} local_committed_sns_size={} commit_sn={} outstanding_commits={}",
        outstanding_sns_size,
        local_committed_sns_size,
        commit_sn,
        outstanding_commits);

    if (commit_sn < 0)
        return;

    /// Commit sequence number to local file system every 100 records
    if (outstanding_commits >= 100)
        commitSNLocal(commit_sn);

    commitSNRemote(commit_sn);
}

inline void StreamShardStore::progressSequences(const SequencePair & seq)
{
    std::lock_guard lock(sns_mutex);
    progressSequencesWithLockHeld(seq);
}

inline void StreamShardStore::progressSequencesWithLockHeld(const SequencePair & seq)
{
    chassert(!outstanding_sns.empty());

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
        const auto & p = outstanding_sns.front();
        if (*local_committed_sns.begin() == p)
        {
            /// sn may be not consecutive due to non-data records
            chassert(p.first >= last_sn + 1);

            last_sn = p.second;

            local_committed_sns.erase(local_committed_sns.begin());
            outstanding_sns.pop_front();
        }
        else
        {
            break;
        }
    }

    chassert(outstanding_sns.size() >= local_committed_sns.size());
    chassert(last_sn >= prev_sn);

    storage->setInMemoryCommittedSN(last_sn);
}

void StreamShardStore::doCommit(
    Block block,
    SequencePair seq_pair,
    std::shared_ptr<IdempotentKeys> keys,
    SequenceRanges missing_sequence_ranges,
    StorageMetadataPtr metadata)
{
    if (!block)
    {
        std::lock_guard lock(sns_mutex);
        assert(seq_pair.first > last_sn);
        outstanding_sns.push_back(seq_pair);
        progressSequencesWithLockHeld(seq_pair);
        return;
    }

    {
        std::lock_guard lock(sns_mutex);
        assert(seq_pair.first > last_sn);
        outstanding_sns.push_back(seq_pair);
        assert(outstanding_sns.size() >= local_committed_sns.size());
    }

    /// We use trySchedule to avoid blocking the current thread for a long time when the pool is full.
    bool scheduled = false;
    using CommitData = std::tuple<Block, SequencePair, std::shared_ptr<IdempotentKeys>, SequenceRanges>;
    auto commit_data = std::make_shared<CommitData>(std::move(block), seq_pair, std::move(keys), std::move(missing_sequence_ranges));
    while (!scheduled)
    {
        if (isStopped())
        {
            LOG_INFO(logger, "Aborted scheduling part commit, current_sn_range={}-{} shard={}", seq_pair.first, seq_pair.second, shard());
            return;
        }

        scheduled = part_commit_pool.trySchedule(
            [&, commit_data, metadata, this]() mutable {
                auto & [moved_block, moved_seq, moved_keys, moved_sequence_ranges] = *commit_data;

                while (!isStopped())
                {
                    try
                    {
                        auto sink = storage->write(nullptr, metadata, storage_stream.getContext());

                        auto * merge_tree_sink = static_cast<MergeTreeSink *>(sink.get());
                        merge_tree_sink->setSequenceInfo(std::make_shared<SequenceInfo>(moved_seq.first, moved_seq.second, moved_keys));
                        merge_tree_sink->setMissingSequenceRanges(std::move(moved_sequence_ranges));

                        merge_tree_sink->onStart();

                        assignIndexTime(const_cast<ColumnWithTypeAndName *>(moved_block.findByName(ProtonConsts::RESERVED_INDEX_TIME)));

                        merge_tree_sink->consume(Chunk(moved_block.getColumns(), moved_block.rows()));
                        merge_tree_sink->onFinish();
                        break;
                    }
                    catch (...)
                    {
                        LOG_ERROR(
                            logger,
                            "Failed to commit rows={} to file system, exception={}",
                            moved_block.rows(),
                            getCurrentExceptionMessage(true, true));
                        /// FIXME : specific error handling. When we sleep here, it occupied the current thread
                        sleepForMilliseconds(2000);
                    }
                }

                progressSequences(moved_seq);
            },
            /*wait_timeout_ms=*/{500});

        if (!scheduled)
            LOG_WARNING(logger, "No available threads in background commit pool with size={}, retry", part_commit_pool.getMaxThreads());
    }

    commitSN();
}

/// Merge `rhs` block to `lhs`
void StreamShardStore::mergeBlocks(Block & lhs, Block & rhs)
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

void StreamShardStore::commit(cluster::SchemaRecordPtrs records, SequenceRanges missing_sequence_ranges)
{
    if (records.empty())
        return;

    Block block;
    auto keys = std::make_shared<IdempotentKeys>();

    Int64 start_sn = -1;
    Int64 end_sn = -1;

    StorageMetadataPtr metadata;
    for (auto & rec : records)
    {
        if (isStopped())
            return;

        /// Skip duplicate record
        if (rec->hasIdempotentKey())
        {
            if (!idempotent_keys->add(rec->getSN(), rec->idempotentKey(), /*deduped_log_prefix=*/"Skipping duplicate record commit"))
                continue;

            keys->emplace_back(idempotent_keys->lastKey());
        }

        auto opcode = rec->opCode();
        if (likely(opcode == cluster::protocol::OpCode::InsertData))
        {
            assignSequenceID(rec);

            if (block && metadata->getVersion() == rec->schemaVersion()) [[likely]]
            {
                /// If the accumulated block and the incoming record differ in dynamic-subcolumn
                /// presence, commit the current block first. Without this boundary, a non-JSON
                /// block merged with a JSON record of the same schema version would silently
                /// become a thresholded JSON batch — a broader behaviour change than intended.
                const bool current_has_dynamic = block.hasDynamicSubcolumns();
                const bool incoming_has_dynamic = rec->getBlock().hasDynamicSubcolumns();
                if (current_has_dynamic != incoming_has_dynamic)
                {
                    chassert(start_sn >= 0 && end_sn >= start_sn);
                    doCommit(
                        std::move(block), std::make_pair(start_sn, end_sn), std::move(keys), missing_sequence_ranges, metadata);
                    block.clear();
                    keys = std::make_shared<IdempotentKeys>();
                    block.swap(rec->getBlock());
                    start_sn = rec->getSN();
                    end_sn = rec->getSN();
                }
                else
                {
                    /// Same dynamic-subcolumn type — safe to merge
                    mergeBlocks(block, rec->getBlock());
                    end_sn = rec->getSN();
                }
            }
            else
            {
                /// Different schema version — commit accumulated block first
                if (block)
                {
                    chassert(metadata->getVersion() != rec->schemaVersion());
                    chassert(start_sn >= 0 && end_sn >= start_sn);
                    doCommit(
                        std::move(block), std::make_pair(start_sn, end_sn), std::move(keys), std::move(missing_sequence_ranges), metadata);
                }

                /// restart first block
                block.swap(rec->getBlock());
                start_sn = rec->getSN();
                end_sn = rec->getSN();
                metadata = storage_stream.getInMemoryMetadataByVersion(rec->schemaVersion());
            }

            /// If block contains JSON column, we cannot merge blocks due to dynamic subcolumns,
            /// but we should NOT commit every single record immediately. That creates a tiny-part
            /// storm that exhausts the commit pool and causes OOM on restart (see issue #1113).
            ///
            /// Instead, we batch JSON records: commit only when accumulated rows or bytes exceed
            /// a threshold. This prevents thousands of 1-row parts from being created while still
            /// ensuring dynamic subcolumn data is committed in bounded groups.
            ///
            /// Cases we handle:
            /// 1. all json blocks: [json_batch_1], [json_batch_2], ...
            /// 2. all non-json blocks: [block], [block], ... (unchanged)
            /// 3. interleaved: [json_batch], [non_json_batch], [json_batch], ...
            if (block.hasDynamicSubcolumns())
            {
                chassert(start_sn >= 0 && rec->getSN() >= start_sn);

                const auto & global_settings = storage_stream.getContext()->getSettingsRef();
                const size_t row_threshold = global_settings.dynamic_commit_row_threshold;
                const size_t byte_threshold = global_settings.dynamic_commit_byte_threshold;

                const auto current_rows = block.rows();
                const auto current_bytes = block.bytes();

                if (current_rows >= row_threshold || current_bytes >= byte_threshold)
                {
                    LOG_DEBUG(
                        logger,
                        "Committing batched json rows={} bytes={} sn_range=[{},{}] to file system "
                        "(thresholds: rows={} bytes={})",
                        current_rows,
                        current_bytes,
                        start_sn,
                        rec->getSN(),
                        row_threshold,
                        byte_threshold);

                    doCommit(std::move(block), std::make_pair(start_sn, rec->getSN()), std::move(keys), missing_sequence_ranges, metadata);

                    /// Explicitly clear since std::move in theory can be implemented as no move
                    block.clear();
                    keys = std::make_shared<IdempotentKeys>();
                }
                else
                {
                    LOG_TRACE(
                        logger,
                        "Accumulating json batch: rows={}/{} bytes={}/{} sn={}",
                        current_rows,
                        row_threshold,
                        current_bytes,
                        byte_threshold,
                        rec->getSN());
                }
            }
        }
        else
        {
            if (block)
            {
                chassert(start_sn >= 0 && rec->getSN() >= start_sn);
                doCommit(std::move(block), std::make_pair(start_sn, end_sn), std::move(keys), std::move(missing_sequence_ranges), metadata);
            }

            /// Reset state
            block.clear();
            keys = std::make_shared<IdempotentKeys>();

            handleControlCommand(opcode, rec->getBlock(), rec->getSN());

            /// Commit the mutation sn
            start_sn = rec->getSN();
            end_sn = rec->getSN();
            std::pair seq_pair = {start_sn, end_sn};
            outstanding_sns.push_back(seq_pair);
            progressSequences(seq_pair);
        }
    }

    LOG_DEBUG(logger, "Committing records={} rows={} bytes={} to file system", records.size(), block.rows(), block.bytes());

    if (block)
    {
        chassert(start_sn >= 0 && records.back()->getSN() >= start_sn);
        doCommit(
            std::move(block),
            std::make_pair(start_sn, records.back()->getSN()),
            std::move(keys),
            std::move(missing_sequence_ranges),
            metadata);
    }
}

inline void StreamShardStore::finalCommit()
{
    commitSN();

    int64_t commit_sn = -1;
    {
        std::lock_guard lock(sns_mutex);
        if (last_sn != local_sn)
            commit_sn = last_sn;
    }

    if (commit_sn >= 0)
        commitSNLocal(commit_sn);
}

inline void StreamShardStore::periodicallyCommit()
{
    int64_t remote_commit_sn = -1;
    int64_t commit_sn = -1;
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

void StreamShardStore::consumeCallback(cluster::SchemaRecordPtrs records, cluster::klog::ConsumeCallbackData * data)
{
    auto * cdata = dynamic_cast<StreamCallbackData *>(data);

    if (records.empty())
    {
        cdata->streamShardStore()->periodicallyCommit();
        return;
    }

    cdata->commit(std::move(records));
}


void StreamShardStore::initLog()
{
    auto ssettings = storage_stream.getSettings();
    const auto & logstore = ssettings->logstore.value;

    if (logstore == ProtonConsts::LOGSTORE_NATIVE_LOG || logstore.empty() || inmemory)
        initNativeLog();
    else
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Logstore type {} is not supported", logstore);
}


void StreamShardStore::initNativeLog()
{
    // nlog_client not needed

    if (isVirtualReplica())
        return;

    LOG_INFO(logger, "Init NativeLog on file system");

    auto & native_log = Globals::getNativeLog();

    auto ssettings = storage_stream.getSettings();
    /// Get or create local log on file system
    auto log_config = native_log.getConfig().log_config->clone();
    log_config->flush_interval_entries = static_cast<int32_t>(ssettings->logstore_flush_messages.value);
    log_config->flush_interval_ms = static_cast<int32_t>(ssettings->logstore_flush_ms.value);

    if (ssettings->logstore_retention_bytes.value)
        log_config->retention_size = ssettings->logstore_retention_bytes.value;

    if (ssettings->logstore_retention_ms.value)
        log_config->retention_ms = ssettings->logstore_retention_ms.value;

    log_config->inmemory = inmemory;

    native_log.create(stream_shard, cluster::nlog::LogConfigEntry{std::move(log_config)});
}

int64_t StreamShardStore::snLoaded() const
{
    std::lock_guard lock(sns_mutex);
    if (local_sn >= 0)
        /// Sequence number committed on disk is sequence of a record
        /// `plus one` is the next sequence expecting
        return local_sn + 1;

    return storage_stream.getSettings()->logstore_auto_offset_reset.value == "earliest" ? cluster::Constants::EarliestSN
                                                                                        : cluster::Constants::LatestSN;
}

Int64 StreamShardStore::maxCommittedSN() const
{
    assert(storage);
    return storage->maxCommittedSN();
}

Int64 StreamShardStore::getOffset(const SeekToInfoPtr & seek_to_info) const
{
    chassert(seek_to_info);
    auto offsets = seek_to_info->getSavedOffsets();
    /// We need to calculate and save offsets at first time
    if (!offsets.has_value())
    {
        seek_to_info->replicateForShards(storage_stream.shards);

        std::vector<Int64> sequences;
        if (seek_to_info->isTimeBased())
        {
            sequences = sequencesForTimestamps(seek_to_info->getSeekPoints());
        }
        else
            sequences = seek_to_info->getSeekPoints();

        seek_to_info->saveOffsets(std::move(sequences));
        offsets = seek_to_info->getSavedOffsets();
    }

    return offsets->at(shard()); /// assume shard() starts from 0
}

std::vector<Int64> StreamShardStore::sequencesForTimestamps(std::vector<Int64> timestamps, bool append_time) const
{
    assert(timestamps.size() == storage_stream.shards);

    std::vector<cluster::ShardTimestamp> shard_timestamps;
    shard_timestamps.reserve(storage_stream.shards);
    for (UInt32 i = 0; i < storage_stream.shards; ++i)
        shard_timestamps.emplace_back(i, timestamps[i]);

    // using getNativeLog directly
    auto & native_log = Globals::getNativeLog();

    cluster::Stream stream{stream_shard.ns, stream_shard.name, stream_shard.id()};
    auto shard_sequences{native_log.translateTimestamps(stream, shard_timestamps, append_time)};

    std::vector<Int64> sequences;
    sequences.reserve(storage_stream.shards);
    for (const auto & shard_sequence : shard_sequences)
        sequences.push_back(shard_sequence.sn);

    return sequences;
}

String StreamShardStore::logStoreClusterId() const
{
    return storage_stream.getSettings()->logstore_cluster_id.value;
}

int64_t StreamShardStore::lastSN() const
{
    std::lock_guard lock(sns_mutex);
    return last_sn;
}

bool StreamShardStore::isLocal() const noexcept
{
    return storage_stream.isLocal();
}

UInt64 StreamShardStore::getLogStoreDiskSize() const
{
    if (!isNativeLog() || isVirtualReplica())
        return 0;

    const auto & native_log = Globals::getNativeLog();
    auto log_stat = native_log.stat(stream_shard.id_shard);
    if (log_stat.hasError() || !log_stat.result.has_value())
        return 0;

    return log_stat.result->size;
}

UInt64 StreamShardStore::getStorageSize() const
{
    return getLogStoreDiskSize() + (storage ? storage->getStorageSize() : 0);
}

UInt64 StreamShardStore::maxEntrySize() const
{
    if (!isNativeLog())
        return 0;
    const auto & native_log = Globals::getNativeLog();
    return native_log.maxEntrySize(stream_shard.id_shard);
}

std::optional<Int64> StreamShardStore::committedSequence() const
{
    auto maybe_log_committed_sn = Globals::getNativeLog().committedSequence(streamIDShard());
    if (!maybe_log_committed_sn)
        return {};

    if (storage)
    {
        /// Historical store may have more recent sn, see committedSequenceForSnapshot
        auto historical_committed_sn = storage->inMemoryCommittedSN();
        return std::max(*maybe_log_committed_sn, historical_committed_sn);
    }
    else
    {
        return *maybe_log_committed_sn;
    }
}

std::optional<Int64> StreamShardStore::appliedSequence() const
{
    if (storage)
        return storage->inMemoryCommittedSN();
    return {};
}

std::pair<Int64, Int64> StreamShardStore::sequenceRange() const
{
    auto maybe_log_committed_sn = Globals::getNativeLog().committedSequence(streamIDShard());
    auto maybe_log_start_sn = Globals::getNativeLog().logStartSequence(streamIDShard());

    return {maybe_log_start_sn.value_or(-1), maybe_log_committed_sn.value_or(-1)};
}

}
