#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/Stream/StorageStream.h>

#include <Bootstrap/Globals.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Cluster/Common/Constants.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/Executors/ExecutingGraph.h>
#include <Storages/AlterCommands.h>
#include <Storages/SelectQueryDescription.h>
#include <Storages/StorageFactory.h>
#include <Common/ProtonCommon.h>
#include <Common/Stopwatch.h>
#include <Common/checkStackSize.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_QUERY;
extern const int NOT_IMPLEMENTED;
extern const int RESOURCE_NOT_INITED;
extern const int INVALID_SHARD_ID;
extern const int UNKNOWN_STREAM;
extern const int UNSUPPORTED;
extern const int MATERIALIZED_VIEW_STOPPED;
}

namespace
{
/// Note, we have other places like DiskCheckpointStorage etc which depend on
/// this query id format : the query id starts with ".mv-"
String generateInnerQueryID(const StorageID & view_id)
{
    if (view_id.hasUUID())
        return fmt::format(".mv-{}", toString(view_id.uuid));

    return fmt::format(".mv-{}", view_id.getTableName());
}
}

StorageMaterializedView::StorageMaterializedView(
    std::unique_ptr<StreamSettings> storage_settings,
    const ASTPtr & sharding_expr_,
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach_,
    ContextMutablePtr context_,
    bool has_force_restore_data_flag_)
    : IStorage(table_id_)
    , WithMutableContext(context_)
    , shards(static_cast<uint32_t>(storage_settings->shards.value))
    , this_node(context_->getNodeID())
    , inner_query_id(generateInnerQueryID(table_id_))
    , logger(getLogger(table_id_.getFullTableName()))
    , pipeline_state(*this, logger)
    , ckpt_log_stream_shard(
          table_id_.database_name, table_id_.table_name, table_id_.uuid, cluster::Constants::InternalCheckpointShardOffset + shards)
    , recover_backoff_ms(
          /*initial_interval_=*/5'000,
          /*multiplier_=*/2,
          /*max_interval_=*/90'000,
          /*jitter_=*/0.3)
{
    validate(table_id_, metadata_, query, attach_);

    setInMemoryMetadata(metadata_);

    /// Save the init params
    init_params = std::make_unique<InitParams>();
    init_params->storage_settings = std::move(storage_settings);
    init_params->attach = attach_;
    init_params->has_force_restore_data_flag = has_force_restore_data_flag_;
    init_params->sharding_expr = sharding_expr_;
    init_params->relative_data_path = relative_data_path_;

    inner_query_id = generateInnerQueryID(table_id_);
}

void StorageMaterializedView::validate(
    const StorageID & table_id_, const StorageInMemoryMetadata & metadata_, const ASTCreateQuery & query, bool attach_)
{
    if (!attach_)
        validateInnerQuery(metadata_);

    bool external_stream_provided = !query.to_table_id.empty();
    bool point_to_itself_by_uuid
        = !external_stream_provided && query.to_inner_uuid != UUIDHelpers::Nil && query.to_inner_uuid == table_id_.uuid;
    bool point_to_itself_by_name = external_stream_provided && query.to_table_id.database_name == table_id_.database_name
        && query.to_table_id.table_name == table_id_.table_name;
    if (point_to_itself_by_uuid || point_to_itself_by_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Materialized view {} cannot point to itself", table_id_.getFullTableName());

    if (external_stream_provided)
    {
        target_table_id = query.to_table_id;

        if (!attach_)
        {
            auto target_table = tryGetTargetTable();
            if (!target_table)
                throw Exception(ErrorCodes::UNKNOWN_STREAM, "Target stream is not found {}", target_table_id->getFullTableName());
        }
    }
}

void StorageMaterializedView::validateInnerQuery(const StorageInMemoryMetadata & storage_metadata) const
{
    /// Validate if the inner query is a streaming query. Only streaming query is supported for now
    auto select_context = Context::createCopy(getContext());
    select_context->makeQueryContext();
    select_context->setCurrentQueryId(""); /// generate random query_id
    auto description = storage_metadata.getSelectQuery();

    InterpreterSelectWithUnionQuery select_interpreter(description.inner_query, select_context, SelectQueryOptions().analyze());
    if (!select_interpreter.isStreamingQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Materialized view doesn't support historical select query");

    if (std::ranges::find(description.select_table_ids, getStorageID()) != description.select_table_ids.end())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "{} {} cannot select from itself", getName(), getStorageID().getFullTableName());
}

StorageMaterializedView::~StorageMaterializedView()
{
    try
    {
        shutdown(/*dropping=*/false);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageMaterializedView::initInnerStream()
{
    if (target_table_id)
        /// When it has a target stream (INTO target_stream), skip creating the inner stream
        return;

    /// There is no external target stream is provided, create an inner stream
    /// with UUID in the query. The inner stream is not in the catalog but the MV.
    StorageStream::MergingParams merging_params;
    merging_params.mode = StorageStream::MergingParams::Mode::Ordinary;

    inner_storage = StorageStream::create(
        shards,
        init_params->sharding_expr,
        getStorageID(),
        init_params->relative_data_path,
        getInMemoryMetadata(),
        init_params->attach,
        getContext(),
        /*date_column_name=*/"",
        merging_params,
        std::move(init_params->storage_settings),
        init_params->has_force_restore_data_flag);

    inner_storage->startup();
}

void StorageMaterializedView::startup()
{
    if (started.test_and_set())
        return;

    if (!isVirtualStorage())
        initInnerStream();

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage_id = getStorageID();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().addDependency(select_table_id, storage_id);

    if (target_table_id)
        DatabaseCatalog::instance().addDependency(*target_table_id, storage_id);

    if (init_params->attach)
    {
        /// Attaching
        /// Skip init background states and pipeline if it is virtual storage for attach since
        /// it was validated when creating.
        if (isVirtualStorage())
            return;
    }
    else
    {
        if (isVirtualStorage())
            return;
    }

    initPipeline(/*recovering=*/init_params->attach);

    /// NOTE: If it's a Create request, we should wait for done of startup background pipeline,
    /// so that, we can report the error if has exception (e.g. bad query)
    if (!init_params->attach)
    {
        Stopwatch stopwatch;
        while (!pipeline_state.validated.load())
        {
            if (pipeline_state.isCancelled())
                break;

            pipeline_state.validated.wait(false);
        }

        pipeline_state.checkException();

        LOG_INFO(
            logger,
            "Took {}ms to wait for built background pipeline during Materialized View '{}' startup",
            stopwatch.elapsedMilliseconds(),
            getStorageID().getFullTableName());
    }
}

void StorageMaterializedView::shutdown(bool dropping)
{
    if (stopped.test_and_set() || !started.test())
        return;

    LOG_INFO(logger, "Stopping");

    dropping = is_dropped || dropping;

    shutdownPipeline();

    if (inner_storage)
        inner_storage->shutdown(dropping);

    /// FIXME: add dependencies in MetaStore ?
    /// Temporary solution, create a materialized view in non-instance mode:
    /// 1) In create request thread, the temporary MV created on the initial node is destructed (startup is not called), the dependencies will be deleted.
    /// 2) In MetadataUpdater thread, we will create a MV and call startup which will add dependencies
    /// When If thread 2) add the dependency first, and then thread 1) delete the dependency, it will cause the dependency to be lost on the initial node.
    auto storage_id = getStorageID();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().removeDependency(select_table_id, storage_id);

    if (target_table_id)
        DatabaseCatalog::instance().removeDependency(*target_table_id, storage_id);

    LOG_INFO(logger, "Stopped");
}

QueryProcessingStage::Enum StorageMaterializedView::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    if (!target_table_id)
        return IStorage::getQueryProcessingStage(local_context, to_stage, storage_snapshot, query_info);
    else
        return getTargetTable()->getQueryProcessingStage(local_context, to_stage, storage_snapshot, query_info);
}

void StorageMaterializedView::drop()
{
    /// 1) Shutdown background resources such as background thread, inner stream etc
    shutdown(/*dropping=*/true);

    /// 2) Remove inner stream
    if (inner_storage)
        inner_storage->drop();

    /// 3) Remove checkpoint
    if (curr_ckpt_ctx)
        Globals::getCheckpointCoordinator().removeCheckpoint(curr_ckpt_ctx);

    /// 4) Remove checkpoint log if exists
    if (logstore_ckpt_ctx)
        Globals::getNativeLog().remove({logstore_ckpt_ctx->log_stream_shard});
}

void StorageMaterializedView::alter(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder)
{
    chassert(!commands.empty());

    /// Checked in `checkAlterIsPossible`, either `ALTER query [settings]` nor `ALTER storage` is allowed
    /// step-1: Alter the settings of inner query and sync with the metadata of mv
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    commands.apply(new_metadata, context_);

    auto table_id = getStorageID();
    DatabaseCatalog::instance()
        .getDatabase(table_id.database_name)
        ->alterTable(context_, table_id, new_metadata, commands.front().typeString(), commands.astCommands(table_id));
}

void StorageMaterializedView::applyAlterCommandsToAllShards(
    const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder)
{
    if (std::ranges::all_of(commands, [](const auto & command) {
            return command.type != AlterCommand::MODIFY_QUERY && command.type != AlterCommand::MODIFY_QUERY_SETTING
                && command.type != AlterCommand::RESET_QUERY_SETTING;
        }))
    {
        /// When we reach here, it is applied in MetadataUpdater and inner storage metadata update shall be treated like local
        if (inner_storage)
            inner_storage->applyAlterCommandsToAllShards(commands, context_, alter_lock_holder);
    }
}

void StorageMaterializedView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr ctx) const
{
    if (commands.size() != 1)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Multi alter commands are not supported for {} storage engine. Alter commands={}",
            getName(),
            fmt::join(commands.stringCommands(), ", "));

    const auto & alter_command = commands[0];
    switch (alter_command.type)
    {
        case AlterCommand::MODIFY_QUERY:
        {
            validateModifiedQuery(alter_command.select);
            return;
        }
        case AlterCommand::MODIFY_QUERY_SETTING:
        {
            std::set<String> allowed_settings{
                "checkpoint_interval", "pause_on_start", "enable_dlq", "dlq_max_message_batch_size", "dlq_consecutive_failures_limit"};
            for (const auto & [setting_name, _] : alter_command.settings_changes)
            {
                if (!allowed_settings.contains(setting_name))
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Modify setting {} is not supported, only {} settings can be modified",
                        setting_name,
                        fmt::join(allowed_settings, ", "));
            }
            return;
        }
        case AlterCommand::RESET_QUERY_SETTING:
        {
            for (const auto & setting_name : alter_command.settings_resets)
            {
                if (setting_name != "checkpoint_interval" && setting_name != "pause_on_start")
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reset setting {} is not supported", setting_name);
            }
            return;
        }
        case AlterCommand::MODIFY_SETTING:
        case AlterCommand::RESET_SETTING:
        case AlterCommand::MODIFY_TTL:
        case AlterCommand::REMOVE_TTL:
        {
            if (target_table_id)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Materialized view with provided target stream can't be altered its settings or TTL. Update the settings or TTL on "
                    "target stream directly");

            chassert(inner_storage);
            inner_storage->checkAlterIsPossible(commands, ctx);
            return;
        }
        case AlterCommand::COMMENT_TABLE:
        {
            return;
        }
        default:
        {
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "Only Materialized View's settings, table TTL, query settings and comment can be altered.");
        }
    }
}

void StorageMaterializedView::onAlterQuery()
{
    if (pipeline_state.pipelineStopped())
        return;

    getContext()->getAdhocSchedulePool().scheduleOrThrowOnError([mv = shared_from_this(), this] {
        setThreadName("MVAlterQuery");

        LOG_INFO(logger, "Applying new query");
        while (true)
        {
            auto err = flushAndStop(PipelineState::Status::Initializing, /*timeout_ms=*/10'000);
            if (err == ErrorCodes::OK)
                break;

            if (err == ErrorCodes::MATERIALIZED_VIEW_STOPPED || pipeline_state.pipelineStopped())
            {
                LOG_INFO(logger, "Skipped applying new query since the pipeline has been stopped");
                return;
            }
        }
        LOG_INFO(logger, "Applied new query");
    });
}

void StorageMaterializedView::onAlterQuerySettings()
{
    if (pipeline_state.isCancelled())
        return;

    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto & settings_changes = metadata_snapshot->getSelectQuery().settings_changes;

    /// Handle checkpoint interval changes
    Field interval_field{Int64(0)};
    if (settings_changes.tryGet("checkpoint_interval", interval_field))
    {
        Int64 interval = SettingFieldInt64(interval_field).value;

        auto & ckpt_coordinator = Globals::getCheckpointCoordinator();
        ckpt_coordinator.updateCheckpointInterval(getInnerQueryId(), interval);
        if (interval < 0)
        {
            pipeline_state.disable_checkpoint.store(true);
        }
    }

    /// Handle dlq changes
    Field enable_dlq_field{false};
    if (settings_changes.tryGet("enable_dlq", enable_dlq_field))
    {
        bool enable_dlq = SettingFieldBool(enable_dlq_field).value;
        if (pipeline_state.enable_dlq.exchange(enable_dlq) != enable_dlq)
        {
            if (enable_dlq)
                LOG_INFO(logger, "Enabled DLQ");
            else
                LOG_INFO(logger, "Disabled DLQ");
        }
    }

    Field dlq_max_message_batch_size{0};
    if (settings_changes.tryGet("dlq_max_message_batch_size", dlq_max_message_batch_size))
    {
        auto new_size = SettingFieldUInt64(dlq_max_message_batch_size).value;
        if (auto old_size = pipeline_state.dlq_max_message_batch_size.exchange(new_size); old_size != new_size)
            LOG_INFO(logger, "Updated dlq_max_message_batch_size from {} to {}", old_size, new_size);
    }

    Field dlq_consecutive_failures_limit{0};
    if (settings_changes.tryGet("dlq_consecutive_failures_limit", dlq_consecutive_failures_limit))
    {
        auto new_limit = SettingFieldUInt64(dlq_consecutive_failures_limit).value;
        if (auto old_limit = pipeline_state.dlq_max_message_batch_size.exchange(new_limit); old_limit != new_limit)
            LOG_INFO(logger, "Updated dlq_consecutive_failures_limit from {} to {}", old_limit, new_limit);
    }
}

void StorageMaterializedView::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    IStorage::renameInMemory(new_table_id);
    if (inner_storage)
        inner_storage->renameInMemory(new_table_id);

    const auto & select_query = metadata_snapshot->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().updateDependency(select_table_id, old_table_id, select_table_id, new_table_id);

    if (target_table_id)
        DatabaseCatalog::instance().addDependency(*target_table_id, new_table_id);
}

StoragePtr StorageMaterializedView::getTargetTable() const
{
    checkStackSize();

    if (target_table_id)
        return DatabaseCatalog::instance().getTable(*target_table_id, getContext());

    if (!inner_storage)
        throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "The inner storage of '{}' is not initialized", getStorageID().getFullTableName());

    return inner_storage;
}

StoragePtr StorageMaterializedView::tryGetTargetTable() const
{
    checkStackSize();

    if (target_table_id)
        return DatabaseCatalog::instance().tryGetTable(*target_table_id, getContext());

    return inner_storage;
}

StorageMetadataPtr StorageMaterializedView::getTargetInMemoryMetadataPtr() const
{
    if (auto target = tryGetTargetTable())
        return target->getInMemoryMetadataPtr();

    /// Virtual inner storage, its metadata is the same as MV metadata
    return getInMemoryMetadataPtr();
}

bool StorageMaterializedView::isReady() const
{
    /// Allow to query only when the target table is ready
    if (auto target_table = tryGetTargetTable())
        return target_table->isReady();

    return false; /// The inner storage is not initialized
}

NamesAndTypesList StorageMaterializedView::getVirtuals() const
{
    if (auto target = tryGetTargetTable())
        return target->getVirtuals();

    /// Virtual inner storage
    return StorageStream::getVirtualsOfAppendOnly();
}

StorageSnapshotPtr StorageMaterializedView::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    if (auto target = tryGetTargetTable())
        return target->getStorageSnapshot(metadata_snapshot, query_context);

    return IStorage::getStorageSnapshot(metadata_snapshot, query_context);
}

std::optional<SettingsChanges> StorageMaterializedView::getTargetStreamSettingsChanges() const
{
    if (auto target = tryGetTargetTable())
    {
        if (const auto * stream = target->as<StorageStream>())
            return stream->getSettings()->changes();
    }
    return std::nullopt;
}

Streaming::DataStreamSemanticEx StorageMaterializedView::dataStreamSemantic() const
{
    if (auto target = tryGetTargetTable())
        return target->dataStreamSemantic();

    /// Virtual inner storage
    return IStorage::dataStreamSemantic();
}

std::optional<UInt64> StorageMaterializedView::totalRows(const Settings & settings) const
{
    if (auto storage = tryGetTargetTable(); storage)
        return storage->totalRows(settings);

    return std::nullopt;
}

std::optional<String> StorageMaterializedView::preferredColumn() const
{
    if (auto storage = tryGetTargetTable(); storage)
        return storage->preferredColumn();

    return std::nullopt;
}

UInt64 StorageMaterializedView::getLogStoreDiskSize() const
{
    if (auto * storage_stream = dynamic_cast<StorageStream *>(inner_storage.get()))
        return storage_stream->getLogStoreDiskSize();
    return 0;
}

std::shared_ptr<StorageMaterializedView> StorageMaterializedView::tryGetStorage(const StorageID & table_id, const ContextPtr & context_)
{
    auto table = DatabaseCatalog::instance().tryGetTable(table_id, context_);
    if (!table)
        return nullptr;

    auto mv = std::dynamic_pointer_cast<DB::StorageMaterializedView>(table);
    if (!mv)
        return nullptr;

    return mv;
}

UInt32 StorageMaterializedView::getShardIDFromCheckpointShardID(UInt32 shard) const
{
    if (!isCheckpointShardID(shard))
        throw Exception(ErrorCodes::INVALID_SHARD_ID, "Invalid checkpoint shard '{}' for {}", shard, getStorageID().getFullTableName());

    return shard - (cluster::Constants::InternalCheckpointShardOffset + shards);
}

bool StorageMaterializedView::isCheckpointShardID(UInt32 shard) const noexcept
{
    return ckpt_log_stream_shard.shard() == shard;
}

UInt64 StorageMaterializedView::getStorageSize() const
{
    return inner_storage ? inner_storage->getStorageSize() : 0;
}

Strings StorageMaterializedView::getDataPaths() const
{
    return inner_storage ? inner_storage->getDataPaths() : Strings{};
}


bool StorageMaterializedView::isVirtualStorage() const
{
    return false;
}

void registerStorageMaterializedView(StorageFactory & factory)
{
    factory.registerStorage("MaterializedView", [](const StorageFactory::Arguments & args) {
        if (!args.query.select)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for Materialized View {}", args.table_id.getFullTableName());

        auto context = args.getContext();

        std::optional<KeyDescription> order_by_key;
        std::optional<KeyDescription> partition_by_key;

        ASTPtr sharding_expr;

        if (args.query.to_table_id.empty())
        {
            sharding_expr = makeASTFunction("rand");

            /// Sorting key
            auto order_by = makeASTFunction("to_start_of_hour", std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME));
            order_by_key = KeyDescription::getKeyFromAST(order_by, args.columns, context);

            /// Partition key
            auto partition_by = makeASTFunction("to_YYYYMM", std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME));
            partition_by_key = KeyDescription::getKeyFromAST(partition_by, args.columns, context);
        }

        auto select = SelectQueryDescription::getSelectQueryFromASTForView(args.query.select->ptr(), args.getLocalContext());

        ASTStorage storage_def;
        auto storage_settings = std::make_unique<StreamSettings>();

        if (args.query.storage_settings)
            storage_def.set(storage_def.settings, args.query.storage_settings->ptr());

        storage_settings->loadFromQuery(storage_def, args.getContext());

        if (!storage_settings->shards)
            storage_settings->shards = 1;


        StorageInMemoryMetadata storage_metadata;
        storage_metadata.settings_changes = storage_def.settings->ptr();
        storage_metadata.setColumns(args.columns);
        storage_metadata.setComment(args.comment);
        storage_metadata.setSelectQuery(select);

        if (order_by_key)
        {
            storage_metadata.sorting_key = *order_by_key;
            storage_metadata.primary_key = *order_by_key;
            storage_metadata.primary_key.definition_ast = nullptr;
            storage_metadata.partition_key = std::move(*partition_by_key);
        }

        if (args.query.mv_inner_storage_ttl)
        {
            storage_metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                args.query.mv_inner_storage_ttl, storage_metadata.columns, context, storage_metadata.primary_key);
        }

        auto column_ttl_asts = args.columns.getColumnTTLs();
        for (const auto & [name, ast] : column_ttl_asts)
        {
            auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, args.columns, context, storage_metadata.primary_key);
            storage_metadata.column_ttls_by_name[name] = new_ttl_entry;
        }

        storage_metadata.setVersion(args.schema_version);

        return StorageMaterializedView::create(
            std::move(storage_settings),
            sharding_expr,
            args.table_id,
            args.query,
            args.relative_data_path,
            storage_metadata,
            args.attach,
            context,
            args.has_force_restore_data_flag);
    });
}

void StorageMaterializedView::validateModifiedQuery(ASTPtr new_query) const
{
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId(""); /// generate random query_id
    syncInnerQueryContext(query_context);

    String new_stateful_node_descs;
    {
        bool can_recover_without_checkpoint = false;
        auto new_pipeline = buildQueryPipelineImpl(new_query, query_context, &can_recover_without_checkpoint);

        /// If the new query pipeline can recover without using checkpoints,
        /// the modification is deemed acceptable and the old pipeline compatibility check is skipped.
        if (can_recover_without_checkpoint)
            return;

        new_stateful_node_descs = formatNodeDescriptions(ExecutingGraph::statefulNodeDescriptions(new_pipeline.getProcessors()));
    }

    String old_stateful_nodes_descs;
    if (auto io = pipeline_state.io.load(); io)
    {
        old_stateful_nodes_descs = formatNodeDescriptions(ExecutingGraph::statefulNodeDescriptions(io->pipeline.getProcessors()));
    }
    else
    {
        auto old_pipeline = buildQueryPipelineImpl(getInMemoryMetadataPtr()->getSelectQuery().inner_query, query_context);
        old_stateful_nodes_descs = formatNodeDescriptions(ExecutingGraph::statefulNodeDescriptions(old_pipeline.getProcessors()));
    }

    if (new_stateful_node_descs != old_stateful_nodes_descs)
    {
        LOG_ERROR(
            logger,
            "The new query of the Materialized View changed the stateful processors which is incompatible with existing query. New "
            "stateful processors: {{{}}}; Old stateful processors: {{{}}}",
            new_stateful_node_descs,
            old_stateful_nodes_descs);

        throw Exception(
            ErrorCodes::UNSUPPORTED,
            "The new query of the Materialized View changed the stateful processors which is incompatible with existing query");
    }
}

void StorageMaterializedView::handleDLQSettings()
{
    const auto & settings = pipeline_state.query_context->getSettingsRef();
    pipeline_state.enable_dlq.store(settings.enable_dlq.value);
    pipeline_state.dlq_max_message_batch_size.store(settings.dlq_max_message_batch_size.value);
    pipeline_state.dlq_consecutive_failures_limit.store(settings.dlq_consecutive_failures_limit.value);
}

bool StorageMaterializedView::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    ContextPtr query_context)
{
    if (target_table_id)
    {
        /// When it has a target stream (INTO target_stream), skip optimizing the Materialized View
        throw Exception(ErrorCodes::UNSUPPORTED, "Invoke optimize command on target stream / table of the Materialized View");
    }

    if (inner_storage)
    {
        return inner_storage->optimize(
            query, inner_storage->getInMemoryMetadataPtr(), partition, final, deduplicate, deduplicate_by_columns, query_context);
    }

    return true;
}
}
