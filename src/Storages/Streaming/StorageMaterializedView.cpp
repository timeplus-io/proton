#include <Storages/Streaming/StorageMaterializedView.h>
#include <Storages/Streaming/StorageStream.h>

#include <IO/WriteBufferFromString.h>
#include <Interpreters/DiskUtilChecker.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/AlterCommands.h>
#include <Storages/SelectQueryDescription.h>
#include <Storages/StorageFactory.h>
#include <Common/ProtonCommon.h>
#include <Common/checkStackSize.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_QUERY;
extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
extern const int RESOURCE_NOT_INITED;
}

namespace
{
String generateInnerTableName(const StorageID & view_id)
{
    if (view_id.hasUUID())
        return ".inner.target-id." + toString(view_id.uuid);
    return ".inner.target." + view_id.getTableName();
}
}

class CheckMaterializedViewValidTransform final : public ISimpleTransform
{
public:
    CheckMaterializedViewValidTransform(const Block & header_, const StorageMaterializedView & view_)
        : ISimpleTransform(header_, header_, false, ProcessorID::CheckMaterializedViewValidTransformID), view(view_)
    {
    }

    String getName() const override { return "CheckMaterializedViewValidTransform"; }

protected:
    void transform(Chunk &) override { view.checkValid(); }

private:
    const StorageMaterializedView & view;
};

StorageMaterializedView::StorageMaterializedView(
    const StorageID & table_id_,
    ContextPtr local_context,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_,
    bool is_virtual_)
    : IStorage(table_id_)
    , WithMutableContext(local_context->getGlobalContext())
    , log(&Poco::Logger::get(fmt::format("StorageMaterializedView ({})", table_id_.getFullTableName())))
    , is_attach(attach_)
    , is_virtual(is_virtual_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());

    /// If `INTO [target stream]` is not specified, use inner stream
    has_inner_table = query.to_table_id.empty();

    auto select = SelectQueryDescription::getSelectQueryFromASTForView(query.select->clone(), local_context);

    storage_metadata.setSelectQuery(select);
    setInMemoryMetadata(storage_metadata);

    if (!attach_)
        validateInnerQuery(storage_metadata, local_context);

    bool point_to_itself_by_uuid = has_inner_table && query.to_inner_uuid != UUIDHelpers::Nil && query.to_inner_uuid == table_id_.uuid;
    bool point_to_itself_by_name = !has_inner_table && query.to_table_id.database_name == table_id_.database_name
        && query.to_table_id.table_name == table_id_.table_name;
    if (point_to_itself_by_uuid || point_to_itself_by_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Materialized view {} cannot point to itself", table_id_.getFullTableName());

    if (!has_inner_table)
    {
        target_table_id = query.to_table_id;

        if (!attach_)
        {
            auto target_table = tryGetTargetTable();
            if (!target_table)
                throw Exception(ErrorCodes::INCORRECT_QUERY, "Target stream is not found", target_table_id.getFullTableName());

            auto * stream = target_table->as<StorageStream>();
            if (stream == nullptr)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED, "MaterializedView doesn't support target storage is {}", target_table->getName());
        }
    }
    else if (attach_)
    {
        /// If this is an ATTACH request, then the internal stream must already be created
        target_table_id = StorageID(getStorageID().database_name, generateInnerTableName(getStorageID()), query.to_inner_uuid);
    }
    else
    {
        target_table_id = StorageID(getStorageID().database_name, generateInnerTableName(getStorageID()), query.to_inner_uuid);

        /// In virtual scenarios, inner stream is supposed to already exist
        if (is_virtual)
            return;

        /// We will create a query to create an internal stream
        createInnerTable();
    }
}

void StorageMaterializedView::validateInnerQuery(const StorageInMemoryMetadata & storage_metadata, const ContextPtr & local_context) const
{
    /// Validate if the inner query is a streaming query. Only streaming query is supported for now
    auto select_context = Context::createCopy(local_context);
    select_context->makeQueryContext();
    select_context->setCurrentQueryId(""); /// generate random query_id

    InterpreterSelectWithUnionQuery select_interpreter(storage_metadata.getSelectQuery().inner_query, select_context, SelectQueryOptions());
    if (!select_interpreter.isStreaming())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Materialized view doesn't support historical select query");
}

StorageMaterializedView::~StorageMaterializedView()
{
    shutdown();
}

///                             /- (inner_target_table)
/// InMemoryTable + TargetTable
///                             \- (into_target_table)
void StorageMaterializedView::startup()
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage_id = getStorageID();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().addDependency(select_table_id, storage_id);

    DatabaseCatalog::instance().addDependency(target_table_id, storage_id);

    /// Sync target table settings
    updateStorageSettings();

    if (!is_virtual)
        executeSelectPipeline();
}

void StorageMaterializedView::shutdown()
{
    if (shutdown_called.test_and_set())
        return;

    cancelBackgroundPipeline();

    auto storage_id = getStorageID();
    const auto & select_query = getInMemoryMetadataPtr()->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().removeDependency(select_table_id, storage_id);

    DatabaseCatalog::instance().removeDependency(target_table_id, storage_id);
}

void StorageMaterializedView::waitForDependencies() const
{
    std::list<StoragePtr> waiting_storages;

    /// Check target stream
    if (auto target = getTargetTable(); !target->isReady())
        waiting_storages.emplace_back(target);

    /// Check source storages
    auto context = getContext();
    const auto & select_query = getInMemoryMetadataPtr()->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
    {
        auto source_storage = DatabaseCatalog::instance().getTable(select_table_id, context);
        if (!source_storage->isReady())
            waiting_storages.emplace_back(source_storage);
    }

    /// Wait to ready
    auto check_ready = [&waiting_storages]() {
        for (auto iter = waiting_storages.begin(); iter != waiting_storages.end();)
        {
            if ((*iter)->isReady())
                iter = waiting_storages.erase(iter);
            else
                ++iter;
        }
        return waiting_storages.empty();
    };

    auto waiting_storages_names_v
        = waiting_storages | std::views::transform([](const auto & storage) { return storage->getStorageID().getNameForLogs(); });

    auto times = 100; /// Timeout 10s
    while (!check_ready())
    {
        if (times-- == 0)
            throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "Timeout, wait for '{}' started", fmt::join(waiting_storages_names_v, ", "));

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        LOG_INFO(log, "Wait for '{}' started", fmt::join(waiting_storages_names_v, ", "));
    }
}

void StorageMaterializedView::executeSelectPipeline()
{
    if (is_virtual)
        return;

    try
    {
        /// During server bootstrap, we shall startup all tables in parallel, so here
        /// needs validity check underlying tables.
        waitForDependencies();

        /// Build inner background query pipeline and keep it alive during the lifetime of Proton
        buildBackgroundPipeline();

        /// Run background pipeline
        executeBackgroundPipeline();
        LOG_INFO(log, "Started background select query pipeline", getStorageID().getFullTableName());
    }
    catch (...)
    {
        background_status.exception = std::current_exception();
        background_status.has_exception = true;

        LOG_ERROR(log, "Failed to start: {}", getExceptionMessage(background_status.exception, false));

        /// Exception safety: failed "startup" does not require a call to "shutdown" from the caller.
        /// And it should be able to safely destroy table after exception in "startup" method.
        /// It means that failed "startup" must not create any background tasks that we will have to wait.
        cancelBackgroundPipeline();

        /// Note: after failed "startup", the stream will be in a state that only allows to destroy the object.
        /// If is an Attach request, we didn't throw exception to avoid the system fail to setup.
        if (!is_attach)
            throw;
    }
}

void StorageMaterializedView::cancelBackgroundPipeline()
{
    if (background_executor)
    {
        background_executor->cancel();
        if (background_thread.joinable())
            background_thread.join();

        background_executor.reset();
        background_pipeline.reset();
    }
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
    /// In some cases, the view background thread has exception, we check it before users access this view
    checkValid();

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
        if (query_info.syntax_analyzer_result->streaming)
        {
            // This check is so weird, disable it for now
            /// Add valid check of the view
            /// If not check, when the view go bad, the streaming query of the target stream will be blocked indefinitely
            /// since there is no ingestion on background.
            /// pipe.addTransform(std::make_shared<CheckMaterializedViewValidTransform>(header, *this));
            /// query_plan.addStep(std::move(read_step));
        }

        query_plan.addStorageHolder(storage);
        query_plan.addTableLock(std::move(lock));
    }
}

void StorageMaterializedView::drop()
{
    dropInnerTableIfAny(true, getContext());
}

void StorageMaterializedView::dropInnerTableIfAny(bool no_delay, ContextPtr local_context)
{
    if (has_inner_table && target_table_id)
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, target_table_id, no_delay);
}

void StorageMaterializedView::alter(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & alter_lock_holder)
{
    getTargetTable()->alter(commands, context_, alter_lock_holder);
    updateStorageSettings();
}

void StorageMaterializedView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr ctx) const
{
    if (!has_inner_table)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Materialized view with specified target stream can't be altered.");

    auto metadata = getInMemoryMetadataPtr();
    if (!std::all_of(
            commands.begin(), commands.end(), [&](const AlterCommand & c) { return c.isSettingsAlter() || c.isTTLAlter(*metadata); }))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only materialized view's settings/ttl can be altered.");

    getTargetTable()->checkAlterIsPossible(commands, ctx);
}

void StorageMaterializedView::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    IStorage::renameInMemory(new_table_id);

    const auto & select_query = metadata_snapshot->getSelectQuery();
    // TODO Actually we don't need to update dependency if MV has UUID, but then db and table name will be outdated
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().updateDependency(select_table_id, old_table_id, select_table_id, getStorageID());
}

StoragePtr StorageMaterializedView::getTargetTable() const
{
    checkStackSize();

    return DatabaseCatalog::instance().getTable(target_table_id, getContext());
}

StoragePtr StorageMaterializedView::tryGetTargetTable() const
{
    checkStackSize();

    return DatabaseCatalog::instance().tryGetTable(target_table_id, getContext());
}

void StorageMaterializedView::checkValid() const
{
    /// check disk quota
    DiskUtilChecker::instance(getContext()).check();

    if (background_status.has_exception)
        throw Exception(
            getExceptionErrorCode(background_status.exception),
            "Bad MaterializedView, please drop it or try recovery by restart server. background exception: {}",
            getExceptionMessage(background_status.exception, false));

    if (!background_thread.joinable())
        throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "Background resources are initializing");
}

bool StorageMaterializedView::isReady() const
{
    if (!background_thread.joinable())
    {
        if (background_status.has_exception)
            return true; /// it's ready but has exception.

        return false;
    }
    return true;
}

NamesAndTypesList StorageMaterializedView::getVirtuals() const
{
    return getTargetTable()->getVirtuals();
}

void StorageMaterializedView::createInnerTable()
{
    assert(!is_attach && !is_virtual && has_inner_table);

    /// Init inner memory table and inner target table
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto local_context = Context::createCopy(getContext());
    local_context->makeQueryContext();
    local_context->setCurrentQueryId(""); // generate random query_id

    int max_retries = 10;
    while (max_retries-- > 0)
    {
        try
        {
            doCreateInnerTable(metadata_snapshot, local_context);
            return;
        }
        catch (...)
        {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            LOG_DEBUG(log, "'{}' waiting for inner stream ready", getStorageID().getFullTableName());
        }
    }

    if (max_retries <= 0)
    {
        LOG_ERROR(log, "Failed to create inner stream for Materialized view '{}'", getStorageID().getFullTableName());
        throw Exception(
            ErrorCodes::RESOURCE_NOT_INITED, "Failed to create inner stream for Materialized view '{}'", getStorageID().getFullTableName());
    }
}

void StorageMaterializedView::doCreateInnerTable(const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr local_context)
{
    assert(has_inner_table && target_table_id);

    /// If there is a Create request, then we need create the target inner table.
    /// Create inner target table query
    ///   create stream <inner_target_table_id> (view_properties) engine = Stream(1, 1, rand());
    /// FIXME: In future, add order clause or remove engine ?
    auto manual_create_query = std::make_shared<ASTCreateQuery>();
    manual_create_query->setDatabase(target_table_id.getDatabaseName());
    manual_create_query->setTable(target_table_id.getTableName());
    manual_create_query->uuid = target_table_id.uuid;

    auto columns_ast = InterpreterCreateQuery::formatColumns(metadata_snapshot->getColumns());
    auto new_columns_list = std::make_shared<ASTColumns>();
    new_columns_list->set(new_columns_list->columns, columns_ast);

    auto new_storage = std::make_shared<ASTStorage>();
    auto engine = makeASTFunction(
        "Stream", std::make_shared<ASTLiteral>(UInt64(1)), std::make_shared<ASTLiteral>(UInt64(1)), makeASTFunction("rand"));
    engine->no_empty_args = true;
    new_storage->set(new_storage->engine, engine);

    manual_create_query->set(manual_create_query->columns_list, new_columns_list);
    manual_create_query->set(manual_create_query->storage, new_storage);

    InterpreterCreateQuery create_interpreter(manual_create_query, local_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();

    target_table_id = DatabaseCatalog::instance()
                          .getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, getContext())
                          ->getStorageID();
}

void StorageMaterializedView::updateStorageSettings()
{
    StorageInMemoryMetadata meta(*getInMemoryMetadataPtr());
    meta.setSettingsChanges(getTargetTable()->getInMemoryMetadataPtr()->getSettingsChanges());
    setInMemoryMetadata(meta);
}

void StorageMaterializedView::buildBackgroundPipeline()
{
    auto target_table = getTargetTable();
    auto target_metadata_snapshot = target_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();

    auto local_context = Context::createCopy(getContext());
    local_context->makeQueryContext();
    local_context->setCurrentQueryId(""); /// generate random query_id
    local_context->setInternalQuery(false); /// We like to log materialized query like a regular one

    InterpreterSelectWithUnionQuery select_interpreter(
        metadata_snapshot->getSelectQuery().inner_query, local_context, SelectQueryOptions());

    /// [Pipeline]: `Source` -> `Converting` -> `Materializing const` -> `target_table`
    background_pipeline = select_interpreter.buildQueryPipeline();
    background_pipeline.resize(1);
    const auto & source_header = background_pipeline.getHeader();

    Block target_header;
    Names insert_columns;
    ActionsDAG::MatchColumnsMode match_mode = ActionsDAG::MatchColumnsMode::Position;

    if (!has_inner_table)
    {
        /// Has specified `INTO [target stream]`
        /// The `select` output header may not match the target stream's schema

        target_header.reserve(source_header.columns());
        /// Insert columns only returned by select query
        const auto & target_table_columns = target_metadata_snapshot->getColumns();
        auto target_storage_header{target_metadata_snapshot->getSampleBlock()};
        for (const auto & source_column : source_header)
        {
            /// Skip columns which target storage doesn't have
            if (target_table_columns.hasPhysical(source_column.name))
            {
                insert_columns.emplace_back(source_column.name);
                target_header.insert(target_storage_header.getByName(source_column.name));
            }
        }
        /// We will need use by name since we may have less columns after filtering
        match_mode = ActionsDAG::MatchColumnsMode::Name;
    }
    else
    {
        /// Internally inner table. The inner storage header shall be identical as the source header
        insert_columns = source_header.getNames();
        target_header = metadata_snapshot->getSampleBlock();

        /// When inserted columns don't contains `_tp_time`, we will skip it, which will be filled by default
        if (!source_header.has(ProtonConsts::RESERVED_EVENT_TIME) && target_header.has(ProtonConsts::RESERVED_EVENT_TIME))
            target_header.erase(ProtonConsts::RESERVED_EVENT_TIME);
    }

    if (!blocksHaveEqualStructure(source_header, target_header))
    {
        auto converting = ActionsDAG::makeConvertingActions(
            source_header.getColumnsWithTypeAndName(), target_header.getColumnsWithTypeAndName(), match_mode);

        background_pipeline.addTransform(std::make_shared<ExpressionTransform>(
            source_header,
            std::make_shared<ExpressionActions>(
                std::move(converting), ExpressionActionsSettings::fromContext(local_context, CompileExpressions::yes))));
    }

    /// Materializing const columns
    background_pipeline.addTransform(std::make_shared<MaterializingTransform>(background_pipeline.getHeader()));

    /// Sink to target table
    InterpreterInsertQuery interpreter(nullptr, local_context, false, false, false);
    /// FIXME, thread status
    auto out_chain = interpreter.buildChain(target_table, target_metadata_snapshot, insert_columns, nullptr, nullptr);
    out_chain.addStorageHolder(target_table);

    background_pipeline.addChain(std::move(out_chain));

    background_pipeline.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
        return std::make_shared<EmptySink>(cur_header);
    });

    local_context->setInsertionTable(target_table->getStorageID());
    local_context->setupQueryStatusPollId(target_table->as<StorageStream &>().nextBlockId()); /// Async insert requried
}

void StorageMaterializedView::executeBackgroundPipeline()
{
    background_executor = background_pipeline.execute();
    background_thread = ThreadFromGlobalPool{[this]() {
        try
        {
            LOG_INFO(log, "Background pipeline started for materialized view {}", getStorageID().getFullTableName());
            assert(background_executor);
            background_executor->execute(background_pipeline.getNumThreads());
        }
        catch (...)
        {
            /// FIXME: checkpointing
            background_status.exception = std::current_exception();
            background_status.has_exception = true;

            LOG_ERROR(log, "Background runtime error: {}", getExceptionMessage(background_status.exception, false));
        }
    }};
}

StorageSnapshotPtr StorageMaterializedView::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return getTargetTable()->getStorageSnapshot(metadata_snapshot, query_context);
}

MergeTreeSettingsPtr StorageMaterializedView::getSettings() const
{
    if (auto * stream = getTargetTable()->as<StorageStream>())
        return stream->getSettings();

    return nullptr;
}

void registerStorageMaterializedView(StorageFactory & factory)
{
    factory.registerStorage("MaterializedView", [](const StorageFactory::Arguments & args) {
        /// Pass local_context here to convey setting for inner table
        return StorageMaterializedView::create(
            args.table_id, args.getLocalContext(), args.query, args.columns, args.attach, args.is_virtual);
    });
}

}
