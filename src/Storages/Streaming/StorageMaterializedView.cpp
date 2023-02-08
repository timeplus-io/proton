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
#include <Storages/SelectQueryDescription.h>
#include <Storages/StorageFactory.h>
#include <Common/ProtonCommon.h>

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

    auto select = SelectQueryDescription::getSelectQueryFromASTForMatView(query.select->clone(), local_context);

    /// FIXME, we shall fix the above resolution ?
    /// Fix database name for tables if we didn't resolve them otherwise addDependency / removeDependency will fail
    for (auto & select_table_id : select.select_table_ids)
        if (select_table_id.database_name.empty())
            select_table_id.database_name = table_id_.getDatabaseName();

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
            auto target_table = getTargetTable();
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
        /// We will create a query to create an internal stream
        /// For now, we create inner stream during startup, FIXME, why?
        target_table_id = StorageID(getStorageID().database_name, generateInnerTableName(getStorageID()), query.to_inner_uuid);
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
///                             \- (into_target_table) so far non-support
///
///                             InMemoryTable                                       TargetTable
/// global_aggr:    (view_properties, RESERVED_VIEW_VERSION)        (view_properties, RESERVED_VIEW_VERSION)
/// others:         (view_properties)                               (view_properties)
void StorageMaterializedView::startup()
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage_id = getStorageID();
    const auto & select_query = metadata_snapshot->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().addDependency(select_table_id, storage_id);

    start_thread = ThreadFromGlobalPool{[this]() {
        createInnerTableIfNecessary();
        executeSelectPipeline();
    }};
}

void StorageMaterializedView::shutdown()
{
    if (shutdown_called.test_and_set())
        return;

    cancelBackgroundPipeline();

    if (start_thread.joinable())
        start_thread.join();
}

void StorageMaterializedView::executeSelectPipeline()
{
    if (is_virtual)
        return;

    try
    {
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
    /// stop query, if the inner table has been deleted.
    if (!storage)
        throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "Inner table of {}", getStorageID().getFullTableName());

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
    auto table_id = getStorageID();
    const auto & select_query = getInMemoryMetadataPtr()->getSelectQuery();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().removeDependency(select_table_id, table_id);

    dropInnerTableIfAny(true, getContext());
}

void StorageMaterializedView::dropInnerTableIfAny(bool no_delay, ContextPtr local_context)
{
    if (has_inner_table && target_table_id)
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, target_table_id, no_delay);

    target_table_storage = nullptr;
}

void StorageMaterializedView::alter(const AlterCommands &, ContextPtr, AlterLockHolder &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Altering Materialized view is not supported");
}

void StorageMaterializedView::checkTableCanBeRenamed() const
{
    auto dependencies = DatabaseCatalog::instance().getDependencies(getStorageID());
    if (!dependencies.empty())
    {
        WriteBufferFromOwnString ss;
        ss << dependencies.begin()->getFullTableName();
        for (auto iter = dependencies.begin() + 1; iter != dependencies.end(); ++iter)
            ss << ", " << iter->getFullTableName();

        throw Exception("Cannot rename, there are some dependencies: " + ss.str(), ErrorCodes::NOT_IMPLEMENTED);
    }
}

void StorageMaterializedView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr ctx) const
{
    auto target = target_table_storage;
    if (!target)
        target = DatabaseCatalog::instance().getTable(target_table_id, getContext());

    if (target)
        target->checkAlterIsPossible(commands, ctx);
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

StoragePtr StorageMaterializedView::getTargetTable()
{
    /// Cache the target table storage
    try
    {
        if (!target_table_storage)
            target_table_storage = DatabaseCatalog::instance().getTable(target_table_id, getContext());
    }
    catch (Exception &)
    {
        /// sometimes during asynchronously deleting mv, the target might have be deleted already
        LOG_ERROR(log, "inner table {} does not exists", target_table_id.getFullTableName());
        return nullptr;
    }

    return target_table_storage;
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
}

NamesAndTypesList StorageMaterializedView::getVirtuals() const
{
    /// So far we always have inner target table.
    assert(target_table_storage);
    return target_table_storage->getVirtuals();
}

bool StorageMaterializedView::createInnerTableIfNecessary()
{
    if (is_attach || is_virtual || !has_inner_table)
        /// In attach or virtual or `INTO [target stream]` scenarios, inner stream is supposed to already exist
        return true;

    assert(target_table_id);

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
            createInnerTable(metadata_snapshot, local_context);
            return true;
        }
        catch (...)
        {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            LOG_DEBUG(log, "'{}' waiting for inner stream ready", getStorageID().getFullTableName());
        }
    }

    if (max_retries <= 0)
    {
        background_status.has_exception = true;
        background_status.exception = std::make_exception_ptr(Exception(
            ErrorCodes::RESOURCE_NOT_INITED,
            "Failed to create inner stream for Materialized view '{}'",
            getStorageID().getFullTableName()));

        LOG_ERROR(log, "Failed to create inner stream for Materialized view '{}'", getStorageID().getFullTableName());
        return false;
    }

    return true;
}

void StorageMaterializedView::createInnerTable(const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr local_context)
{
    assert(has_inner_table);

    /// If there is a Create request, then we need create the target inner table.
    /// Create inner target table query
    ///   create stream <inner_target_table_id> (view_properties[, ProtonConsts::RESERVED_VIEW_VERSION]) engine = Stream(1, 1, rand());
    /// FIXME: In future, add order clause or remove engine ?
    auto manual_create_query = std::make_shared<ASTCreateQuery>();
    manual_create_query->setDatabase(target_table_id.getDatabaseName());
    manual_create_query->setTable(target_table_id.getTableName());
    manual_create_query->uuid = target_table_id.uuid;

    auto names_and_types = metadata_snapshot->getColumns().getAll();
    auto columns_ast = InterpreterCreateQuery::formatColumns(names_and_types);
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

    target_table_storage
        = DatabaseCatalog::instance().getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, local_context);

    updateStorageSettings();
}

void StorageMaterializedView::updateStorageSettings()
{
    auto target = getTargetTable();
    if (target)
    {
        StorageInMemoryMetadata meta(*getInMemoryMetadataPtr());
        meta.setSettingsChanges(target->getInMemoryMetadataPtr()->getSettingsChanges());
        setInMemoryMetadata(meta);
    }
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
    if (target_table_storage)
    {
        auto storage_snapshot = target_table_storage->getStorageSnapshot(metadata_snapshot, query_context)->clone();
        /// Add virtuals, such as `_tp_version`
        storage_snapshot->addVirtuals(getVirtuals());
        return storage_snapshot;
    }
    else
        /// TODO: Update dynamic object description for InMemoryTable ?
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot);
}

MergeTreeSettingsPtr StorageMaterializedView::getSettings() const
{
    if (target_table_storage)
    {
        if (auto * stream = target_table_storage->as<StorageStream>())
            return stream->getSettings();
    }
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
