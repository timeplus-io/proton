#include "StorageStreamingView.h"

#include <Storages/SelectQueryDescription.h>
#include <Storages/StorageFactory.h>

#include <IO/WriteBufferFromString.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
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

StorageStreamingView::InMemoryTable::InMemoryTable(size_t max_blocks_count_, size_t max_blocks_bytes_)
    : max_blocks_count(max_blocks_count_), max_blocks_bytes(max_blocks_bytes_)
{
}

void StorageStreamingView::InMemoryTable::write(Block && block)
{
    std::lock_guard lock(mutex);
    total_blocks_bytes += block.allocatedBytes();
    data.push_back(std::make_shared<Block>(std::move(block)));

    /// Limits by max blocks count and bytes
    while (total_blocks_bytes > max_blocks_bytes || data.size() > max_blocks_count)
    {
        total_blocks_bytes -= data.front()->allocatedBytes();
        data.pop_front();
    }
}

StorageStreamingView::Data StorageStreamingView::InMemoryTable::get() const
{
    std::shared_lock lock(mutex);
    return data;
}

class CheckStreamingViewValidTransform : public ISimpleTransform
{
public:
    CheckStreamingViewValidTransform(const Block & header_, const StorageStreamingView & view_)
        : ISimpleTransform(header_, header_, false), view(view_)
    {
    }

    String getName() const override { return "CheckStreamingViewValidTransform"; }

protected:
    void transform(Chunk &) override { view.checkValid(); }

private:
    const StorageStreamingView & view;
};

class StreamingViewMemorySource : public SourceWithProgress
{
public:
    StreamingViewMemorySource(const StorageStreamingView & view_, const Block & header)
        : SourceWithProgress(header)
        , column_names_and_types(header.getNamesAndTypesList())
        , data(view_.memory_table ? view_.memory_table->get() : StorageStreamingView::Data())
        , iter(data.begin())
    {
    }

    String getName() const override { return "StreamingViewMemory"; }

protected:
    Chunk generate() override
    {
        if (iter == data.end())
            return {};

        const Block & src = *(iter->get());
        auto rows = src.rows();

        Columns columns;
        columns.reserve(column_names_and_types.size());

        /// Add only required columns to `res`.
        for (const auto & elem : column_names_and_types)
            columns.emplace_back(getColumnFromBlock(src, elem));

        ++iter;
        return Chunk(std::move(columns), rows);
    }

private:
    const NamesAndTypesList column_names_and_types;
    StorageStreamingView::Data data;
    StorageStreamingView::DataConstIterator iter;
};

class PushingToStreamingViewMemorySink final : public ExceptionKeepingTransform
{
private:
    StorageStreamingView & view;
    const StorageStreamingView::VirtualColumns & to_calc_virtual_columns;
    size_t expected_virtual_num = 0;

public:
    PushingToStreamingViewMemorySink(
        const Block & in_header,
        const Block & out_header,
        StorageStreamingView & view_,
        const StorageStreamingView::VirtualColumns & to_calc_virtual_columns_)
        : ExceptionKeepingTransform(in_header, out_header)
        , view(view_)
        , to_calc_virtual_columns(to_calc_virtual_columns_)
        , expected_virtual_num(out_header.columns() - in_header.columns())
    {
        assert(expected_virtual_num <= to_calc_virtual_columns.size());
    }

    String getName() const override { return "PushingToStreamingViewMemory"; }

protected:
    void onConsume(Chunk chunk) override
    {
        if (!chunk.hasColumns())
            return;

        auto newest_block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        auto rows = newest_block.rows();

        /// Calc and add virtual columns if expected
        for (size_t i = 0; i < expected_virtual_num; ++i)
        {
            auto & [name, type, calc_func] = to_calc_virtual_columns[i];
            auto virtual_column = rows > 0 ? type->createColumnConst(rows, calc_func()) : type->createColumn();
            newest_block.insert({virtual_column, type, name});
        }

        chunk.setColumns(newest_block.getColumns(), rows);

        /// Write newest data snapshot in memory
        /// There may be some empty block(heart block) from tail mode, ignore it.
        if (rows > 0 && view.memory_table)
            view.memory_table->write(std::move(newest_block));

        cur_chunk = std::move(chunk);
    }

    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        return res;
    }

    Chunk cur_chunk;
};

StorageStreamingView::StorageStreamingView(
    const StorageID & table_id_, ContextPtr local_context, const ASTCreateQuery & query, const ColumnsDescription & columns_, bool attach_)
    : IStorage(table_id_)
    , WithMutableContext(local_context->getGlobalContext())
    , log(&Poco::Logger::get("StorageStreamingView (" + table_id_.database_name + "." + table_id_.table_name + ")"))
    , is_attach(attach_)
    , virtual_columns({{RESERVED_VIEW_VERSION, std::make_shared<DataTypeInt64>(), []() -> Int64 { return UTCMilliseconds::now(); }}})
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception("UNION is not supported for Streaming View", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW);

    auto select = SelectQueryDescription::getSelectQueryFromASTForMatView(query.select->clone(), local_context);
    storage_metadata.setSelectQuery(select);
    setInMemoryMetadata(storage_metadata);

    if (!query.to_table_id.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Streaming View {} doesn't support INTO clause", table_id_.getFullTableName());

    bool point_to_itself_by_uuid = query.to_inner_uuid != UUIDHelpers::Nil && query.to_inner_uuid == table_id_.uuid;
    if (point_to_itself_by_uuid)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Streaming View {} cannot point to itself", table_id_.getFullTableName());


    target_table_id = StorageID(getStorageID().database_name, generateInnerTableName(getStorageID()), query.to_inner_uuid);
}

StorageStreamingView::~StorageStreamingView()
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
void StorageStreamingView::startup()
{
    try
    {
        auto local_context = Context::createCopy(getContext());
        local_context->makeQueryContext();
        local_context->setCurrentQueryId(""); // generate random query_id

        auto metadata_snapshot = getInMemoryMetadataPtr();
        InterpreterSelectQuery select_interpreter(metadata_snapshot->getSelectQuery().inner_query, local_context, SelectQueryOptions());
        if (!select_interpreter.isStreaming())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Streaming View doesn't support historical query");

        is_global_aggr_query = select_interpreter.hasGlobalAggregation();

        /// Init inner memory table and inner target table
        initInnerTable(metadata_snapshot, local_context);

        /// Build inner background query pipeline and keep it alive during the lifetime of Proton
        buildBackgroundPipeline(select_interpreter, metadata_snapshot, local_context);

        /// Run background pipeline
        executeBackgroundPipeline();

        /// Update metadata in memory since we want to show version column for global aggr (select *)
        if (is_global_aggr_query)
        {
            auto new_metadata = getInMemoryMetadata();
            auto new_names_and_types = metadata_snapshot->getColumns().getAll();
            const auto & virtuals = getVirtuals();
            new_names_and_types.insert(new_names_and_types.end(), virtuals.begin(), virtuals.end());
            new_metadata.setColumns(ColumnsDescription(new_names_and_types));
            setInMemoryMetadata(new_metadata);
        }
    }
    catch (...)
    {
        background_status.exception = std::current_exception();
        background_status.has_exception = true;

        LOG_ERROR(log, "{}", getExceptionMessage(background_status.exception, false));

        /// Exception safety: failed "startup" does not require a call to "shutdown" from the caller.
        /// And it should be able to safely destroy table after exception in "startup" method.
        /// It means that failed "startup" must not create any background tasks that we will have to wait.
        shutdown();

        /// Note: after failed "startup", the table will be in a state that only allows to destroy the object.
        /// If is an Attach request, we didn't throw exception to avoid the system fail to setup.
        if (!is_attach)
            throw;
    }
}

void StorageStreamingView::shutdown()
{
    if (shutdown_called.test_and_set())
        return;

    if (background_executor)
    {
        background_executor->cancel();
        if (background_thread.joinable())
            background_thread.join();

        background_executor.reset();
        background_pipeline.reset();
    }

    if (memory_table)
        memory_table.reset();
}

Pipe StorageStreamingView::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe(
        QueryPlanOptimizationSettings::fromContext(local_context), BuildQueryPipelineSettings::fromContext(local_context));
}

void StorageStreamingView::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    /// There are two paths:
    /// 1) read newest streaming data in target table.      [streaming query from target table]
    ///     e.g. "select * from streaming_view;" <=> "select * from target_table" (streaming)
    /// 2) read newest data snapshot in memory              [historical query from memory table]
    ///     e.g. "select * from table(streaming_view);" <=> "select * from memory_table" (historical)

    /// In some cases, the view background thread has exception, we check it before users access this view
    checkValid();

    Block header;
    if (!column_names.empty())
        header = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
    else
        header = metadata_snapshot->getSampleBlockForColumns({RESERVED_VIEW_VERSION}, getVirtuals(), getStorageID());

    if (query_info.syntax_analyzer_result->streaming)
    {
        auto storage = getTargetTable();
        auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
        auto target_metadata_snapshot = storage->getInMemoryMetadataPtr();

        if (query_info.order_optimizer)
            query_info.input_order_info = query_info.order_optimizer->getInputOrder(target_metadata_snapshot, local_context);

        auto pipe = storage->read(
            column_names, target_metadata_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);

        /// Add valid check of the view
        /// If not check, when the view go bad, the streaming query of the target table will be blocked indefinitely since there is no ingestion on background.
        pipe.addTransform(std::make_shared<CheckStreamingViewValidTransform>(header, *this));

        auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), getName() + "-Target");
        query_plan.addStep(std::move(read_step));

        StreamLocalLimits limits;
        SizeLimits leaf_limits;

        /// Add table lock for destination table.
        auto adding_limits_and_quota = std::make_unique<SettingQuotaAndLimitsStep>(
            query_plan.getCurrentDataStream(), storage, std::move(lock), limits, leaf_limits, nullptr, nullptr);

        adding_limits_and_quota->setStepDescription("Lock destination table for StreamingView");
        query_plan.addStep(std::move(adding_limits_and_quota));
    }
    else
    {
        Pipe pipe(std::make_shared<StreamingViewMemorySource>(*this, header));

        /// Materializing const column
        pipe.addTransform(std::make_shared<MaterializingTransform>(header));

        auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), getName() + "-Memory");
        query_plan.addStep(std::move(read_step));
    }
}

void StorageStreamingView::drop()
{
    dropInnerTableIfAny(true, getContext());
}

void StorageStreamingView::dropInnerTableIfAny(bool no_delay, ContextPtr local_context)
{
    /// So far, the target tabel is always inner table
    if (target_table_id)
        InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, target_table_id, no_delay);

    target_table_storage = nullptr;
}

void StorageStreamingView::checkTableCanBeRenamed() const
{
    auto dependencies = DatabaseCatalog::instance().getDependencies(getStorageID());
    if (dependencies.size() > 0)
    {
        WriteBufferFromOwnString ss;
        ss << dependencies.begin()->getFullTableName();
        for (auto iter = dependencies.begin() + 1; iter != dependencies.end(); ++iter)
            ss << ", " << iter->getFullTableName();

        throw Exception("Cannot rename, there are some dependencies: " + ss.str(), ErrorCodes::NOT_IMPLEMENTED);
    }
}

void StorageStreamingView::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    IStorage::renameInMemory(new_table_id);

    const auto & select_query = metadata_snapshot->getSelectQuery();
    // TODO Actually we don't need to update dependency if MV has UUID, but then db and table name will be outdated
    DatabaseCatalog::instance().updateDependency(select_query.select_table_id, old_table_id, select_query.select_table_id, getStorageID());
}

StoragePtr StorageStreamingView::getTargetTable()
{
    /// Cache the target table storage
    if (!target_table_storage)
        target_table_storage = DatabaseCatalog::instance().getTable(target_table_id, getContext());

    return target_table_storage;
}

void StorageStreamingView::checkValid() const
{
    if (background_status.has_exception)
        throw Exception(
            getExceptionErrorCode(background_status.exception),
            "Bad StreamingView, please drop it or try recovery by restart server. background exception: {}",
            getExceptionMessage(background_status.exception, false));
}

NamesAndTypesList StorageStreamingView::getVirtuals() const
{
    if (is_global_aggr_query)
        return NamesAndTypesList{NameAndTypePair(RESERVED_VIEW_VERSION, std::make_shared<DataTypeInt64>())};
    else
        return {};
}

void StorageStreamingView::initInnerTable(const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr local_context)
{
    /// Init in memory table
    const auto & settings = local_context->getSettingsRef();
    memory_table.reset(new InMemoryTable(
        is_global_aggr_query ? 1 /* only cache current block result */
                             : settings.max_streaming_view_cached_block_count,
        settings.max_streaming_view_cached_block_bytes));

    /// If there is a Create request, then we need create the target inner table.
    assert(target_table_id);
    if (!is_attach)
    {
        /// Create inner target table query
        ///   create table <inner_target_table_id> (view_properties[, RESERVED_VIEW_VERSION]) engine = DistributedMergeTree(1, 1, rand());
        /// FIXME: In future, add order clause or remove engine ?
        auto manual_create_query = std::make_shared<ASTCreateQuery>();
        manual_create_query->setDatabase(target_table_id.getDatabaseName());
        manual_create_query->setTable(target_table_id.getTableName());
        manual_create_query->uuid = target_table_id.uuid;

        auto names_and_types = metadata_snapshot->getColumns().getAll();
        const auto & virtuals = getVirtuals();
        names_and_types.insert(names_and_types.end(), virtuals.begin(), virtuals.end());
        auto columns_ast = InterpreterCreateQuery::formatColumns(names_and_types);
        auto new_columns_list = std::make_shared<ASTColumns>();
        new_columns_list->set(new_columns_list->columns, columns_ast);

        auto new_storage = std::make_shared<ASTStorage>();
        auto engine = makeASTFunction(
            "DistributedMergeTree",
            std::make_shared<ASTLiteral>(UInt64(1)),
            std::make_shared<ASTLiteral>(UInt64(1)),
            makeASTFunction("rand"));
        engine->no_empty_args = true;
        new_storage->set(new_storage->engine, engine);

        manual_create_query->set(manual_create_query->columns_list, new_columns_list);
        manual_create_query->set(manual_create_query->storage, new_storage);

        InterpreterCreateQuery create_interpreter(manual_create_query, local_context);
        create_interpreter.setInternal(true);
        create_interpreter.execute();

        target_table_storage
            = DatabaseCatalog::instance().getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, local_context);
    }
    else
        getTargetTable();
}

void StorageStreamingView::buildBackgroundPipeline(
    InterpreterSelectQuery & inner_interpreter, const StorageMetadataPtr & metadata_snapshot, ContextMutablePtr local_context)
{
    /// [Pipeline]: `Source` -> `Converting` -> `PushingToStreamingViewMemorySink` -> `Materializing const` -> `target_table`
    background_pipeline = inner_interpreter.buildQueryPipeline();
    background_pipeline.resize(1);
    const auto & current_header = background_pipeline.getHeader();

    /// Converting since the view properties allows to explicitly specify
    auto inner_converting_view_dag = ActionsDAG::makeConvertingActions(
        current_header.getColumnsWithTypeAndName(),
        metadata_snapshot->getSampleBlock().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Position);
    auto inner_converting_view_actions = std::make_shared<ExpressionActions>(
        inner_converting_view_dag, ExpressionActionsSettings::fromContext(local_context, CompileExpressions::yes));
    background_pipeline.addSimpleTransform([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
        return std::make_shared<ExpressionTransform>(cur_header, inner_converting_view_actions);
    });

    /// Pushing newest data with virtual generated data to memory
    /// and output:
    /// 1) if is global aggr, we output additional `RESERVED_VIEW_VERSION` for target table
    auto out_header = current_header;
    if (is_global_aggr_query)
    {
        assert(virtual_columns.size() > 0);
        const auto & [name, type, calc_func] = virtual_columns.front();
        out_header.insert({type->createColumn(), type, name});
    }

    background_pipeline.addSimpleTransform([&, this](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
        return std::make_shared<PushingToStreamingViewMemorySink>(cur_header, out_header, *this, virtual_columns);
    });

    /// Materializing const columns
    background_pipeline.addSimpleTransform([](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
        return std::make_shared<MaterializingTransform>(cur_header);
    });

    auto target_table = getTargetTable();
    if (target_table->getName() != "DistributedMergeTree")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Streaming View doesn't support target table is {}", target_table->getName());

    /// Sink to target table
    InterpreterInsertQuery interpreter(nullptr, local_context, false, true /* no_squash */);
    auto out_chain = interpreter.buildChain(target_table, target_table->getInMemoryMetadataPtr(), out_header.getNames(), nullptr, nullptr);
    out_chain.addStorageHolder(target_table);

    background_pipeline.addChain(std::move(out_chain));

    background_pipeline.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
        return std::make_shared<EmptySink>(cur_header);
    });

    local_context->setInsertionTable(target_table->getStorageID());
    local_context->setupQueryStatusPollId();
}

void StorageStreamingView::executeBackgroundPipeline()
{
    background_executor = background_pipeline.execute();
    background_thread = ThreadFromGlobalPool{[this]() {
        try
        {
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

void registerStorageStreamingView(StorageFactory & factory)
{
    factory.registerStorage("StreamingView", [](const StorageFactory::Arguments & args) {
        /// Pass local_context here to convey setting for inner table
        return StorageStreamingView::create(args.table_id, args.getLocalContext(), args.query, args.columns, args.attach);
    });
}

}
