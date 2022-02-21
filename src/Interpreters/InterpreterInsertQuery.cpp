#include <Interpreters/InterpreterInsertQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnNullable.h>
#include <Processors/Transforms/buildPushingToViewsChain.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/processColumnTransformers.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Transforms/CheckConstraintsTransform.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Storages/StorageDistributed.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/checkStackSize.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
}

InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, ContextPtr context_, bool allow_materialized_, bool no_squash_, bool no_destination_, bool async_insert_)
    : WithContext(context_)
    , query_ptr(query_ptr_)
    , allow_materialized(allow_materialized_)
    , no_squash(no_squash_)
    , no_destination(no_destination_)
    , async_insert(async_insert_)
{
    checkStackSize();
}


StoragePtr InterpreterInsertQuery::getTable(ASTInsertQuery & query)
{
    if (query.table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        TableFunctionPtr table_function_ptr = factory.get(query.table_function, getContext());
        return table_function_ptr->execute(query.table_function, getContext(), table_function_ptr->getName());
    }

    if (query.table_id)
    {
        query.table_id = getContext()->resolveStorageID(query.table_id);
    }
    else
    {
        /// Insert query parser does not fill table_id because table and
        /// database can be parameters and be filled after parsing.
        StorageID local_table_id(query.getDatabase(), query.getTable());
        query.table_id = getContext()->resolveStorageID(local_table_id);
    }

    return DatabaseCatalog::instance().getTable(query.table_id, getContext());
}

Block InterpreterInsertQuery::getSampleBlock(
    const ASTInsertQuery & query,
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot) const
{
    /// If the query does not include information about columns
    if (!query.columns)
    {
        if (no_destination)
            return metadata_snapshot->getSampleBlockWithVirtuals(table->getVirtuals());
        else
            return metadata_snapshot->getSampleBlockNonMaterialized();
    }

    /// Form the block based on the column names from the query
    Names names;
    const auto columns_ast = processColumnTransformers(getContext()->getCurrentDatabase(), table, metadata_snapshot, query.columns);
    for (const auto & identifier : columns_ast->children)
    {
        std::string current_name = identifier->getColumnName();
        names.emplace_back(std::move(current_name));
    }

    return getSampleBlock(names, table, metadata_snapshot);
}

Block InterpreterInsertQuery::getSampleBlock(
    const Names & names,
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot) const
{
    Block table_sample = metadata_snapshot->getSampleBlock();
    Block table_sample_non_materialized = metadata_snapshot->getSampleBlockNonMaterialized();
    Block res;
    for (const auto & current_name : names)
    {
        /// The table does not have a column with that name
        if (!table_sample.has(current_name))
            throw Exception("No such column " + current_name + " in table " + table->getStorageID().getNameForLogs(),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!allow_materialized && !table_sample_non_materialized.has(current_name))
            throw Exception("Cannot insert column " + current_name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        if (res.has(current_name))
            throw Exception("Column " + current_name + " specified more than once", ErrorCodes::DUPLICATE_COLUMN);

        res.insert(ColumnWithTypeAndName(table_sample.getByName(current_name).type, current_name));
    }
    return res;
}


/** A query that just reads all data without any complex computations or filetering.
  * If we just pipe the result to INSERT, we don't have to use too many threads for read.
  */
static bool isTrivialSelect(const ASTPtr & select)
{
    if (auto * select_query = select->as<ASTSelectQuery>())
    {
        const auto & tables = select_query->tables();

        if (!tables)
            return false;

        const auto & tables_in_select_query = tables->as<ASTTablesInSelectQuery &>();

        if (tables_in_select_query.children.size() != 1)
            return false;

        const auto & child = tables_in_select_query.children.front();
        const auto & table_element = child->as<ASTTablesInSelectQueryElement &>();
        const auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

        if (table_expr.subquery)
            return false;

        /// Note: how to write it in more generic way?
        return (!select_query->distinct
            && !select_query->limit_with_ties
            && !select_query->prewhere()
            && !select_query->where()
            && !select_query->groupBy()
            && !select_query->having()
            && !select_query->orderBy()
            && !select_query->limitBy());
    }
    /// This query is ASTSelectWithUnionQuery subquery
    return false;
};

Chain InterpreterInsertQuery::buildChain(
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & columns,
    ThreadStatus * thread_status,
    std::atomic_uint64_t * elapsed_counter_ms)
{
    auto sample = getSampleBlock(columns, table, metadata_snapshot);
    return buildChainImpl(table, metadata_snapshot, std::move(sample) , thread_status, elapsed_counter_ms);
}

Chain InterpreterInsertQuery::buildChainImpl(
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    const Block & query_sample_block,
    ThreadStatus * thread_status,
    std::atomic_uint64_t * elapsed_counter_ms)
{
    auto context_ptr = getContext();
    const Settings & settings = context_ptr->getSettingsRef();
    if (settings.enable_light_ingest)
        return buildChainLightImpl(table, metadata_snapshot, query_sample_block, thread_status, elapsed_counter_ms);

    /// The only case is system tables inserts. System tables are still merge tree
    /// we don't support in-memory table
    assert(table->getName() == "MergeTree");

    const ASTInsertQuery * query = nullptr;
    if (query_ptr)
        query = query_ptr->as<ASTInsertQuery>();

    bool null_as_default = query && query->select && settings.insert_null_as_default;

    /// We create a pipeline of several streams, into which we will write data.
    Chain out;

    /// Keep a reference to the context to make sure it stays alive until the chain is executed and destroyed
    out.addInterpreterContext(context_ptr);

    auto sink = table->write(query_ptr, metadata_snapshot, context_ptr);
    sink->setRuntimeData(thread_status, elapsed_counter_ms);
    out.addSource(std::move(sink));

    /// Note that we wrap transforms one on top of another, so we write them in reverse of data processing order.

    /// proton: we don't support constraints
    /// Checking constraints. It must be done after calculation of all defaults, so we can check them on calculated columns.
    /// if (const auto & constraints = metadata_snapshot->getConstraints(); !constraints.empty())
    ///    out.addSource(std::make_shared<CheckConstraintsTransform>(
    ///        table->getStorageID(), out.getInputHeader(), metadata_snapshot->getConstraints(), context_ptr));

    auto adding_missing_defaults_dag = addMissingDefaults(
        query_sample_block,
        out.getInputHeader().getNamesAndTypesList(),
        metadata_snapshot->getColumns(),
        context_ptr,
        null_as_default);

    auto adding_missing_defaults_actions = std::make_shared<ExpressionActions>(adding_missing_defaults_dag);

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    out.addSource(std::make_shared<ConvertingTransform>(query_sample_block, adding_missing_defaults_actions));

    /// It's important to squash blocks as early as possible (before other transforms),
    ///  because other transforms may work inefficient if block size is small.

    /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
    /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
    if (!(settings.insert_distributed_sync && table->isRemote()) && !no_squash && !(query && query->watch))
    {
        bool table_prefers_large_blocks = table->prefersLargeBlocks();

        out.addSource(std::make_shared<SquashingChunksTransform>(
            out.getInputHeader(),
            table_prefers_large_blocks ? settings.min_insert_block_size_rows : settings.max_block_size,
            table_prefers_large_blocks ? settings.min_insert_block_size_bytes : 0));
    }

    auto counting = std::make_shared<CountingTransform>(out.getInputHeader(), thread_status);
    counting->setProcessListElement(context_ptr->getProcessListElement());
    out.addSource(std::move(counting));

    return out;
}

/// proton: light insert. We don't like to insert missing columns which don't have defaults
/// to streaming store since it is inefficient and blow streaming storage.
/// Instead during query, we refill the missing columns.
/// With this change, the block i streaming store may contain partial columns of the table schema
Chain InterpreterInsertQuery::buildChainLightImpl(
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot,
    const Block & query_sample_block,
    ThreadStatus * thread_status,
    std::atomic_uint64_t * elapsed_counter_ms)
{
    auto context_ptr = getContext();
    const ASTInsertQuery * query = nullptr;
    if (query_ptr)
        query = query_ptr->as<ASTInsertQuery>();

    assert(table->getName() == "DistributedMergeTree");

    const Settings & settings = context_ptr->getSettingsRef();
    bool null_as_default = query && query->select && settings.insert_null_as_default;

    /// We create a pipeline of several streams, into which we will write data.
    Chain out;

    /// Keep a reference to the context to make sure it stays alive until the chain is executed and destroyed
    out.addInterpreterContext(context_ptr);

    /// First figure out the final (sink) block header
    /// There are several cases
    /// 1) Clients ingest full columns
    /// 2) Client ingest partial columns and the other missing columns don't have a default value expr
    /// 3) Client ingest partial columns and the other missing columns have a default value expr which don't depend on other column(s)
    ///    For example, `Int cpu_usage DEFAULT 1`
    /// 4) Client ingest partial columns and the other missing columns have a default value expr which depend on other column(s), but
    ///    the dependent columns are all in `INSERT INTO`
    ///    For example, `Int cpu_usage DEFAULT util`, INSERT INTO t(util) VALUES (2.0), (3.0)
    /// 5) Client ingest partial columns and the other missing columns have a default value expr which depend on other column(s), but
    ///    the dependent columns are not or partially in `INSERT INTO`
    ///    For example, `Int cpu_usage DEFAULT util2`, INSERT INTO t(util) VALUES (2.0), (3.0)

    /// Since there may be dependent columns in columns' default value expressions. We will need manually figure out them first
    /// for example, `_tp_time DateTime64(3) DEFAULT to_timestamp(json_extract(json_column, 'timestamp'))`

    /// required columns include columns users ingest and columns with default values / value expression
    auto header{metadata_snapshot->getSampleBlock()};

    /// We would like sort required columns according to their positions in schema. We like the block serialized
    /// in this order as well. The deserialization assume full column position order is in schema order
    std::map<size_t, NameAndTypePair> sorted_required_columns;
    for (size_t pos = 0; const auto & column : metadata_snapshot->getColumns())
    {
        const auto * col = query_sample_block.findByName(column.name);
        if (col)
        {
            sorted_required_columns.emplace(pos, NameAndTypePair(col->name, col->type));
        }
        else if (column.default_desc.expression)
        {
            /// We don't support materialized columns yet: column.default_desc.kind == ColumnDefaultKind::Materialized
            col = header.findByName(column.name);
            assert(col);
            sorted_required_columns.emplace(pos, NameAndTypePair(col->name, col->type));
        }
        ++pos;
    }

    NamesAndTypesList required_columns;
    for (auto & pos_name_type : sorted_required_columns)
        required_columns.push_back(std::move(pos_name_type.second));

    auto adding_missing_defaults_dag = addMissingDefaultsWithDefaults(
        query_sample_block,
        required_columns,
        metadata_snapshot->getColumns(),
        context_ptr,
        null_as_default);
    auto final_header = ExpressionTransform::transformHeader(query_sample_block, *adding_missing_defaults_dag);

    /// Compare final_header with metadata_snapshot. If final_header doesn't contain all columns in metadata_snapshot
    /// we need create a new metadata_snapshot which just contains final_header columns; otherwise keep metadata_snapshot
    /// as it is
    auto final_metadata_snapshot = metadata_snapshot;
    if (final_header.columns() != header.columns())
    {
        auto light_metadata_snapshot = std::make_shared<StorageInMemoryMetadata>(*metadata_snapshot);
        for (const auto & column : metadata_snapshot->columns)
            if (!final_header.has(column.name))
                light_metadata_snapshot->columns.remove(column.name);

        assert(light_metadata_snapshot->columns.size() == final_header.columns());
        final_metadata_snapshot = light_metadata_snapshot;
    }

    /// After figure out the final structure / columns, use it in Sink
    auto sink = table->write(query_ptr, final_metadata_snapshot, context_ptr);
    sink->setRuntimeData(thread_status, elapsed_counter_ms);
    out.addSource(std::move(sink));

    /// Note that we wrap transforms one on top of another, so we write them in reverse of data processing order.
    auto adding_missing_defaults_actions = std::make_shared<ExpressionActions>(adding_missing_defaults_dag);

    /// Actually we don't know structure of input blocks from query/table,
    /// because some clients break insertion protocol (columns != header)
    out.addSource(std::make_shared<ConvertingTransform>(query_sample_block, adding_missing_defaults_actions));

    /// It's important to squash blocks as early as possible (before other transforms),
    ///  because other transforms may work inefficient if block size is small.

    /// proton: we simply disable squash for distributed merge tree since wal client has buffering already
    /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
    /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
    /// auto counting = std::make_shared<CountingTransform>(out.getInputHeader(), thread_status);
    /// counting->setProcessListElement(context_ptr->getProcessListElement());
    /// out.addSource(std::move(counting));

    return out;
}

BlockIO InterpreterInsertQuery::execute()
{
    const Settings & settings = getContext()->getSettingsRef();
    auto & query = query_ptr->as<ASTInsertQuery &>();

    QueryPipelineBuilder pipeline;

    StoragePtr table = getTable(query);
    StoragePtr inner_table;

    if (query.partition_by && !table->supportsPartitionBy())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PARTITION BY clause is not supported by storage");

    auto table_lock = table->lockForShare(getContext()->getInitialQueryId(), settings.lock_acquire_timeout);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot);
    if (!query.table_function)
        getContext()->checkAccess(AccessType::INSERT, query.table_id, query_sample_block.getNames());

    bool is_distributed_insert_select = false;

    if (query.select && table->isRemote() && settings.parallel_distributed_insert_select)
    {
        // Distributed INSERT SELECT
        if (auto maybe_pipeline = table->distributedWrite(query, getContext()))
        {
            pipeline = std::move(*maybe_pipeline);
            is_distributed_insert_select = true;
        }
    }

    std::vector<Chain> out_chains;
    if (!is_distributed_insert_select || query.watch)
    {
        size_t out_streams_size = 1;

        if (query.select)
        {
            bool is_trivial_insert_select = false;

            if (settings.optimize_trivial_insert_select)
            {
                const auto & select_query = query.select->as<ASTSelectWithUnionQuery &>();
                const auto & selects = select_query.list_of_selects->children;
                const auto & union_modes = select_query.list_of_modes;

                /// ASTSelectWithUnionQuery is not normalized now, so it may pass some queries which can be Trivial select queries
                const auto mode_is_all = [](const auto & mode) { return mode == SelectUnionMode::ALL; };

                is_trivial_insert_select =
                    std::all_of(union_modes.begin(), union_modes.end(), std::move(mode_is_all))
                    && std::all_of(selects.begin(), selects.end(), isTrivialSelect);
            }

            if (is_trivial_insert_select)
            {
                /** When doing trivial INSERT INTO ... SELECT ... FROM table,
                  * don't need to process SELECT with more than max_insert_threads
                  * and it's reasonable to set block size for SELECT to the desired block size for INSERT
                  * to avoid unnecessary squashing.
                  */

                Settings new_settings = getContext()->getSettings();

                new_settings.max_threads = std::max<UInt64>(1, settings.max_insert_threads);

                if (table->prefersLargeBlocks())
                {
                    if (settings.min_insert_block_size_rows)
                        new_settings.max_block_size = settings.min_insert_block_size_rows;
                    if (settings.min_insert_block_size_bytes)
                        new_settings.preferred_block_size_bytes = settings.min_insert_block_size_bytes;
                }

                auto new_context = Context::createCopy(context);
                new_context->setSettings(new_settings);

                InterpreterSelectWithUnionQuery interpreter_select{
                    query.select, new_context, SelectQueryOptions(QueryProcessingStage::Complete, 1)};
                pipeline = interpreter_select.buildQueryPipeline();
            }
            else
            {
                /// Passing 1 as subquery_depth will disable limiting size of intermediate result.
                InterpreterSelectWithUnionQuery interpreter_select{
                    query.select, getContext(), SelectQueryOptions(QueryProcessingStage::Complete, 1)};
                pipeline = interpreter_select.buildQueryPipeline();
            }

            pipeline.dropTotalsAndExtremes();

            if (table->supportsParallelInsert() && settings.max_insert_threads > 1)
                out_streams_size = std::min(size_t(settings.max_insert_threads), pipeline.getNumStreams());

            pipeline.resize(out_streams_size);

            /// Allow to insert Nullable into non-Nullable columns, NULL values will be added as defaults values.
            if (getContext()->getSettingsRef().insert_null_as_default)
            {
                const auto & input_columns = pipeline.getHeader().getColumnsWithTypeAndName();
                const auto & query_columns = query_sample_block.getColumnsWithTypeAndName();
                const auto & output_columns = metadata_snapshot->getColumns();

                if (input_columns.size() == query_columns.size())
                {
                    for (size_t col_idx = 0; col_idx < query_columns.size(); ++col_idx)
                    {
                        /// Change query sample block columns to Nullable to allow inserting nullable columns, where NULL values will be substituted with
                        /// default column values (in AddingDefaultBlockOutputStream), so all values will be cast correctly.
                        if (input_columns[col_idx].type->isNullable() && !query_columns[col_idx].type->isNullable() && output_columns.hasDefault(query_columns[col_idx].name))
                            query_sample_block.setColumn(col_idx, ColumnWithTypeAndName(makeNullable(query_columns[col_idx].column), makeNullable(query_columns[col_idx].type), query_columns[col_idx].name));
                    }
                }
            }
        }
        else if (query.watch)
        {
            InterpreterWatchQuery interpreter_watch{ query.watch, getContext() };
            pipeline = interpreter_watch.buildQueryPipeline();
        }

        for (size_t i = 0; i < out_streams_size; ++i)
        {
            auto out = buildChainImpl(table, metadata_snapshot, query_sample_block, nullptr, nullptr);
            out_chains.emplace_back(std::move(out));
        }
    }

    BlockIO res;

    /// What type of query: INSERT or INSERT SELECT or INSERT WATCH?
    if (is_distributed_insert_select)
    {
        res.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline));
    }
    else if (query.select || query.watch)
    {
        const auto & header = out_chains.at(0).getInputHeader();
        auto actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Position);
        auto actions = std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes));

        pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
        {
            return std::make_shared<ExpressionTransform>(in_header, actions);
        });

        /// We need to convert Sparse columns to full, because it's destination storage
        /// may not support it may have different settings for applying Sparse serialization.
        pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
        {
            return std::make_shared<MaterializingTransform>(in_header);
        });

        size_t num_select_threads = pipeline.getNumThreads();
        size_t num_insert_threads = std::max_element(out_chains.begin(), out_chains.end(), [&](const auto &a, const auto &b)
        {
            return a.getNumThreads() < b.getNumThreads();
        })->getNumThreads();
        pipeline.addChains(std::move(out_chains));

        pipeline.setMaxThreads(num_insert_threads);
        /// Don't use more threads for insert then for select to reduce memory consumption.
        if (!settings.parallel_view_processing && pipeline.getNumThreads() > num_select_threads)
            pipeline.setMaxThreads(num_select_threads);

        pipeline.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
        {
            return std::make_shared<EmptySink>(cur_header);
        });

        if (!allow_materialized)
        {
            for (const auto & column : metadata_snapshot->getColumns())
                if (column.default_desc.kind == ColumnDefaultKind::Materialized && header.has(column.name))
                    throw Exception("Cannot insert column " + column.name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        }

        res.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline));
    }
    else
    {
        res.pipeline = QueryPipeline(std::move(out_chains.at(0)));
        res.pipeline.setNumThreads(std::min<size_t>(res.pipeline.getNumThreads(), settings.max_threads));

        if (query.hasInlinedData() && !async_insert)
        {
            /// can execute without additional data
            auto pipe = getSourceFromASTInsertQuery(query_ptr, true, query_sample_block, getContext(), nullptr);
            res.pipeline.complete(std::move(pipe));
        }
    }

    res.pipeline.addStorageHolder(table);
    if (inner_table)
        res.pipeline.addStorageHolder(inner_table);

    return res;
}


StorageID InterpreterInsertQuery::getDatabaseTable() const
{
    return query_ptr->as<ASTInsertQuery &>().table_id;
}


void InterpreterInsertQuery::extendQueryLogElemImpl(QueryLogElement & elem, ContextPtr context_)
{
    elem.query_kind = "Insert";
    const auto & insert_table = context_->getInsertionTable();
    if (!insert_table.empty())
    {
        elem.query_databases.insert(insert_table.getDatabaseName());
        elem.query_tables.insert(insert_table.getFullNameNotQuoted());
    }
}

void InterpreterInsertQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr context_) const
{
    extendQueryLogElemImpl(elem, context_);
}

}
