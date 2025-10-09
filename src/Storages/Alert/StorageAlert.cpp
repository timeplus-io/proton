#include <Storages/Alert/StorageAlert.h>

#include "config.h"

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Streaming/ISource.h>
#include <Processors/Transforms/BatchingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/ThrottlingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <TableFunctions/TableFunctionPythonCall.h>
#include <Common/Random.h>

#include <boost/smart_ptr/make_shared.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
extern const int RESOURCE_NOT_INITED;
extern const int UNKNOWN_EXCEPTION;
extern const int UNSUPPORTED;
}

namespace
{
String generateInnerQueryID(const StorageID & alert_id)
{
    if (alert_id.hasUUID())
        return fmt::format(".alert-{}", toString(alert_id.uuid));

    return fmt::format(".alert-{}", alert_id.getTableName());
}

ASTPtr parseSelectQuery(String query, ContextPtr context)
{
    ParserSelectQuery select_p;
    String parse_err;
    const auto * query_text = query.data();
    return tryParseQuery(
        select_p,
        query_text,
        query_text + query.size(),
        parse_err,
        /*hilite=*/false,
        "validate alert select query",
        /*allow_multi_statements=*/false,
        /*max_query_size=*/0,
        context->getSettingsRef().max_parser_depth);
}

StorageID getSelectedStorageID(ASTPtr ast)
{
    const auto & select = ast->as<ASTSelectQuery &>();
    if (!select.tables())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Alert query must select from a stream");

    const auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.size() != 1)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY, "Alert query must select from 1 stream, but got {}", tables_in_select_query.children.size());

    const auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    if (!tables_element.table_expression)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Alert query must select from a stream");

    const auto & table_expression = tables_element.table_expression->as<ASTTableExpression &>();
    if (table_expression.database_and_table_name == nullptr)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Alert query must select from a stream");

    auto & id = table_expression.database_and_table_name->as<ASTTableIdentifier &>();
    return id.getTableId();
}
}

void StorageAlert::validateSelectQuery(bool quick)
{
    auto context = getContext();
    auto select_context = Context::createCopy(context);
    select_context->makeQueryContext();
    select_context->setCurrentQueryId(""); /// generate random query_id

    SelectQueryOptions opts;
    if (quick)
    {
        opts.to_stage = QueryProcessingStage::Enum::FetchColumns;
        opts.analyze(true);
    }

    InterpreterSelectQuery select_interpreter(select_query.inner_query, select_context, opts);
    const auto & query_info = select_interpreter.getQueryInfo();
    if (!query_info.syntax_analyzer_result->streaming)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Alert query must be streaming query");

    if (query_info.has_aggregates)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Alert query does not support aggregation");

    if (query_info.has_order_by)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Alert query does not support ORDER BY");

    const auto & analysis = select_interpreter.getAnalysisResult();
    if (analysis.hasJoin())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Alert query does not support JOINs");
}

StorageAlert::StorageAlert(
    StorageID storage_id, cluster::protocol::AlertDescriptorPtr desc_, ValidationLevel validation_level, ContextMutablePtr context_)
    : IStorage(storage_id)
    , WithMutableContext(context_)
    , desc(desc_)
    , query_id(generateInnerQueryID(storage_id))
    , wait_for_dependencies(validation_level == ValidationLevel::None)
    , logger(getLogger("StorageAlert#" + desc_->name))
{
    auto ast = parseSelectQuery(desc->select_query, getContext());
    if (!ast)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Not a select query: {}", desc->select_query);

    auto target_storage_id = getSelectedStorageID(ast);

    select_query.inner_query = ast;
    select_query.select_table_ids.emplace_back(target_storage_id.database_name, target_storage_id.table_name);

    if (validation_level != ValidationLevel::None)
        validateSelectQuery(validation_level == ValidationLevel::Quick);
}

StorageAlert::~StorageAlert()
{
    try
    {
        shutdown(/*dropping=*/false);
        if (executor.joinable())
            executor.join();

        LOG_INFO(logger, "Alert stopped");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void StorageAlert::startup()
{
#if USE_PYTHON_UDF
    if (started.test_and_set())
        return;

    auto storage_id = getStorageID();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().addDependency(select_table_id, storage_id);

    if (!wait_for_dependencies)
        checkDependencies();

    executor = ThreadFromGlobalPool{[alert_holder = shared_from_this(), context_holder = getContext(), this]() noexcept { run(); }};
#else
    throw Exception(ErrorCodes::UNSUPPORTED, "Python UDF is disabled, cannot create Alerts");
#endif
}

void StorageAlert::run()
{
    query_context = Context::createCopy(getContext());
    query_context->setCurrentQueryId(query_id);
    query_context->setIngestMode(IngestMode::Sync); /// Sync ingest to make sure the sink is successful

    CurrentThread::QueryScope query_scope(query_context);

    SCOPE_EXIT(stopped.test_and_set());

    pipeline_stage.store(Stage::Build);

    while (!stopped.test())
    {
        switch (pipeline_stage.load())
        {
            case Stage::Unknown:
                pipeline_stage.store(Stage::Failed);
                setPipelineExeception(Exception(ErrorCodes::LOGICAL_ERROR, "Query pipeline was in unknown stage"));
                break;
            case Stage::Build:
                tryBuildBackgroundPipeline();
                break;
            case Stage::Execute:
                tryExecuteBackgroundPipeline();
                break;
            case Stage::Failed:
                LOG_ERROR(logger, "Pipeline execution failed, error={}", pipeline_exception->message());
                return;
            case Stage::Stopped:
                return;
            case Stage::Error:
                metrics.error_count += 1;

                io.store(nullptr);
                pipeline_stage = Stage::Build;

                auto wait_time = globalRandomInt(1, 2);
                LOG_ERROR(
                    logger,
                    "Encountered error, wait for {} seconds to restart pipeline, error={}",
                    wait_time,
                    pipeline_exception->message());
                std::this_thread::sleep_for(std::chrono::seconds(wait_time));

                break;
        }
    }
}

String StorageAlert::getStatus() const
{
    switch (pipeline_stage.load())
    {
        case Stage::Unknown:
            return "unknown";
        case Stage::Build:
            return "initializing";
        case Stage::Execute:
            return "normal";
        case Stage::Error:
            return "error";
        case Stage::Failed:
            return "failed";
        case Stage::Stopped:
            return "stopped";
    }
}

void StorageAlert::checkDependencies()
{
    LOG_INFO(logger, "Checking dependencies");

    /// Check source storages
    auto context = getContext();
    chassert(select_query.select_table_ids.size() == 1);
    const auto & select_table_id = select_query.select_table_ids.at(0);
    auto source_storage = DatabaseCatalog::instance().getTable(select_table_id, context);
    if (!source_storage->isReady())
        throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "{} was not ready", source_storage->getStorageID().getFullTableName());
}

void StorageAlert::tryBuildBackgroundPipeline()
{
    try
    {
        if (wait_for_dependencies)
            checkDependencies();
        buildBackgroundPipeline();
    }
    catch (Exception & e)
    {
        e.addMessage("Failed to build query pipeline");
        pipeline_stage.store(Stage::Error);

        setPipelineExeception(std::move(e));
        return;
    }
    catch (...)
    {
        pipeline_stage.store(Stage::Error);

        std::lock_guard lock{pipeline_exception_mutux};
        setPipelineExeception(
            Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to build query pipeline, error={}", getCurrentExceptionMessage(false)));
        pipeline_exception_ts = localNowMilliseconds();
        return;
    }
}

void StorageAlert::buildBackgroundPipeline()
{
#if USE_PYTHON_UDF
    LOG_INFO(logger, "Building pipeline");
    chassert(pipeline_stage == Stage::Build);

    auto call_fn = std::make_shared<TableFunctionPythonCall>();
    auto udf_name = std::make_shared<ASTLiteral>(desc->function_name);
    auto fn_ast = makeASTFunction("call", udf_name);
    call_fn->parseArguments(fn_ast, query_context);
    storage_udf = call_fn->execute(fn_ast, query_context, "call");

    auto inner_query = select_query.inner_query->clone();
    auto io_ = boost::make_shared<BlockIO>();
    io_->process_list_entry = query_context->getProcessList().insert(
        serializeAST(*inner_query), inner_query.get(), query_context, Stopwatch{CLOCK_MONOTONIC}.getStart());

    io_->pipeline = buildQueryPipelineImpl(inner_query);
    pipeline_sources = io_->pipeline.getStreamingSources();

    last_trigger.sns.reserve(pipeline_sources.size());
    recoverFromCheckpoints();

    /// Other threads may call `shutdown` to cancel the pipeline execution (via `process_list_entry->cancelQuery(true)`)
    chassert(io_ && io_->process_list_entry && io_->pipeline.initialized());
    io.store(io_);

    pipeline_stage = Stage::Execute;
#endif
}

void StorageAlert::recoverFromCheckpoints()
{
    auto sns = recover();
    if (sns.empty())
    {
        LOG_INFO(logger, "No checkpoints found");
    }
    else
    {
        if (sns.size() != pipeline_sources.size())
            LOG_INFO(
                logger,
                "The number of checkpoints found ({}) did not match the number of sources in the pipeline ({})",
                sns.size(),
                pipeline_sources.size());

        for (const auto & source : pipeline_sources)
        {
            auto shard = source->getStream();
            if (sns.contains(shard))
            {
                auto sn = sns.at(shard);
                source->resetStartSN(sn + 1);
                LOG_INFO(logger, "Recovered checkpoint, shard={} sn={}", shard, sn);
            }
            else
            {
                LOG_INFO(logger, "Checkpoint for shard {} did not exist", shard);
            }
        }
    }
}

QueryPipeline StorageAlert::buildQueryPipelineImpl(ASTPtr inner_query)
{
    /// NOTE: Here we build two stages `select` + `insert` instead of directly building `insert select`,
    /// so that the pipeline can be flexibly adjusted
    QueryPipelineBuilder pipeline_builder;
    {
        InterpreterSelectQuery select_interpreter(inner_query, query_context, SelectQueryOptions{});

        /// [Pipeline]: `Source` -> `Converting` -> `Materializing const` -> `target_table`
        pipeline_builder = select_interpreter.buildQueryPipeline();
        pipeline_builder.dropTotalsAndExtremes();
    }

    const auto & source_header = pipeline_builder.getHeader();

    Block target_header;
    Names insert_columns;
    target_header.reserve(source_header.columns());
    insert_columns.reserve(source_header.columns());

    auto target_metadata_snapshot = storage_udf->getInMemoryMetadataPtr();

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
        else
        {
            LOG_WARNING(
                logger,
                "Column '{}' in select output was not found in udf '{}' arguments, will be skipped",
                source_column.name,
                desc->function_name);
        }
    }

    if (target_header.columns() == 0)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "No matching columns found between select outputs and udf '{}' arguments, select output: [{}], udf arguments: [{}]",
            desc->function_name,
            source_header.dumpNames(),
            target_storage_header.dumpNames());

    /// It's rare that alerts require paralle UDF fucntion calls, we can support it when there is a real use case.
    size_t out_streams_size = 1;
    pipeline_builder.resize(out_streams_size);

    if (!blocksHaveEqualStructure(source_header, target_header))
    {
        /// Use match by name since the `select` output header may not match the target stream's schema
        auto converting = ActionsDAG::makeConvertingActions(
            source_header.getColumnsWithTypeAndName(), target_header.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name);
        auto actions = std::make_shared<ExpressionActions>(
            std::move(converting), ExpressionActionsSettings::fromContext(query_context, CompileExpressions::yes));

        pipeline_builder.addSimpleTransform(
            [&](const Block & header) -> ProcessorPtr { return std::make_shared<ExpressionTransform>(header, actions); });
    }

    /// Materializing const columns
    pipeline_builder.addSimpleTransform(
        [&](const Block & header) -> ProcessorPtr { return std::make_shared<MaterializingTransform>(header); });

    if (desc->batch.count > 0)
        pipeline_builder.addTransform(std::make_shared<BatchingTransform>(
            target_header, desc->batch.count, desc->batch.interval * desc->batch.interval_kind.toSeconds() * 1000));

    if (desc->limit.count > 0)
        pipeline_builder.addTransform(std::make_shared<ThrottlingTransform>(
            target_header, desc->limit.count, desc->limit.interval * desc->limit.interval_kind.toSeconds() * 1000, [this]() {
                last_throttled_ts = localNowMilliseconds();
                metrics.throttle_count += 1;

                checkpoint();
            }));

    /// Sink to target table
    std::vector<Chain> out_chains;
    out_chains.reserve(out_streams_size);
    for (size_t i = 0; i < out_streams_size; ++i)
    {
        InterpreterInsertQuery interpreter(nullptr, query_context, false, false, false);
        /// FIXME, thread status
        auto out_chain = interpreter.buildChain(
            storage_udf, target_metadata_snapshot, insert_columns, nullptr, nullptr, pipeline_builder.isStreaming());

        auto & sink = dynamic_cast<SinkToStorage &>(out_chain.getSink());
        sink.setProgressCallback([this](const auto & progress) {
            auto rows = progress.written_rows.load();
            if (rows < 1)
                return;

            metrics.trigger_count += 1;
            {
                std::lock_guard guard{trigger_mutex};
                last_trigger.timestamp = localNowMilliseconds();
                last_trigger.rows = rows;
            }

            checkpoint();
        });
        out_chains.emplace_back(std::move(out_chain));
    }

    QueryPlanResourceHolder resources;
    for (auto & out_chain : out_chains)
        resources = out_chain.detachResources();

    pipeline_builder.addChains(std::move(out_chains));

    pipeline_builder.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
        return std::make_shared<EmptySink>(cur_header);
    });

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    pipeline.addResources(std::move(resources));
    pipeline.addStorageHolder(storage_udf);
    pipeline.setProgressCallback(query_context->getProgressCallback());
    pipeline.setProcessListElement(query_context->getProcessListElement());
    return pipeline;
}

void StorageAlert::tryExecuteBackgroundPipeline()
{
    try
    {
        executeBackgroundPipeline();
    }
    catch (Exception & e)
    {
        e.addMessage("Failed to execute query pipeline");
        setPipelineExeception(std::move(e));
        pipeline_stage = Stage::Error;
        return;
    }
    catch (...)
    {
        setPipelineExeception(Exception(ErrorCodes::UNKNOWN_EXCEPTION, "{}", getCurrentExceptionMessage(true)));
        pipeline_stage = Stage::Error;
        return;
    }

    pipeline_stage = Stage::Stopped;
}

void StorageAlert::executeBackgroundPipeline()
{
    LOG_INFO(logger, "Executing pipeline with query_id={}", query_id);
    chassert(pipeline_stage == Stage::Execute);

    auto pipeline_io = io.load();
    chassert(pipeline_io);

    CompletedPipelineExecutor pipeline_executor(pipeline_io->pipeline);
    pipeline_executor.setCancelCallback(
        [this]() {
            /// TODO handle leader change
            return shut.test();
        },
        internal_recheck_interval_ms);
    pipeline_executor.execute();
}

void StorageAlert::shutdown(bool /*dropping*/)
{
    if (shut.test_and_set() || !started.test())
        return;

    LOG_INFO(logger, "Shutting down");

    auto storage_id = getStorageID();
    for (const auto & select_table_id : select_query.select_table_ids)
        DatabaseCatalog::instance().removeDependency(select_table_id, storage_id);

    while (!stopped.test())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    io.store(nullptr);
    query_context.reset();

    LOG_INFO(logger, "Shut down");
}

namespace
{
void saveCheckpoint(const UUID & uuid, const StorageAlert::ShardSNs & sns, const ContextPtr & context)
{
    if (sns.empty())
        return;

    WriteBufferFromOwnString buf;
    writeString("INSERT INTO system.alert_ckpt_log (uuid, shard, sn) VALUES ", buf);
    for (const auto [shard, sn] : sns)
    {
        writeChar('(', buf);

        writeChar('\'', buf);
        writeUUIDText(uuid, buf);
        writeChar('\'', buf);
        writeChar(',', buf);

        writeIntText(shard, buf);
        writeChar(',', buf);

        writeIntText(sn, buf);

        writeChar(')', buf);
    }

    const auto & insert_stat = buf.str();
    ParserInsertQuery parser{insert_stat.data() + insert_stat.size(), false};
    auto insert_ast = parseQuery(parser, insert_stat, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    InterpreterInsertQuery q{insert_ast, context};
    auto io = q.execute();
    CompletedPipelineExecutor pipeline_executor(io.pipeline);
    pipeline_executor.execute();
}
}

void StorageAlert::markSNsWithoutLock()
{
    for (const auto & source : pipeline_sources)
    {
        if (auto sn = source->lastProcessedSN(); sn >= 0)
            last_trigger.sns[source->getStream()] = sn;
    }
}

void StorageAlert::checkpoint() noexcept
{
    std::lock_guard lock{trigger_mutex};

    markSNsWithoutLock();

    if (last_trigger.sns.empty())
        return;

    try
    {
        saveCheckpoint(getStorageID().uuid, last_trigger.sns, query_context);
        LOG_INFO(logger, "Checkpoint saved sn={}", last_trigger.sns.toString());
    }
    catch (...)
    {
        LOG_ERROR(logger, "Failed to save checkpoint for alert, error={}", getCurrentExceptionMessage(true));
    }
}

StorageAlert::ShardSNs StorageAlert::recover() noexcept
{
    static auto recover_query
        = fmt::format("SELECT shard, sn FROM table(system.alert_ckpt_log) WHERE uuid = '{}'", toString(getStorageID().uuid));
    auto local_context = Context::createCopy(query_context);
    try
    {
        StorageAlert::ShardSNs sns;
        executeNonInsertQuery(recover_query, local_context, [&sns](Block && block) {
            const auto & shard_col = block.findByName("shard")->column;
            const auto & sn_col = block.findByName("sn")->column;

            auto rows = block.rows();
            sns.reserve(rows);
            for (size_t i = 0; i < rows; ++i)
                sns.emplace(shard_col->getUInt(i), sn_col->getInt(i));
        });
        return sns;
    }
    catch (...)
    {
        LOG_ERROR(
            logger,
            "Failed to restore checkpoints, alert will start without recovering from checkpoints, error={}",
            getCurrentExceptionMessage(true));
        return {};
    }
}

void StorageAlert::setPipelineExeception(Exception && ex)
{
    std::lock_guard lock{pipeline_exception_mutux};
    pipeline_exception = std::move(ex);
    pipeline_exception_ts = localNowMilliseconds();
}

StorageAlert::TriggerRecord StorageAlert::getLastTrigger()
{
    std::lock_guard guard{trigger_mutex};
    return last_trigger;
}

std::pair<Int64, String> StorageAlert::getLastError()
{
    std::lock_guard lock{pipeline_exception_mutux};

    if (pipeline_exception)
        return {pipeline_exception_ts, pipeline_exception->message()};

    return {-1, ""};
}

Int64 StorageAlert::getLastThrottleTimestamp() const
{
    return last_throttled_ts.load();
}

const StorageAlert::Metrics & StorageAlert::getMetrics() const
{
    return metrics;
}

}
