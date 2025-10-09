#include <Task/TaskExecution.h>

#include <Access/Common/AccessFlags.h>
#include <Bootstrap/Globals.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/executeSelectQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Task/TaskScheduler.h>
#include <Task/Utils.h>
#include <base/sleep.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_HISTORY;
extern const int INCORRECT_QUERY;
extern const int UNKNOWN_EXCEPTION;
}

namespace Task
{
struct CheckpointWithMutex
{
    Checkpoint checkpoint;
    std::mutex mutex;
};

class CheckPointRecordTransform final : public ExceptionKeepingTransform
{
public:
    CheckPointRecordTransform(
        const Block & header, std::shared_ptr<CheckpointWithMutex> checkpoint_, std::vector<std::string> checkpoint_names_)
        : ExceptionKeepingTransform(header, header, true, ProcessorID::CheckPointRecordTransformID)
        , checkpoint(std::move(checkpoint_))
        , checkpoint_names(std::move(checkpoint_names_))
    {
        checkpoint_pos.reserve(checkpoint_names.size());
        for (const auto & name : checkpoint_names)
            checkpoint_pos.push_back(header.getPositionByName(name));
    }

    String getName() const override { return "CheckPointRecordTransform"; }

    void onConsume(Chunk chunk) override
    {
        auto rows = chunk.rows();
        if (rows > 0)
        {
            const auto & columns = chunk.getColumns();

            std::scoped_lock lk{checkpoint->mutex};
            for (size_t i = 0; i < checkpoint_names.size(); ++i)
            {
                Field field;
                columns[checkpoint_pos[i]]->get(rows - 1, field);
                checkpoint->checkpoint.emplace(checkpoint_names[i], toString(field));
            }
        }

        cur_chunk = std::move(chunk);
    }

    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        return res;
    }

protected:
    std::shared_ptr<CheckpointWithMutex> checkpoint;
    std::vector<std::string> checkpoint_names;
    std::vector<size_t> checkpoint_pos;

    Chunk cur_chunk;
};

std::optional<std::string> TaskExecution::getQuery(const cluster::protocol::TaskDescriptor & task, TaskExecutionState & state)
{
    auto maybe_loaded_state = loadTaskExecutionState(task.id, task.data_version, task.checkpoint_init_values);
    if (maybe_loaded_state.hasError())
    {
        LOG_ERROR(
            logger, "Failed to load task execution state: ns={} name={} error={{{}}}", task.ns, task.name, maybe_loaded_state.err.string());
        state.error.error_code = ErrorCodes::CANNOT_READ_HISTORY;
        state.error.error_message = fmt::format("Failed to load task execution state");
        return std::nullopt;
    }

    state = std::move(maybe_loaded_state.result);

    auto query = getTaskCompleteQuery(task.sql, state.checkpoint);
    LOG_DEBUG(logger, "Task execution query: {}", query);

    return query;
}

void TaskExecution::executeQuery(
    const std::string & query, const ContextPtr & query_context, const cluster::protocol::TaskDescriptor & task, TaskExecutionState & state)
{
    const auto & settings = query_context->getSettingsRef();

    state.error = cluster::Error();
    try
    {
        ParserQuery parser(query.data() + query.size());
        auto select_ast
            = parseQuery(parser, query.data(), query.data() + query.size(), "", settings.max_query_size, settings.max_parser_depth);
        if (const auto * select = select_ast->as<ASTSelectWithUnionQuery>(); select == nullptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only support to execute SELECT query");

        QueryPipelineBuilder pipeline_builder;
        InterpreterSelectWithUnionQuery select_interpreter(select_ast, query_context, SelectQueryOptions());
        pipeline_builder = select_interpreter.buildQueryPipeline();
        pipeline_builder.dropTotalsAndExtremes();

        auto checkpoint = std::make_shared<CheckpointWithMutex>();
        QueryPipeline pipeline;

        bool has_target_table = !task.target_table_name.empty();
        if (has_target_table)
        {
            const auto & source_header = pipeline_builder.getHeader();
            Block target_header;
            Names insert_columns;
            target_header.reserve(source_header.columns());
            insert_columns.reserve(source_header.columns());

            StorageID target_table_id{task.target_database_name, task.target_table_name};
            auto target_table = DatabaseCatalog::instance().getTable(target_table_id, query_context);
            auto target_metadata_snapshot = target_table->getInMemoryMetadataPtr();

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

            if (target_header.columns() == 0)
            {
                state.error.error_code = ErrorCodes::INCORRECT_QUERY;
                state.error.error_message = fmt::format(
                    "No matching columns found between select outputs and target table '{}', select output: [{}], target header: [{}]",
                    target_table->getStorageID().getFullTableName(),
                    source_header.dumpNames(),
                    target_storage_header.dumpNames());
                return;
            }

            query_context->checkAccess(AccessType::INSERT, target_table_id, target_header.getNames());

            size_t out_streams_size = 1;
            if (target_table->supportsParallelInsert() && settings.max_insert_threads > 1)
                out_streams_size = std::min<size_t>(settings.max_insert_threads, pipeline_builder.getNumStreams());

            pipeline_builder.resize(out_streams_size);

            if (!blocksHaveEqualStructure(source_header, target_header))
            {
                /// Use match by name since the `select` output header may not match the target stream's schema
                auto converting = ActionsDAG::makeConvertingActions(
                    source_header.getColumnsWithTypeAndName(),
                    target_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Name);
                auto actions = std::make_shared<ExpressionActions>(
                    std::move(converting), ExpressionActionsSettings::fromContext(query_context, CompileExpressions::yes));

                pipeline_builder.addSimpleTransform(
                    [&](const Block & header) -> ProcessorPtr { return std::make_shared<ExpressionTransform>(header, actions); });
            }

            /// Record checkpoint
            pipeline_builder.addSimpleTransform([&checkpoint, &task](const Block & header) -> ProcessorPtr {
                return std::make_shared<CheckPointRecordTransform>(header, checkpoint, task.checkpoint_columns);
            });

            /// Sink to target table
            std::vector<Chain> out_chains;
            out_chains.reserve(out_streams_size);
            for (size_t i = 0; i < out_streams_size; ++i)
            {
                InterpreterInsertQuery interpreter(nullptr, query_context, false, false, false);
                auto out_chain = interpreter.buildChain(
                    target_table, target_metadata_snapshot, insert_columns, nullptr, nullptr, pipeline_builder.isStreaming());
                out_chains.emplace_back(std::move(out_chain));
            }

            QueryPlanResourceHolder resources;
            for (auto & out_chain : out_chains)
                resources = out_chain.detachResources();

            pipeline_builder.addChains(std::move(out_chains));

            pipeline_builder.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
                return std::make_shared<EmptySink>(cur_header);
            });

            pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
            pipeline.addResources(std::move(resources));
            pipeline.addStorageHolder(target_table);
        }
        else
        {
            /// Record checkpoint
            pipeline_builder.addSimpleTransform([&checkpoint, &task](const Block & header) -> ProcessorPtr {
                return std::make_shared<CheckPointRecordTransform>(header, checkpoint, task.checkpoint_columns);
            });

            pipeline_builder.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
                return std::make_shared<EmptySink>(cur_header);
            });

            pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
        }

        pipeline.setProgressCallback(query_context->getProgressCallback());
        pipeline.setProcessListElement(query_context->getProcessListElement());

        CompletedPipelineExecutor executor{pipeline};

        const auto timeout_ms = task.timeout * task.timeout_unit.toAvgSeconds() * 1000;
        const auto now = MonotonicMilliseconds::now();
        executor.setCancelCallback(
            [now, timeout_ms]() { return MonotonicMilliseconds::now() - now >= static_cast<Int64>(timeout_ms); },
            std::min(timeout_ms, static_cast<uint64_t>(10000)));

        executor.execute();

        /// Update checkpoint when there is some data from select-query
        if (!checkpoint->checkpoint.empty())
            state.checkpoint = std::move(checkpoint->checkpoint);
    }
    catch (const Exception & ex)
    {
        state.error.error_code = ex.code();
        state.error.error_message = ex.displayText();
    }
    catch (...)
    {
        state.error.error_code = ErrorCodes::UNKNOWN_EXCEPTION;
        state.error.error_message = getCurrentExceptionMessage(false);
    }
}

/// Should not throw exception in execution(); otherwise task scheduling may terminate.
void TaskExecution::execute()
{
    auto task_descriptor = task_scheduler->getTaskDescriptor(task_id);
    if (task_descriptor == nullptr || task_descriptor->data_version != data_version)
    {
        LOG_INFO(logger, "Skip executing removed or out-dated task: id={}", task_id);
        /// Update scheduled task counter.
        task_scheduler->onTaskExecutionComplete(task_descriptor, std::nullopt);
        return;
    }

    if (task_descriptor->status == 1)
    {
        LOG_INFO(logger, "Skip executing disabled task: task={}.{}", task_descriptor->ns, task_descriptor->name);
        /// Update scheduled task counter.
        task_scheduler->onTaskExecutionComplete(task_descriptor, std::nullopt);
        return;
    }

    /// Make sure execution starts after next run time since time wheel may be inaccurate
    auto now = UTCSeconds::now();
    if (now < next_run_sec)
        sleepForSeconds(next_run_sec - now);

    if (!task_scheduler->isExecutor())
    {
        task_scheduler->onTaskExecutionComplete(task_descriptor, std::nullopt);
        return;
    }

    TaskExecutionState state;

    auto maybe_query = getQuery(*task_descriptor, state);
    if (!maybe_query.has_value())
    {
        task_scheduler->onTaskExecutionComplete(task_descriptor, std::move(state));
        return;
    }

    state.execution_start = UTCSeconds::now();

    auto query_context = Context::createCopy(Globals::getGlobalContext().getGlobalContext());
    query_context->makeQueryContext();
    query_context->setUserByName(task_descriptor->created_by); /// Run as task creator

    executeQuery(maybe_query.value(), query_context, *task_descriptor, state);
    if (state.error.hasError())
        LOG_ERROR(logger, "Failed in executing task query: query='{}' error='{}'", maybe_query.value(), state.error.string());

    state.execution_end = UTCSeconds::now();

    task_scheduler->onTaskExecutionComplete(task_descriptor, std::move(state));
}
}
}
