#include <Task/TaskExecution.h>

#include <Access/Common/AccessFlags.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Requests/GetTaskRequest.h>
#include <Dictionaries/HashedDictionaryParallelLoader.h>
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
#include <aws/crt/io/ChannelHandler.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_HISTORY;
extern const int INCORRECT_QUERY;
extern const int METADATA_VERSION_CHANGED;
extern const int TASK_IS_DISABLED;
extern const int UNKNOWN_EXCEPTION;
extern const int UNKNOWN_TASK;
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
            checkpoint_pos.push_back(header.tryGetPositionByName(name));
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
                if (checkpoint_pos[i].has_value())
                {
                    Field field;
                    columns[checkpoint_pos[i].value()]->get(rows - 1, field);
                    checkpoint->checkpoint.emplace(checkpoint_names[i], toString(field));
                }
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
    std::vector<std::optional<size_t>> checkpoint_pos;

    Chunk cur_chunk;
};

std::pair<std::string, Checkpoint> TaskExecution::getQueryAndCheckpoint()
{
    auto maybe_loaded_state
        = loadTaskExecutionResult(task_descriptor->id, task_descriptor->data_version, task_descriptor->checkpoint_init_values);
    if (maybe_loaded_state.hasError())
    {
        LOG_ERROR(
            logger,
            "Failed to load task execution state: ns={} name={} error={{{}}}",
            task_descriptor->ns,
            task_descriptor->name,
            maybe_loaded_state.err.string());

        throw Exception(
            maybe_loaded_state.err.error_code,
            "Failed to load task execution state: name={}.{}",
            task_descriptor->ns,
            task_descriptor->name);
    }

    auto query = getTaskQuery(task_descriptor->sql, maybe_loaded_state.result.checkpoint);
    LOG_DEBUG(logger, "Task execution query: {}", query);

    return {std::move(query), std::move(maybe_loaded_state.result.checkpoint)};
}

Checkpoint TaskExecution::executeQuery(const std::string & query, const ContextPtr & query_context)
{
    const auto & settings = query_context->getSettingsRef();
    ParserQuery parser(query.data() + query.size());
    auto select_ast = parseQuery(parser, query.data(), query.data() + query.size(), "", settings.max_query_size, settings.max_parser_depth);
    if (const auto * select = select_ast->as<ASTSelectWithUnionQuery>(); select == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only support to execute SELECT query");

    QueryPipelineBuilder pipeline_builder;
    InterpreterSelectWithUnionQuery select_interpreter(select_ast, query_context, SelectQueryOptions());
    pipeline_builder = select_interpreter.buildQueryPipeline();
    pipeline_builder.dropTotalsAndExtremes();

    auto checkpoint = std::make_shared<CheckpointWithMutex>();
    QueryPipeline pipeline;

    const bool has_target_table = !task_descriptor->target_table_name.empty();
    if (has_target_table)
    {
        const auto & source_header = pipeline_builder.getHeader();
        Block target_header;
        Names insert_columns;
        target_header.reserve(source_header.columns());
        insert_columns.reserve(source_header.columns());

        StorageID target_table_id{task_descriptor->target_database_name, task_descriptor->target_table_name};
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
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "No matching columns found between select outputs and target table '{}', select output: [{}], target header: [{}]",
                target_table->getStorageID().getFullTableName(),
                source_header.dumpNames(),
                target_storage_header.dumpNames());
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
                source_header.getColumnsWithTypeAndName(), target_header.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name);
            auto actions = std::make_shared<ExpressionActions>(
                std::move(converting), ExpressionActionsSettings::fromContext(query_context, CompileExpressions::yes));

            pipeline_builder.addSimpleTransform(
                [&](const Block & header) -> ProcessorPtr { return std::make_shared<ExpressionTransform>(header, actions); });
        }

        /// Record checkpoint
        pipeline_builder.addSimpleTransform([this, &checkpoint](const Block & header) -> ProcessorPtr {
            return std::make_shared<CheckPointRecordTransform>(header, checkpoint, task_descriptor->checkpoint_columns);
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
        pipeline_builder.addSimpleTransform([this, &checkpoint](const Block & header) -> ProcessorPtr {
            return std::make_shared<CheckPointRecordTransform>(header, checkpoint, task_descriptor->checkpoint_columns);
        });

        pipeline_builder.setSinks([&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr {
            return std::make_shared<EmptySink>(cur_header);
        });

        pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
    }

    pipeline.setProgressCallback(query_context->getProgressCallback());
    pipeline.setProcessListElement(query_context->getProcessListElement());

    CompletedPipelineExecutor executor{pipeline};

    const auto timeout_ms = task_descriptor->getTimeoutMS();
    const auto now = MonotonicMilliseconds::now();
    executor.setCancelCallback(
        [now, timeout_ms]() { return MonotonicMilliseconds::now() - now >= static_cast<Int64>(timeout_ms); },
        std::min(timeout_ms, static_cast<uint64_t>(10000)));

    executor.execute();

    return checkpoint->checkpoint;
}

void TaskExecution::getAndCheckTaskDescriptor(std::optional<uint32_t> data_version)
{
    auto & meta_store = Globals::getMetaStore();
    auto req = std::make_shared<cluster::GetTaskRequest>(
        task_id.database_name,
        task_id.table_name,
        /*versions_requested=*/1,
        meta_store.nodeID(),
        /*consistent_read=*/false,
        /*timeout_ms=*/5000,
        /*request_version=*/1);

    auto resp = meta_store.getTask(std::move(req));
    if (resp->hasError() || resp->data().descs.empty())
    {
        throw Exception(
            ErrorCodes::UNKNOWN_TASK, "Failed to load task: name={}, error={{{}}}", task_id.getFullTableName(), resp->error().string());
    }

    task_descriptor = std::move(resp->data().descs[0]);
    if (task_descriptor == nullptr || (task_id.hasUUID() && task_id.uuid != task_descriptor->id)
        || (data_version.has_value() && task_descriptor->data_version != data_version.value()))
    {
        throw Exception(ErrorCodes::METADATA_VERSION_CHANGED, "Task is removed or updated: name={}", task_id.getFullTableName());
    }
    task_id.uuid = task_descriptor->id;

    if (task_descriptor->status == cluster::protocol::TaskStatus::Disabled)
    {
        throw Exception(ErrorCodes::TASK_IS_DISABLED, "Task is disabled: name={}", task_id.getFullTableName());
    }
}

TaskExecutionResult TaskExecution::execute(const ContextPtr & context)
{
    TaskExecutionResult result;
    result.execution_node = Globals::getNodeID();
    result.execution_start = UTCMilliseconds::now();

    try
    {
        auto [query, checkpoint] = getQueryAndCheckpoint();
        result.checkpoint = std::move(checkpoint);
        auto new_checkpoint = executeQuery(query, context);
        for (auto & [k, v] : new_checkpoint)
            result.checkpoint.insert_or_assign(k, std::move(v));
    }
    catch (const Exception & ex)
    {
        result.error.error_code = ex.code();
        result.error.error_message = ex.displayText();
    }
    catch (...)
    {
        result.error.error_code = ErrorCodes::UNKNOWN_EXCEPTION;
        result.error.error_message = getCurrentExceptionMessage(false);
    }

    result.execution_end = UTCMilliseconds::now();

    if (!result.error.hasError() && result.execution_end - result.execution_start > static_cast<Int64>(task_descriptor->getTimeoutMS()))
    {
        result.error.error_code = ErrorCodes::TIMEOUT_EXCEEDED;
        result.error.error_message = "Task execution timeout.";
    }

    const auto save_res = saveTaskExecutionResult(task_id, task_descriptor->data_version, result);
    if (save_res != ErrorCodes::OK)
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to save task execution state: {}", ErrorCodes::getName(save_res));

    return result;
}
}
}
