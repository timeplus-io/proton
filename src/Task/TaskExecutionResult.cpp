#include <Task/TaskExecutionResult.h>

#include <Bootstrap/Globals.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/executeSelectQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>

#include <fmt/format.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int OK;
extern const int UNKNOWN_EXCEPTION;
}

namespace Task
{

cluster::CallResultV<Checkpoint> parseCheckpoint(const std::string & checkpoint_str)
{
    using ReturnType = cluster::CallResultV<Checkpoint>;

    if (checkpoint_str.empty())
        return ReturnType(Checkpoint{});

    try
    {
        Poco::JSON::Parser parser;
        auto obj = parser.parse(checkpoint_str).extract<Poco::JSON::Object::Ptr>();
        if (!obj)
            return ReturnType{cluster::Error{ErrorCodes::INCORRECT_DATA, "Checkpoint string is not valid JSON."}};

        Checkpoint checkpoint;
        auto keys = obj->getNames();
        for (const auto & key : keys)
            checkpoint.emplace(key, obj->get(key).toString());

        return ReturnType{std::move(checkpoint)};
    }
    catch (const Exception & ex)
    {
        return ReturnType{cluster::Error{ex.code(), ex.displayText()}};
    }
    catch (...)
    {
        return ReturnType{cluster::Error{ErrorCodes::UNKNOWN_EXCEPTION, "Failed in parsing Checkpoint string."}};
    }
}

int saveTaskExecutionResult(StorageID id, uint32_t version, const TaskExecutionResult & result)
{
    const auto & checkpoint = result.checkpoint;

    Poco::JSON::Object::Ptr obj = new Poco::JSON::Object();
    for (const auto & [key, value] : checkpoint)
        obj->set(key, value);

    std::stringstream ss;
    Poco::JSON::Stringifier::stringify(obj, ss);

    WriteBufferFromOwnString buf;
    writeString(
        "INSERT INTO system.task_execution_state (database, name, uuid, version, checkpoint, last_execution_node, last_execution_start, "
        "last_execution_end, "
        "last_execution_result) VALUES ",
        buf);

    writeChar('(', buf);
    writeQuotedString(id.database_name, buf);
    writeChar(',', buf);

    writeQuotedString(id.table_name, buf);
    writeChar(',', buf);

    writeChar('\'', buf);
    writeUUIDText(id.uuid, buf);
    writeChar('\'', buf);
    writeChar(',', buf);

    writeIntText(version, buf);
    writeChar(',', buf);

    writeQuotedString(ss.str(), buf);
    writeChar(',', buf);

    writeIntText(result.execution_node, buf);
    writeChar(',', buf);

    writeIntText(result.execution_start, buf);
    writeChar(',', buf);

    writeIntText(result.execution_end, buf);
    writeChar(',', buf);

    writeQuotedString(result.displayError(), buf);
    writeChar(')', buf);

    buf.finalize();
    const auto & save_query = buf.str();

    LOG_DEBUG(getLogger("TaskExecutionState"), "Save task execution state: {}", save_query);

    try
    {
        auto query_context = Context::createCopy(Globals::getGlobalContext().getGlobalContext());
        query_context->makeQueryContext();

        ParserInsertQuery parser{save_query.data() + save_query.size(), false};
        auto insert_ast = parseQuery(parser, save_query, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        InterpreterInsertQuery q{insert_ast, query_context};
        auto io = q.execute();
        CompletedPipelineExecutor pipeline_executor(io.pipeline);
        pipeline_executor.execute();
    }
    catch (const Exception & ex)
    {
        return ex.code();
    }
    catch (...)
    {
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }

    return ErrorCodes::OK;
}

cluster::CallResultV<TaskExecutionResult>
loadTaskExecutionResult(UUID id, uint32_t data_version, const std::string & checkpoint_init_values)
{
    auto load_query = fmt::format(
        "SELECT checkpoint FROM table(system.task_execution_state) WHERE uuid = '{}' AND version = {}", toString(id), data_version);

    LOG_DEBUG(getLogger("TaskExecutionState"), "Load task execution state: {}", load_query);

    auto query_context = Context::createCopy(Globals::getGlobalContext().getGlobalContext());
    query_context->makeQueryContext();
    std::string ckpt_json_str;
    try
    {
        executeNonInsertQuery(load_query, query_context, [&ckpt_json_str](Block && block) {
            const auto & ckpt_col = block.findByName("checkpoint")->column;

            auto rows = block.rows();
            if (rows > 0)
                ckpt_json_str = ckpt_col->getDataAt(rows - 1).toString();
        });
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(getLogger("TaskExecutionState"), "Failed to query checkpoint: {}", ex.displayText());
        return cluster::CallResultV<TaskExecutionResult>{cluster::Error{ex.code(), ex.displayText()}};
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return cluster::CallResultV<TaskExecutionResult>{cluster::Error{ErrorCodes::UNKNOWN_EXCEPTION}};
    }

    if (ckpt_json_str.empty() || ckpt_json_str == "{}")
    {
        if (checkpoint_init_values.empty())
            return {};
        ckpt_json_str = checkpoint_init_values;
    }

    auto maybe_checkpoint = parseCheckpoint(ckpt_json_str);
    if (maybe_checkpoint.hasError())
    {
        LOG_ERROR(getLogger("TaskExecutionState"), "Failed to parse loaded checkpoint as JSON: checkpoint={}", ckpt_json_str);
        return cluster::CallResultV<TaskExecutionResult>{std::move(maybe_checkpoint.err)};
    }

    TaskExecutionResult state;
    state.checkpoint = std::move(maybe_checkpoint.result);
    return cluster::CallResultV<TaskExecutionResult>{std::move(state)};
}
}
}
