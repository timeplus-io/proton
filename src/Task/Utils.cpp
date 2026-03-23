#include <Task/Utils.h>

#include <Bootstrap/Globals.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTCreateTaskQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Task/TaskScheduler.h>

#include <ranges>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_QUERY;
extern const int UNKNOWN_STREAM;
}

namespace Task
{

std::string getTaskQuery(const std::string & sql, const Checkpoint & checkpoint)
{
    auto complete_query = sql;

    /// Fill query with checkpoint values
    for (const auto & [k, v] : checkpoint)
    {
        auto token = fmt::format("${{{}}}", k);
        size_t start_pos = 0;
        while ((start_pos = sql.find(token, start_pos)) != std::string::npos)
        {
            complete_query.replace(start_pos, token.length(), v);
            start_pos += v.length();
        }
    }

    return complete_query;
}

std::vector<TaskInfoPtr> getTaskInfos(const std::string & ns)
{
    const auto task_scheduler = Globals::getGlobalContext().getTaskScheduler();
    chassert(task_scheduler);
    return task_scheduler->getTasks(ns);
}

void validateCreateTaskQuery(const ASTCreateTaskQuery * create_task_query, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    /// Expand the SQL with checkpoint initial values
    Task::Checkpoint checkpoint;
    if (!create_task_query->checkpoint.empty())
    {
        auto maybe_checkpoint = Task::parseCheckpoint(create_task_query->checkpoint);
        if (maybe_checkpoint.hasError())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Failed to parse checkpoint initial values: {}",
                maybe_checkpoint.err.string());
        }
        checkpoint = std::move(maybe_checkpoint.result);
    }
    const auto expanded_sql = Task::getTaskQuery(create_task_query->sql, checkpoint);

    /// 1. Parse and validate the query syntax
    ParserQuery parser(expanded_sql.data() + expanded_sql.size());
    ASTPtr select_ast;
    try
    {
        select_ast = parseQuery(parser, expanded_sql.data(), expanded_sql.data() + expanded_sql.size(), "", settings.max_query_size, settings.max_parser_depth);
    }
    catch (Exception & e)
    {
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Task query is invalid: query='{}' error='{}'", expanded_sql, e.displayText());
    }

    if (select_ast->as<ASTSelectWithUnionQuery>() == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Task query must be a SELECT query");

    /// 2. Validate target stream exists and schema compatibility if INTO is specified
    if (create_task_query->to_table_id)
    {
        String target_database;
        if (!create_task_query->to_table_id.database_name.empty())
            target_database = create_task_query->to_table_id.database_name;
        else
            target_database = context->getCurrentDatabase();

        const String & target_table_name = create_task_query->to_table_id.table_name;
        StorageID target_table_id{target_database, target_table_name};

        auto target_table = DatabaseCatalog::instance().tryGetTable(target_table_id, context);
        if (!target_table)
            throw Exception(
                ErrorCodes::UNKNOWN_STREAM, "Target stream {}.{} does not exist", target_database, target_table_name);

        /// 3. Validate schema compatibility
        Block source_header = InterpreterSelectWithUnionQuery::getSampleBlock(select_ast, context, false);
        auto target_metadata_snapshot = target_table->getInMemoryMetadataPtr();
        const auto & target_table_columns = target_metadata_snapshot->getColumns();
        auto target_storage_header = target_metadata_snapshot->getSampleBlock();

        /// Build target header with columns that match by name
        Block target_header;
        Block source_header_matched;
        for (const auto & source_column : source_header)
        {
            if (target_table_columns.hasPhysical(source_column.name))
            {
                target_header.insert(target_storage_header.getByName(source_column.name));
                source_header_matched.insert(source_column);
            }
        }

        if (target_header.columns() == 0)
        {
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "No matching columns found between select outputs and target stream '{}', "
                "select output: [{}], target header: [{}]",
                target_table->getStorageID().getFullTableName(),
                source_header.dumpNames(),
                target_storage_header.dumpNames());
        }

        /// Check type compatibility by trying to create converting actions
        try
        {
            ActionsDAG::makeConvertingActions(
                source_header_matched.getColumnsWithTypeAndName(),
                target_header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);
        }
        catch (const Exception & e)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Column types are not compatible between select outputs and target stream '{}'. "
                "select output: [{}], target header: [{}], error: {}",
                target_table->getStorageID().getFullTableName(),
                source_header_matched.dumpStructure(),
                target_header.dumpStructure(),
                e.displayText());
        }
    }

}

}
}
