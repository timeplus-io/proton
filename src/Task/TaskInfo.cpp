#include <Task/TaskInfo.h>

#include <Bootstrap/Globals.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/SelectQueryDescription.h>
#include <Task/Utils.h>


namespace DB::Task
{
TaskInfo::TaskInfo(cluster::protocol::TaskDescriptorPtr descriptor_) : descriptor(std::move(descriptor_))
{
    if (!descriptor->target_table_name.empty())
        target_table = StorageID{descriptor->target_database_name, descriptor->target_table_name};

    /// Get source tables
    auto maybe_checkpoint = parseCheckpoint(descriptor->checkpoint_init_values);
    if (!maybe_checkpoint.hasError())
    {
        auto source_query = getTaskQuery(descriptor->sql, maybe_checkpoint.result);
        ParserQuery parser(source_query.data() + source_query.size());
        auto select_ast = parseQuery(
            parser,
            source_query.data(),
            source_query.data() + source_query.size(),
            "",
            DBMS_DEFAULT_MAX_QUERY_SIZE,
            DBMS_DEFAULT_MAX_PARSER_DEPTH);

        if (const auto * select = select_ast->as<ASTSelectWithUnionQuery>(); select != nullptr)
        {
            auto description
                = SelectQueryDescription::getSelectQueryFromASTForView(select_ast, Globals::getGlobalContext().getGlobalContext());
            source_tables = std::move(description.select_table_ids);
        }
    }
}

}
