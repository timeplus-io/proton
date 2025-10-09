#include <Server/RestRouterHandlers/queryStreams.h>

#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>
#include <Common/quoteString.h>

namespace DB
{
void queryStreams(ContextMutablePtr query_context, const std::function<void(Block &&)> & callback)
{
    /// Default max_block_size is 65505 (rows) which shall be bigger enough for a block to contain
    /// all tables on a single node

    /// We include "system.tables" and / or "system.tasks" tables in the resulting block on purpose .
    /// It is to avoid an empty block if there are no production tables in the system, which will cause
    /// consistency problem in CatalogService (like the last deleted table does not get deleted from CatalogService)
    String cols = "database, name, engine, mode, uuid, dependencies_table, create_table_query, engine_full, partition_key, sorting_key, "
                  "primary_key, sampling_key, storage_policy";
    String query = fmt::format(
        "SELECT {} FROM system.tables WHERE NOT is_temporary AND ((database != 'system' AND database != 'INFORMATION_SCHEMA' AND database "
        "!= 'information_schema') OR (database = 'system' AND (name = 'tasks' OR name = 'tables'))) settings "
        "show_uuid = 1, _tp_internal_system_open_sesame=true",
        cols);

    executeNonInsertQuery(query, query_context, callback);
}

void queryOneStream(
    ContextMutablePtr query_context, const String & database_name, const String & name, const std::function<void(Block &&)> & callback)
{
    String cols = "database, name, engine, mode, uuid, dependencies_table, create_table_query, engine_full, partition_key, sorting_key, "
                  "primary_key, sampling_key, storage_policy";
    /// Use properly quoted string literals to avoid SQL syntax errors when names contain quotes
    String query = fmt::format(
        "SELECT {} FROM system.tables WHERE database = {} AND name = {} AND engine != 'Dictionary' settings "
        "show_uuid = 1, _tp_internal_system_open_sesame=true",
        cols,
        quoteString(database_name),
        quoteString(name));

    executeNonInsertQuery(query, query_context, callback);
}

void queryStreamsByDatabase(ContextMutablePtr query_context, const String & database_name, const std::function<void(Block &&)> & callback)
{
    String cols = "database, name, engine, mode, uuid, dependencies_table, create_table_query, engine_full, partition_key, sorting_key, "
                  "primary_key, sampling_key, storage_policy";

    /// Use properly quoted string literal for database name
    String query = fmt::format(
        "SELECT {} FROM system.tables WHERE database = {} AND engine != 'Dictionary' settings "
        "show_uuid = 1, _tp_internal_system_open_sesame=true",
        cols,
        quoteString(database_name));

    executeNonInsertQuery(query, query_context, callback);
}
}
