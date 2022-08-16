#include "queryStreams.h"

#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>

namespace DB
{
void queryStreams(ContextMutablePtr query_context, const std::function<void(Block &&)> & callback)
{
    /// Default max_block_size is 65505 (rows) which shall be bigger enough for a block to contain
    /// all tables on a single node

    /// We include "system.tables" and / or "system.tasks" tables in the resulting block on purpose .
    /// It is to avoid an empty block if there are no production tables in the system, which will cause
    /// consistency problem in CatalogService (like the last deleted table does not get deleted from CatalogService)
    String cols = "database, name, engine, uuid, dependencies_table, create_table_query, engine_full, partition_key, sorting_key, "
                  "primary_key, sampling_key, storage_policy";
    String query = fmt::format(
        "SELECT {} FROM system.tables WHERE NOT is_temporary AND ((database != 'system') OR (database = 'system' AND (name = 'tables' OR "
        "name = 'tasks'))) settings show_table_uuid_in_table_create_query_if_not_nil=1",
        cols);

    executeNonInsertQuery(query, query_context, callback);
}
}
