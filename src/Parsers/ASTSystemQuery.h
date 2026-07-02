#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST.h>

#include "config.h"

#include <vector>


namespace DB
{

class ASTSystemQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    enum class Type : UInt64
    {
        UNKNOWN,
        SHUTDOWN,
        KILL,
        SUSPEND,
        DROP_DNS_CACHE,
        DROP_CONNECTIONS_CACHE,
        DROP_MARK_CACHE,
        DROP_UNCOMPRESSED_CACHE,
        DROP_INDEX_MARK_CACHE,
        DROP_INDEX_UNCOMPRESSED_CACHE,
        DROP_MMAP_CACHE,
#if USE_EMBEDDED_COMPILER
        DROP_COMPILED_EXPRESSION_CACHE,
#endif
        DROP_FILESYSTEM_CACHE,
        DROP_SCHEMA_CACHE,
        DROP_FORMAT_SCHEMA_CACHE,
#if USE_AWS_S3
        DROP_S3_CLIENT_CACHE,
#endif
        STOP_LISTEN_QUERIES,
        START_LISTEN_QUERIES,
        RESTART_REPLICAS,
        RESTART_REPLICA,
        RESTORE_REPLICA,
        WAIT_LOADING_PARTS,
        DROP_REPLICA,
#if USE_JEMALLOC
        JEMALLOC_PURGE,
        JEMALLOC_ENABLE_PROFILE,
        JEMALLOC_DISABLE_PROFILE,
        JEMALLOC_FLUSH_PROFILE,
#endif
        SYNC_REPLICA,
        RELOAD_DICTIONARY,
        RELOAD_DICTIONARIES,
        RELOAD_MODEL,
        RELOAD_MODELS,
        RELOAD_FUNCTION,
        RELOAD_FUNCTIONS,
        RELOAD_EMBEDDED_DICTIONARIES,
        RELOAD_CONFIG,
        RESTART_DISK,
        STOP_MERGES,
        START_MERGES,
        STOP_TTL_MERGES,
        START_TTL_MERGES,
        STOP_FETCHES,
        START_FETCHES,
        STOP_MOVES,
        START_MOVES,
        STOP_REPLICATED_SENDS,
        START_REPLICATED_SENDS,
        STOP_REPLICATION_QUEUES,
        START_REPLICATION_QUEUES,
        FLUSH_LOGS,
        FLUSH_DISTRIBUTED,
        STOP_DISTRIBUTED_SENDS,
        START_DISTRIBUTED_SENDS,
        START_THREAD_FUZZER,
        STOP_THREAD_FUZZER,
        /// proton : starts.
        /// For stream recovery
        PAUSE_STREAM,
        RECOVER_STREAM,
        RESUME_STREAM,

        /// For materialized view recovery
        PAUSE_MATERIALIZED_VIEW,
        RESUME_MATERIALIZED_VIEW,
        ABORT_MATERIALIZED_VIEW,
        RECOVER_MATERIALIZED_VIEW,

        SET_LOG_LEVEL,

        SHOW_LOGGERS,

        PAUSE_TASK,
        RESUME_TASK,
        EXECUTE_TASK,

        INSTALL_PYTHON_PACKAGE,
        UNINSTALL_PYTHON_PACKAGE,
        LIST_PYTHON_PACKAGES,
        /// proton : ends.
        END
    };

    static const char * typeToString(Type type);

    /// proton: starts.
    static Type stringToType(const String & type);
    /// proton: ends

    Type type = Type::UNKNOWN;

    ASTPtr database;
    ASTPtr table;

    String getDatabase() const;
    String getTable() const;

    void setDatabase(const String & name);
    void setTable(const String & name);

    String target_model;
    String target_function;
    String replica;
    String replica_zk_path;
    /// proton: starts
    UInt32 shard = 0;
    bool is_permanent = false;
    UInt64 from_node = 0;
    UInt64 to_node = 0;
    /// proton: ends
    bool is_drop_whole_replica{};
    String storage_policy;
    String volume;
    String disk;
    UInt64 seconds{};

    String filesystem_cache_name;

    String schema_cache_storage;

    String schema_cache_format;

    String log_level;
    String logger_name;

    String python_package_name;
    String python_package_version;
    String python_package_requirements_text;
    String python_package_index_url;
    std::vector<String> python_package_extra_index_urls;

    String getID(char) const override { return "SYSTEM query"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTSystemQuery>(*this);
        res->children.clear();

        if (database) { res->database = database->clone(); res->children.push_back(res->database); }
        if (table) { res->table = table->clone(); res->children.push_back(res->table); }

        return res;
    }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTSystemQuery>(clone(), new_database);
    }

    QueryKind getQueryKind() const override { return QueryKind::System; }

protected:

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
