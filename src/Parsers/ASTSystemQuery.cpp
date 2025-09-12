#include <Parsers/ASTIdentifier.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTSystemQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

#include <magic_enum.hpp>

/// proton: starts
#include <boost/algorithm/string.hpp>
/// proton: ends

namespace DB
{

namespace
{
    std::vector<std::string> getTypeIndexToTypeName()
    {
        constexpr std::size_t types_size = magic_enum::enum_count<ASTSystemQuery::Type>();

        std::vector<std::string> type_index_to_type_name;
        type_index_to_type_name.resize(types_size);

        auto entries = magic_enum::enum_entries<ASTSystemQuery::Type>();
        for (const auto & [entry, str] : entries)
        {
            auto str_copy = String(str);
            std::replace(str_copy.begin(), str_copy.end(), '_', ' ');
            type_index_to_type_name[static_cast<UInt64>(entry)] = std::move(str_copy);
        }

        return type_index_to_type_name;
    }
}

const char * ASTSystemQuery::typeToString(Type type)
{
    /** During parsing if SystemQuery is not parsed properly it is added to Expected variants as description check IParser.h.
      * Description string must be statically allocated.
      */
    static std::vector<std::string> type_index_to_type_name = getTypeIndexToTypeName();
    const auto & type_name = type_index_to_type_name[static_cast<UInt64>(type)];
    return type_name.data();
}

/// proton: starts
ASTSystemQuery::Type ASTSystemQuery::stringToType(const String & type)
{
    String upper = boost::to_upper_copy<String>(type);
    std::replace(upper.begin(), upper.end(), ' ', '_');
    auto enum_type = magic_enum::enum_cast<ASTSystemQuery::Type>(upper);
    if (enum_type.has_value())
        return enum_type.value();

    return ASTSystemQuery::Type::UNKNOWN;
}
/// proton: ends

String ASTSystemQuery::getDatabase() const
{
    String name;
    tryGetIdentifierNameInto(database, name);
    return name;
}

String ASTSystemQuery::getTable() const
{
    String name;
    tryGetIdentifierNameInto(table, name);
    return name;
}

void ASTSystemQuery::setDatabase(const String & name)
{
    if (database)
    {
        std::erase(children, database);
        database.reset();
    }

    if (!name.empty())
    {
        database = std::make_shared<ASTIdentifier>(name);
        children.push_back(database);
    }
}

void ASTSystemQuery::setTable(const String & name)
{
    if (table)
    {
        std::erase(children, table);
        table.reset();
    }

    if (!name.empty())
    {
        table = std::make_shared<ASTIdentifier>(name);
        children.push_back(table);
    }
}

void ASTSystemQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SYSTEM ";
    settings.ostr << typeToString(type) << (settings.hilite ? hilite_none : "");

    auto print_database_table = [&]
    {
        settings.ostr << " ";
        if (database)
        {
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getDatabase())
                          << (settings.hilite ? hilite_none : "") << ".";
        }
        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getTable())
                      << (settings.hilite ? hilite_none : "");
    };

    auto print_drop_replica = [&]
    {
        settings.ostr << " " << quoteString(replica);
        if (table)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM TABLE"
                          << (settings.hilite ? hilite_none : "");
            print_database_table();
        }
        else if (!replica_zk_path.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM ZKPATH "
                          << (settings.hilite ? hilite_none : "") << quoteString(replica_zk_path);
        }
        else if (database)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM DATABASE "
                          << (settings.hilite ? hilite_none : "");
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(getDatabase())
                          << (settings.hilite ? hilite_none : "");
        }
    };

    auto print_on_volume = [&]
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON VOLUME "
                      << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(storage_policy)
                      << (settings.hilite ? hilite_none : "")
                      << "."
                      << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(volume)
                      << (settings.hilite ? hilite_none : "");
    };

    if (!cluster.empty())
        formatOnCluster(settings);

    if (   type == Type::STOP_MERGES
        || type == Type::START_MERGES
        || type == Type::STOP_TTL_MERGES
        || type == Type::START_TTL_MERGES
        || type == Type::STOP_MOVES
        || type == Type::START_MOVES
        || type == Type::STOP_FETCHES
        || type == Type::START_FETCHES
        || type == Type::STOP_REPLICATED_SENDS
        || type == Type::START_REPLICATED_SENDS
        || type == Type::STOP_REPLICATION_QUEUES
        || type == Type::START_REPLICATION_QUEUES
        || type == Type::STOP_DISTRIBUTED_SENDS
        || type == Type::START_DISTRIBUTED_SENDS)
    {
        if (table)
            print_database_table();
        else if (!volume.empty())
            print_on_volume();

        if (is_permanent)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " PERMANENT" << (settings.hilite ? hilite_none : "");
    }
    else if (  type == Type::RESTART_REPLICA
            || type == Type::RESTORE_REPLICA
            || type == Type::SYNC_REPLICA
            || type == Type::WAIT_LOADING_PARTS
            || type == Type::FLUSH_DISTRIBUTED
            || type == Type::RELOAD_DICTIONARY)
    {
        print_database_table();
    }
    else if (type == Type::DROP_REPLICA)
    {
        print_drop_replica();
    }
    else if (type == Type::SUSPEND)
    {
         settings.ostr << (settings.hilite ? hilite_keyword : "") << " FOR "
            << (settings.hilite ? hilite_none : "") << seconds
            << (settings.hilite ? hilite_keyword : "") << " SECOND"
            << (settings.hilite ? hilite_none : "");
    }
    else if (type == Type::DROP_FORMAT_SCHEMA_CACHE)
    {
        if (!schema_cache_format.empty())
            settings.ostr << (settings.hilite ? hilite_none : "") << " FOR " << backQuoteIfNeed(schema_cache_format);
    }
    else if (type == Type::DROP_FILESYSTEM_CACHE)
    {
        if (!filesystem_cache_name.empty())
            settings.ostr << (settings.hilite ? hilite_none : "") << " " << filesystem_cache_name;
    }
    /// proton: starts.
    else if (
        type == Type::PAUSE_STREAM || type == Type::RESUME_STREAM || type == Type::RECOVER_STREAM || type == Type::PAUSE_MATERIALIZED_VIEW
        || type == Type::RESUME_MATERIALIZED_VIEW || type == Type::ABORT_MATERIALIZED_VIEW || type == Type::RECOVER_MATERIALIZED_VIEW
        || type == Type::PAUSE_TASK || type == Type::RESUME_TASK || type == Type::EXECUTE_TASK)
    {
        if (table)
            print_database_table();

        /// PAUSE/RESUME/RECOVER STREAM <database.table> <shard_num>
        if (type == Type::PAUSE_STREAM || type == Type::RESUME_STREAM || type == Type::RECOVER_STREAM)
            settings.ostr << shard;

        if (is_permanent)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " PERMANENT" << (settings.hilite ? hilite_none : "");
    }
    else if (type == Type::SET_LOG_LEVEL)
    {
        /// SYSTEM SET LOG LEVEL <level> [FOR <logger_name>]
        settings.ostr << " " << quoteString(log_level);
        if (!logger_name.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FOR " << (settings.hilite ? hilite_none : "")
                          << quoteString(logger_name);
        }
    }
    else if (type == Type::SHOW_LOGGERS)
    {
        /// SYSTEM SHOW LOGGERS
    }
    else if (type == Type::INSTALL_PYTHON_PACKAGE)
    {
        /// SYSTEM INSTALL PYTHON PACKAGE 'package_name' ['version']
        settings.ostr << " " << quoteString(python_package_name);
        if (!python_package_version.empty())
        {
            settings.ostr << " " << quoteString(python_package_version);
        }
    }
    else if (type == Type::UNINSTALL_PYTHON_PACKAGE)
    {
        /// SYSTEM UNINSTALL PYTHON PACKAGE 'package_name'
        settings.ostr << " " << quoteString(python_package_name);
    }
    else if (type == Type::LIST_PYTHON_PACKAGES)
    {
        /// SYSTEM LIST PYTHON PACKAGES
    }
    /// proton: ends.
}


}
