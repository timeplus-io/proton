#include <Interpreters/InterpreterAlterQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/BlockUtils.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>
#include <Storages/MutationCommands.h>
#include <Storages/PartitionCommands.h>
#include <Common/typeid_cast.h>

/// proton: starts
#include <DistributedMetadata/CatalogService.h>
#include <Interpreters/Streaming/DDLHelper.h>
/// proton: ends

#include <boost/range/algorithm_ext/push_back.hpp>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
    extern const int TABLE_IS_READ_ONLY;
    /// proton: start
    extern const int UNKNOWN_TABLE;
    /// proton: end
}

namespace
{
    void setupColumnIfNotSet(ContextMutablePtr ctx, const ASTAlterQuery & alter)
    {
        if (alter.command_list->children.empty() || ctx->getQueryParameters().contains("column"))
            return;

        for (const auto & child : alter.command_list->children)
        {
            if (auto * cmd = child->as<ASTAlterCommand>())
            {
                if (cmd->type == ASTAlterCommand::Type::ADD_COLUMN || cmd->type == ASTAlterCommand::Type::MODIFY_COLUMN)
                {
                    const auto & column = cmd->col_decl->as<ASTColumnDeclaration &>();
                    if (!column.name.starts_with("_tp_"))
                    {
                        return ctx->setQueryParameter("column", column.name);
                    }
                }
                else if (cmd->type == ASTAlterCommand::Type::DROP_COLUMN || cmd->type == ASTAlterCommand::Type::RENAME_COLUMN)
                {
                    String col = queryToString(cmd->column);
                    if (!col.starts_with("_tp_"))
                    {
                        return ctx->setQueryParameter("column", col);
                    }
                }
            }
        }
    }
}

InterpreterAlterQuery::InterpreterAlterQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterAlterQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    const auto & alter = query_ptr->as<ASTAlterQuery &>();
    if (alter.alter_object == ASTAlterQuery::AlterObjectType::DATABASE)
    {
        return executeToDatabase(alter);
    }
    /// proton: starts
    else if (alter.alter_object == ASTAlterQuery::AlterObjectType::STREAM)
    /// proton: ends
    {
        return executeToTable(alter);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown alter object type");
}


BlockIO InterpreterAlterQuery::executeToTable(const ASTAlterQuery & alter)
{
    BlockIO res;

    getContext()->checkAccess(getRequiredAccess());
    auto table_id = getContext()->resolveStorageID(alter, Context::ResolveOrdinary);
    query_ptr->as<ASTAlterQuery &>().setDatabase(table_id.database_name);

    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (table->isStaticStorage())
        /// proton: starts
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Stream is read-only");
        /// proton: ends
    auto table_lock = table->lockForShare(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    /// Add default database to table identifiers that we can encounter in e.g. default expressions, mutation expression, etc.
    AddDefaultDatabaseVisitor visitor(getContext(), table_id.getDatabaseName());
    ASTPtr command_list_ptr = alter.command_list->ptr();
    visitor.visit(command_list_ptr);

    AlterCommands alter_commands;
    PartitionCommands partition_commands;
    MutationCommands mutation_commands;
    for (const auto & child : alter.command_list->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        if (auto alter_command = AlterCommand::parse(command_ast))
        {
            alter_commands.emplace_back(std::move(*alter_command));
        }
        else if (auto partition_command = PartitionCommand::parse(command_ast))
        {
            partition_commands.emplace_back(std::move(*partition_command));
        }
        else if (auto mut_command = MutationCommand::parse(command_ast))
        {
            if (mut_command->type == MutationCommand::MATERIALIZE_TTL && !metadata_snapshot->hasAnyTTL())
                /// proton: starts
                throw Exception("Cannot MATERIALIZE TTL as there is no TTL for stream "
                    + table->getStorageID().getNameForLogs(), ErrorCodes::INCORRECT_QUERY);
                /// proton: ends

            mutation_commands.emplace_back(std::move(*mut_command));
        }
        else
            throw Exception("Wrong parameter type in ALTER query", ErrorCodes::LOGICAL_ERROR);
    }

    if (!mutation_commands.empty())
    {
        table->checkMutationIsPossible(mutation_commands, getContext()->getSettingsRef());
        MutationsInterpreter(table, metadata_snapshot, mutation_commands, getContext(), false).validate();
        table->mutate(mutation_commands, getContext());
    }

    if (!partition_commands.empty())
    {
        table->checkAlterPartitionIsPossible(partition_commands, metadata_snapshot, getContext()->getSettingsRef());
        auto partition_commands_pipe = table->alterPartition(metadata_snapshot, partition_commands, getContext());
        if (!partition_commands_pipe.empty())
            res.pipeline = QueryPipeline(std::move(partition_commands_pipe));
    }

    if (!alter_commands.empty())
    {
        auto alter_lock = table->lockForAlter(getContext()->getSettingsRef().lock_acquire_timeout);
        StorageInMemoryMetadata metadata = table->getInMemoryMetadata();
        alter_commands.validate(metadata, getContext());
        alter_commands.prepare(metadata);
        table->checkAlterIsPossible(alter_commands, getContext());

        /// proton: start
        if (alterTableDistributed(alter))
        {
            return {};
        }
        /// proton: end

        table->alter(alter_commands, getContext(), alter_lock);
    }

    return res;
}

/// proton: start
bool InterpreterAlterQuery::alterTableDistributed(const ASTAlterQuery & query)
{
    auto ctx = getContext();
    if (!ctx->isDistributedEnv())
    {
        return false;
    }

    const String & database = query.getDatabase().empty() ? ctx->getCurrentDatabase() : query.getDatabase();
    String payload;
    if (ctx->isLocalQueryFromTCP())
    {
        /// We assume it is a `Stream`, so either `on local` or `not exists but is virtual`，
        /// and try do distributed ddl operation.
        /// NOTE: we can not check it from `CatalogService`, there are some reasons:
        /// 1）When a table successfully created but failed to startup, will not update it in CatalogService
        /// 2) When a table successfully created and startup, but fail to update it in CatalogService.
        auto table = DatabaseCatalog::instance().tryGetTable({database, query.getTable()}, ctx);
        if (!table || table->getName() == "Stream")
        {
            /// Build json payload here from SQL statement
            payload = getJSONFromAlterQuery(query);
            ctx->setDistributedDDLOperation(true);
        }
        else
            return false;
    }
    else
    {
        payload = ctx->getQueryParameters().at("_payload");
    }

    if (ctx->isDistributedDDLOperation())
    {
        const auto & catalog_service = CatalogService::instance(ctx->getGlobalContext());
        auto tables = catalog_service.findTableByName(database, query.getTable());
        if (tables.empty())
            /// proton: starts
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Stream {}.{} does not exist.", query.getDatabase(), query.getTable());
            /// proton: ends

        if (tables[0]->engine != "Stream")
        {
            /// FIXME: We only support `Stream` table engine for now
            return false;
        }

        assert(!ctx->getCurrentQueryId().empty());

        auto * log = &Poco::Logger::get("InterpreterAlterQuery");

        auto query_str = queryToString(query);
        /// proton: starts
        LOG_INFO(log, "Altering stream query={} query_id={}", query_str, ctx->getCurrentQueryId());
        /// proton: ends

        std::vector<std::pair<String, String>> string_cols
            = {{"payload", payload},
               {"database", query.getDatabase()},
               {"table", query.getTable()},
               {"query_id", ctx->getCurrentQueryId()},
               {"user", ctx->getUserName()}};

        std::vector<std::pair<String, Int32>> int32_cols;

        auto now = std::chrono::system_clock::now().time_since_epoch();
        std::vector<std::pair<String, UInt64>> uint64_cols = {
            /// Milliseconds since epoch
            std::make_pair("timestamp", std::chrono::duration_cast<std::chrono::milliseconds>(now).count()),
        };

        /// Schema: (payload, database, table, timestamp, query_id, user)
        Block block = buildBlock(string_cols, int32_cols, uint64_cols);
        setupColumnIfNotSet(ctx, query);
        DWAL::OpCode op_code;
        op_code = ctx->getQueryParameters().contains("_payload") ? getAlterTableParamOpCode(ctx->getQueryParameters())
                                                                : getOpCodeFromQuery(query);
        appendDDLBlock(
            std::move(block), ctx, {"table_type", "column", "query_method"}, op_code, log);

        /// proton: starts
        LOG_INFO(
            log, "Request of altering stream query={} query_id={} has been accepted", query_str, ctx->getCurrentQueryId());
        /// proton: ends

        /// FIXME, project tasks status
        return true;
    }
    return false;
}
/// proton: end

BlockIO InterpreterAlterQuery::executeToDatabase(const ASTAlterQuery & alter)
{
    BlockIO res;
    getContext()->checkAccess(getRequiredAccess());
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(alter.getDatabase());
    AlterCommands alter_commands;

    for (const auto & child : alter.command_list->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        if (auto alter_command = AlterCommand::parse(command_ast))
            alter_commands.emplace_back(std::move(*alter_command));
        else
            throw Exception("Wrong parameter type in ALTER DATABASE query", ErrorCodes::LOGICAL_ERROR);
    }

    if (!alter_commands.empty())
    {
        /// Only ALTER SETTING is supported.
        for (const auto & command : alter_commands)
        {
            if (command.type != AlterCommand::MODIFY_DATABASE_SETTING)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported alter type for database engines");
        }

        for (const auto & command : alter_commands)
        {
            if (!command.ignore)
            {
                if (command.type == AlterCommand::MODIFY_DATABASE_SETTING)
                    database->applySettingsChanges(command.settings_changes, getContext());
                else
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported alter command");
            }
        }
    }

    return res;
}
AccessRightsElements InterpreterAlterQuery::getRequiredAccess() const
{
    AccessRightsElements required_access;
    const auto & alter = query_ptr->as<ASTAlterQuery &>();
    for (const auto & child : alter.command_list->children)
        boost::range::push_back(required_access, getRequiredAccessForCommand(child->as<ASTAlterCommand&>(), alter.getDatabase(), alter.getTable()));
    return required_access;
}


AccessRightsElements InterpreterAlterQuery::getRequiredAccessForCommand(const ASTAlterCommand & command, const String & database, const String & table)
{
    AccessRightsElements required_access;

    auto column_name = [&]() -> String { return getIdentifierName(command.column); };
    auto column_name_from_col_decl = [&]() -> std::string_view { return command.col_decl->as<ASTColumnDeclaration &>().name; };
    auto column_names_from_update_assignments = [&]() -> std::vector<std::string_view>
    {
        std::vector<std::string_view> column_names;
        for (const ASTPtr & assignment_ast : command.update_assignments->children)
            column_names.emplace_back(assignment_ast->as<const ASTAssignment &>().column_name);
        return column_names;
    };

    switch (command.type)
    {
        case ASTAlterCommand::UPDATE:
        {
            required_access.emplace_back(AccessType::ALTER_UPDATE, database, table, column_names_from_update_assignments());
            break;
        }
        case ASTAlterCommand::ADD_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_COLUMN, database, table, column_name_from_col_decl());
            break;
        }
        case ASTAlterCommand::DROP_COLUMN:
        {
            if (command.clear_column)
                required_access.emplace_back(AccessType::ALTER_CLEAR_COLUMN, database, table, column_name());
            else
                required_access.emplace_back(AccessType::ALTER_DROP_COLUMN, database, table, column_name());
            break;
        }
        case ASTAlterCommand::MODIFY_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_MODIFY_COLUMN, database, table, column_name_from_col_decl());
            break;
        }
        case ASTAlterCommand::COMMENT_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_COMMENT_COLUMN, database, table, column_name());
            break;
        }
        case ASTAlterCommand::MATERIALIZE_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_MATERIALIZE_COLUMN, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_ORDER_BY:
        {
            required_access.emplace_back(AccessType::ALTER_ORDER_BY, database, table);
            break;
        }
        case ASTAlterCommand::REMOVE_SAMPLE_BY:
        case ASTAlterCommand::MODIFY_SAMPLE_BY:
        {
            required_access.emplace_back(AccessType::ALTER_SAMPLE_BY, database, table);
            break;
        }
        case ASTAlterCommand::ADD_INDEX:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_INDEX, database, table);
            break;
        }
        case ASTAlterCommand::DROP_INDEX:
        {
            if (command.clear_index)
                required_access.emplace_back(AccessType::ALTER_CLEAR_INDEX, database, table);
            else
                required_access.emplace_back(AccessType::ALTER_DROP_INDEX, database, table);
            break;
        }
        case ASTAlterCommand::MATERIALIZE_INDEX:
        {
            required_access.emplace_back(AccessType::ALTER_MATERIALIZE_INDEX, database, table);
            break;
        }
        case ASTAlterCommand::ADD_CONSTRAINT:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_CONSTRAINT, database, table);
            break;
        }
        case ASTAlterCommand::DROP_CONSTRAINT:
        {
            required_access.emplace_back(AccessType::ALTER_DROP_CONSTRAINT, database, table);
            break;
        }
        case ASTAlterCommand::ADD_PROJECTION:
        {
            required_access.emplace_back(AccessType::ALTER_ADD_PROJECTION, database, table);
            break;
        }
        case ASTAlterCommand::DROP_PROJECTION:
        {
            if (command.clear_projection)
                required_access.emplace_back(AccessType::ALTER_CLEAR_PROJECTION, database, table);
            else
                required_access.emplace_back(AccessType::ALTER_DROP_PROJECTION, database, table);
            break;
        }
        case ASTAlterCommand::MATERIALIZE_PROJECTION:
        {
            required_access.emplace_back(AccessType::ALTER_MATERIALIZE_PROJECTION, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_TTL:
        case ASTAlterCommand::REMOVE_TTL:
        {
            required_access.emplace_back(AccessType::ALTER_TTL, database, table);
            break;
        }
        case ASTAlterCommand::MATERIALIZE_TTL:
        {
            required_access.emplace_back(AccessType::ALTER_MATERIALIZE_TTL, database, table);
            break;
        }
        case ASTAlterCommand::RESET_SETTING: [[fallthrough]];
        case ASTAlterCommand::MODIFY_SETTING:
        {
            required_access.emplace_back(AccessType::ALTER_SETTINGS, database, table);
            break;
        }
        case ASTAlterCommand::ATTACH_PARTITION:
        {
            required_access.emplace_back(AccessType::INSERT, database, table);
            break;
        }
        case ASTAlterCommand::DELETE:
        case ASTAlterCommand::DROP_PARTITION:
        case ASTAlterCommand::DROP_DETACHED_PARTITION:
        {
            required_access.emplace_back(AccessType::ALTER_DELETE, database, table);
            break;
        }
        case ASTAlterCommand::MOVE_PARTITION:
        {
            switch (command.move_destination_type)
            {
                case DataDestinationType::DISK: [[fallthrough]];
                case DataDestinationType::VOLUME:
                    required_access.emplace_back(AccessType::ALTER_MOVE_PARTITION, database, table);
                    break;
                case DataDestinationType::TABLE:
                    required_access.emplace_back(AccessType::SELECT | AccessType::ALTER_DELETE, database, table);
                    required_access.emplace_back(AccessType::INSERT, command.to_database, command.to_table);
                    break;
                case DataDestinationType::SHARD:
                    required_access.emplace_back(AccessType::SELECT | AccessType::ALTER_DELETE, database, table);
                    required_access.emplace_back(AccessType::MOVE_PARTITION_BETWEEN_SHARDS);
                    break;
                case DataDestinationType::DELETE:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected destination type for command.");
            }
            break;
        }
        case ASTAlterCommand::REPLACE_PARTITION:
        {
            required_access.emplace_back(AccessType::SELECT, command.from_database, command.from_table);
            required_access.emplace_back(AccessType::ALTER_DELETE | AccessType::INSERT, database, table);
            break;
        }
        case ASTAlterCommand::FETCH_PARTITION:
        {
            required_access.emplace_back(AccessType::ALTER_FETCH_PARTITION, database, table);
            break;
        }
        case ASTAlterCommand::FREEZE_PARTITION: [[fallthrough]];
        case ASTAlterCommand::FREEZE_ALL: [[fallthrough]];
        case ASTAlterCommand::UNFREEZE_PARTITION: [[fallthrough]];
        case ASTAlterCommand::UNFREEZE_ALL:
        {
            required_access.emplace_back(AccessType::ALTER_FREEZE_PARTITION, database, table);
            break;
        }
        case ASTAlterCommand::MODIFY_QUERY:
        {
            required_access.emplace_back(AccessType::ALTER_VIEW_MODIFY_QUERY, database, table);
            break;
        }
        case ASTAlterCommand::RENAME_COLUMN:
        {
            required_access.emplace_back(AccessType::ALTER_RENAME_COLUMN, database, table, column_name());
            break;
        }
        case ASTAlterCommand::MODIFY_DATABASE_SETTING:
        {
            required_access.emplace_back(AccessType::ALTER_DATABASE_SETTINGS, database, table);
            break;
        }
        case ASTAlterCommand::NO_TYPE: break;
        case ASTAlterCommand::MODIFY_COMMENT:
        {
            required_access.emplace_back(AccessType::ALTER_MODIFY_COMMENT, database, table);
            break;
        }
    }

    return required_access;
}

void InterpreterAlterQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr) const
{
    const auto & alter = ast->as<const ASTAlterQuery &>();

    elem.query_kind = "Alter";
    if (alter.command_list != nullptr && alter.alter_object != ASTAlterQuery::AlterObjectType::DATABASE)
    {
        // Alter queries already have their target table inserted into `elem`.
        /// proton: starts
        if (elem.query_tables.size() != 1)
            throw Exception("Alter query should have target stream recorded already", ErrorCodes::LOGICAL_ERROR);
        /// proton: ends

        String prefix = *elem.query_tables.begin() + ".";
        for (const auto & child : alter.command_list->children)
        {
            const auto * command = child->as<ASTAlterCommand>();

            if (command->column)
                elem.query_columns.insert(prefix + command->column->getColumnName());

            if (command->rename_to)
                elem.query_columns.insert(prefix + command->rename_to->getColumnName());

            // ADD COLUMN
            if (command->col_decl)
            {
                elem.query_columns.insert(prefix + command->col_decl->as<ASTColumnDeclaration &>().name);
            }

            if (!command->from_table.empty())
            {
                String database = command->from_database.empty() ? getContext()->getCurrentDatabase() : command->from_database;
                elem.query_databases.insert(database);
                elem.query_tables.insert(database + "." + command->from_table);
            }

            if (!command->to_table.empty())
            {
                String database = command->to_database.empty() ? getContext()->getCurrentDatabase() : command->to_database;
                elem.query_databases.insert(database);
                elem.query_tables.insert(database + "." + command->to_table);
            }
        }
    }
}

}
