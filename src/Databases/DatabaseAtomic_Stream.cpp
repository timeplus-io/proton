#include <Databases/DatabaseAtomic.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Databases/DDLDependencyVisitor.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/MetadataHelper.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/Random/StorageRandom.h>
#include <Storages/StorageNull.h>
#include <Storages/StorageView.h>
#include <Storages/Stream/StorageStream.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
extern const int UNKNOWN_STREAM;
}

void DatabaseAtomic::dropTableFromMetaStore(
    const ContextPtr & local_context, const String & /*table_name*/, StoragePtr storage, [[maybe_unused]] bool sync, const ASTPtr & query)
{
    assert(query);
    assert(!storage->isLocal());
    auto storage_id = storage->getStorageID();

    auto timeout_ms = local_context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();
    auto request = std::make_shared<cluster::DeleteStreamRequest>(
        storage_id.database_name,
        storage_id.table_name,
        storage_id.uuid,
        queryToString(query, true),
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/2);
    auto resp = metastore.deleteStream(std::move(request));
    if (resp->hasError())
    {
        const auto & data = resp->data();

        /// Tolerate UNKNOWN_STREAM when the caller uses IF EXISTS or CASCADE
        /// semantics.  A prior DROP STREAM IF EXISTS may have timed out at the
        /// client while the server-side delete eventually succeeded, so a
        /// subsequent DROP DATABASE CASCADE (or retry) can legitimately arrive
        /// after the stream is already gone.
        if (data.err.error_code == ErrorCodes::UNKNOWN_STREAM)
        {
            if (const auto * drop_query = query->as<ASTDropQuery>())
            {
                if (drop_query->if_exists || drop_query->cascade)
                {
                    LOG_WARNING(
                        log,
                        "Stream {}.{} (UUID {}) was already deleted; ignored under {} semantics",
                        storage_id.database_name,
                        storage_id.table_name,
                        toString(storage_id.uuid),
                        drop_query->cascade ? "CASCADE" : "IF EXISTS");
                    return;
                }
            }
        }

        throw Exception::createRuntime(data.err.error_code, data.err.error_message);
    }
}

bool DatabaseAtomic::renameTableInMemory(
    const String & old_table_name, const String & new_table_name, const String & table_data_path, const StoragePtr & table)
{
    /// Remove old entries
    tables.erase(old_table_name);
    table_name_to_path.erase(old_table_name);

    /// Add new entries
    tables.emplace(new_table_name, table);
    table_name_to_path.emplace(new_table_name, table_data_path);

    return true;
}

void DatabaseAtomic::renameTableInMetaStore(
    const ContextPtr & local_context, StoragePtr table, [[maybe_unused]] const String & table_name, const String & to_table_name, [[maybe_unused]] bool /*dictionary*/)
{
    chassert(!table->isLocal());

    /// 0) check for dependency
    table->checkTableCanBeRenamed(local_context);

    auto old_table_id = table->getStorageID();

    auto timeout_ms = local_context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();

    /// 1) create rename request
    auto request = std::make_shared<cluster::RenameStreamRequest>(
        old_table_id.getDatabaseName(),
        old_table_id.getTableName(),
        std::string(to_table_name),
        /*sql_ddl_=*/"",
        local_context->getUserName(),
        /*initiator_=*/metastore.nodeID(),
        timeout_ms,
        /*request_version=*/2);

    auto response = metastore.renameStream(std::move(request));
    if (response->hasError())
    {
        const auto & data = response->data();
        throw Exception::createRuntime(data.err.error_code, data.err.error_message);
    }
}

void DatabaseAtomic::alterTableInMetaStore_Settings(
    const ContextPtr & local_context,
    const StoragePtr & table,
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    const ASTPtr & ast,
    String && statement,
    std::vector<String> && alter_command_asts)
{
    auto modified_by = local_context->getUserName();
    auto timeout_ms = local_context->getSettingsRef().query_timeout_sec * 1000;

    cluster::nlog::LogConfigPtr log_config;
    if (table->as<StorageMaterializedView>())
        /// proton: starts
        log_config = Globals::getNativeLog().getLogConfig({table_id.uuid, cluster::Constants::InternalCheckpointShardOffset + 1});
        /// proton: ends
    else
        /// proton: starts
        log_config = Globals::getNativeLog().getLogConfig({table_id.uuid, /*shard_=*/0});
        /// proton: ends

    if (!log_config)
        /// proton: starts
        log_config = Globals::getNativeLog().getConfig().log_config;
        /// proton: ends

    /// Handle logstore settings
    int32_t flush_ms = static_cast<int32_t>(log_config->flush_interval_ms);
    int32_t flush_messages = static_cast<int32_t>(log_config->flush_interval_entries);
    int64_t retention_bytes = log_config->retention_size;
    int64_t retention_ms = log_config->retention_ms;

    const auto & create_query = ast->as<const ASTCreateQuery &>();
    const ASTSetQuery * settings = nullptr;
    if (create_query.isMaterializedView())
        settings = create_query.storage_settings;
    else if (create_query.storage)
        settings = create_query.storage->settings;

    bool logstore_settings_changed = false;
    if (settings)
    {
        if (const auto & changes = settings->changes; !changes.empty())
        {
            if (const auto * field = changes.tryGet("logstore_flush_messages"))
            {
                flush_messages = static_cast<UInt32>(field->get<UInt32>());
                logstore_settings_changed = true;
            }

            if (const auto * field = changes.tryGet("logstore_flush_ms"))
            {
                flush_ms = static_cast<UInt32>(field->get<UInt32>());
                logstore_settings_changed = true;
            }

            if (const auto * field = changes.tryGet("logstore_retention_bytes"))
            {
                retention_bytes = field->get<int64_t>();
                logstore_settings_changed = true;
            }

            if (const auto * field = changes.tryGet("logstore_retention_ms"))
            {
                retention_ms = field->get<int64_t>();
                logstore_settings_changed = true;
            }
        }
    }

    std::unordered_map<std::string, int32_t> flush_settings;
    std::unordered_map<std::string, int64_t> retention_settings;
    if (logstore_settings_changed)
    {
        flush_settings = std::unordered_map<std::string, int32_t>{{"flush_ms", flush_ms}, {"flush_messages", flush_messages}};
        retention_settings = std::unordered_map<std::string, int64_t>{{"retention_ms", retention_ms}, {"retention_bytes", retention_bytes}};
    }

    auto & metastore = Globals::getMetaStore();

    auto request = std::make_shared<cluster::UpdateStreamSettingsRequest>(
        table_id.getDatabaseName(),
        table_id.getTableName(),
        table_id.uuid,
        metadata.getVersion(),
        std::move(statement),
        std::move(flush_settings),
        std::move(retention_settings),
        std::move(alter_command_asts),
        /*initiator_=*/metastore.nodeID(),
        timeout_ms,
        /*request_version=*/3);

    request->data().modified_by.swap(modified_by);

    auto update_resp{metastore.updateStreamSettings(std::move(request))};
    if (update_resp->hasError())
        throw DB::Exception(
            update_resp->data().err.error_code, "Failed to update {}, error={}", table_id.table_name, update_resp->data().err.string());

    LOG_INFO(
        log,
        "{} was updated in metastore successfully{}: "
        "logstore_flush_messages={}, logstore_flush_ms={}, "
        "logstore_retention_bytes={}, logstore_retention_ms={}",
        table_id.table_name,
        logstore_settings_changed ? " with settings" : ", remain old settings",
        flush_messages,
        flush_ms,
        retention_bytes,
        retention_ms);
}

void DatabaseAtomic::alterTableInMetaStore_Schema(
    const ContextPtr & local_context,
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    String && statement,
    std::vector<String> && alter_command_asts)
{
    auto timeout_ms = local_context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();
    auto request = std::make_shared<cluster::UpdateStreamSchemaRequest>(
        table_id.getDatabaseName(),
        table_id.getTableName(),
        table_id.uuid,
        std::move(alter_command_asts),
        std::move(statement),
        /*initiator_=*/metastore.nodeID(),
        timeout_ms,
        /*version_before_update_=*/metadata.getVersion(),
        /*request_version=*/3);

    request->data().modified_by = local_context->getUserName();

    auto update_resp{metastore.updateStreamSchema(std::move(request))};
    if (update_resp->hasError())
        throw Exception(
            update_resp->data().err.error_code, "Failed to update {}, error={}", table_id.table_name, update_resp->data().err.string());
    else
        LOG_INFO(log, "{} was updated in metastore successfully", table_id.table_name);
}

void DatabaseAtomic::alterTableInMetaStore(
    const ContextPtr & local_context,
    const StoragePtr & table,
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    String alter_command,
    std::vector<String> alter_command_asts)
{
    chassert(!alter_command.empty());
    chassert(!alter_command_asts.empty());

    /// Read the definition of the table and replace the necessary parts with new ones.
    ASTPtr ast = getCreateTableFromMetaStore(table, local_context);

    applyMetadataChangesToCreateQuery(ast, metadata);

    String statement = getObjectDefinitionFromCreateQuery(ast);

    TableNamesSet new_dependencies = getDependenciesSetFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), ast);
    DatabaseCatalog::instance().updateLoadingDependencies(table_id, std::move(new_dependencies));
    static const std::unordered_set<std::string> settings_alter_commands
        = {"MODIFY_SETTING", "MODIFY_QUERY_SETTING", "RESET_QUERY_SETTING"};

    static const std::unordered_set<std::string> schema_alter_commands
        = {"ADD_INDEX", "DROP_INDEX", "ADD_COLUMN", "RENAME_COLUMN", "MODIFY_TTL", "REMOVE_TTL", "MODIFY_QUERY", "COMMENT_TABLE"};

    if (settings_alter_commands.contains(alter_command))
    {
        alterTableInMetaStore_Settings(local_context, table, table_id, metadata, ast, std::move(statement), std::move(alter_command_asts));
    }
    else if (schema_alter_commands.contains(alter_command))
    {
        alterTableInMetaStore_Schema(local_context, table_id, metadata, std::move(statement), std::move(alter_command_asts));
    }
    else
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Alter command='{}' is not supported for storage engine='{}' yet",
            alter_command,
            table->getName());
    }
}

void DatabaseAtomic::createTableInMetaStore(
    const ContextPtr & local_context, const String & table_name, const StoragePtr & table, const ASTPtr & query)
{
    const auto & create = query->as<ASTCreateQuery &>();
    assert(table_name == create.getTable());

    auto storage_id = table->getStorageID();

    UInt32 shards = 1;

    /// proton: starts
    const auto & log_config = Globals::getNativeLog().getConfig().log_config;
    /// proton: ends
    UInt64 retention_ms = log_config->retention_ms;
    UInt64 retention_bytes = log_config->retention_size;
    UInt32 flush_interval_entries = static_cast<UInt32>(log_config->flush_interval_entries);
    UInt32 flush_bytes = static_cast<UInt32>(log_config->flush_bytes);
    UInt32 flush_interval_ms = static_cast<UInt32>(log_config->flush_interval_ms);

    auto apply_settings = [&retention_ms, &retention_bytes, &flush_interval_entries, &flush_interval_ms](auto settings) {
        retention_ms = settings->logstore_retention_ms.value;
        retention_bytes = settings->logstore_retention_bytes.value;
        flush_interval_entries = static_cast<UInt32>(settings->logstore_flush_messages);
        flush_interval_ms = static_cast<UInt32>(settings->logstore_flush_ms);
    };

    if (const auto * stream = table->as<StorageStream>())
    {
        shards = stream->getShards();
        apply_settings(stream->getSettings());
    }
    else if (const auto * mv = table->as<StorageMaterializedView>(); mv)
    {
        shards = mv->getShards();
        /// Doesn't use `StorageMaterializedView::getStorageSettings()` since the inner storage is not created yet
        chassert(create.isMaterializedView());
        if (create.storage_settings)
        {
            if (!mv->getExternalTargetTableID())
            {
                if (const auto * field = create.storage_settings->changes.tryGet("logstore_flush_ms"))
                    flush_interval_ms = static_cast<UInt32>(field->get<UInt32>());

                if (const auto * field = create.storage_settings->changes.tryGet("logstore_flush_messages"))
                    flush_interval_entries = static_cast<UInt32>(field->get<UInt32>());

                if (const auto * field = create.storage_settings->changes.tryGet("logstore_retention_ms"))
                    retention_ms = field->get<UInt64>();

                if (const auto * field = create.storage_settings->changes.tryGet("logstore_retention_bytes"))
                    retention_bytes = field->get<UInt64>();
            }
        }
    }
    else if (const auto * random_stream = table->as<StorageRandom>(); random_stream)
    {
        /// Set shards to 0 to avoid placement
        shards = 0;
    }
    else if (const auto * view = table->as<StorageView>(); view)
    {
        /// Set shards to 0 to avoid placement
        shards = 0;
    }
    else if (const auto * null_stream = table->as<StorageNull>(); null_stream)
    {
        /// Set shards to 0 to avoid placement
        shards = 0;
    }

    auto timeout_ms = local_context->getSettingsRef().query_timeout_sec.value * 1000;
    auto & metastore = Globals::getMetaStore();

    String statement = getObjectDefinitionFromCreateQuery(query);
    auto request = std::make_shared<cluster::CreateStreamRequest>(
        std::string{storage_id.database_name},
        std::string{storage_id.table_name},
        storage_id.uuid,
        shards,
        1,  /// Single-instance: always use replica count of 1
        std::move(statement),
        queryToString(query, true),
        /*initiator_=*/metastore.nodeID(),
        timeout_ms,
        /*request_version=*/2);

    request->setRetentionInterval(retention_ms);
    request->setRetentionBytes(retention_bytes);
    request->setFlushInterval(flush_interval_ms);
    request->setFlushMessages(flush_interval_entries);
    request->setFlushBytes(flush_bytes);
    request->setCreatedBy(local_context->getUserName());
    request->setLastModifiedBy(local_context->getUserName());

    auto resp = metastore.createStream(request);
    if (resp->hasError())
    {
        const auto & data = resp->data();
        throw Exception(
            data.err.error_code, "Failed to create stream '{}.{}', error={}", database_name, table_name, data.err.error_message);
    }
}

}
