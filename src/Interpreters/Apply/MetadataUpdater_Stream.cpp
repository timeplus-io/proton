#include <Interpreters/Apply/MetadataUpdater.h>

#include <Cluster/Common/Nulls.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DatabaseAtomic.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/MetadataHelper.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/Stream/StorageStream.h>
#include <Common/Exception.h>
#include <Common/LogstoreRetentionSettings.h>
#include <Common/logger_useful.h>

namespace DB
{
/// From DatabaseOnDisk.h
std::pair<String, StoragePtr> createTableFromAST(
    ASTCreateQuery ast_create_query,
    const String & database_name,
    const String & table_data_path_relative,
    ContextMutablePtr context,
    bool has_force_restore_data_flag,
    Int32 schema_version,
    bool attach);

/// From DatabasesCommon.h
StorageInMemoryCreateQueryPtr parseCreateQueryFromAST(const ASTPtr & query, const String & database_, const String & table_);

namespace ErrorCodes
{
extern const int OK;

extern const int UNKNOWN_STREAM;
extern const int UNKNOWN_FUNCTION;
extern const int UNKNOWN_IDENTIFIER;
extern const int UNKNOWN_TYPE;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_EXCEPTION;
extern const int UNKNOWN_DISK;
extern const int INVALID_DISK;

extern const int NOT_IMPLEMENTED;
extern const int UNSUPPORTED;
extern const int UNSUPPORTED_PARAMETER;
extern const int RESOURCE_NOT_FOUND;
extern const int RESOURCE_NOT_INITED;
extern const int BAD_ARGUMENTS;
extern const int BAD_REQUEST_PARAMETER;
extern const int UDF_INVALID_NAME;
extern const int UNSUPPORTED_METHOD;
extern const int INCORRECT_DATA;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TYPE_MISMATCH;
extern const int HAVE_DEPENDENT_OBJECTS;

extern const int UDF_INTERNAL_ERROR;
extern const int SYNTAX_ERROR;
extern const int LOGICAL_ERROR;
extern const int INCORRECT_QUERY;

extern const int STREAM_ALREADY_EXISTS;
extern const int FUNCTION_ALREADY_EXISTS;
extern const int DIRECTORY_DOESNT_EXIST;
extern const int QUERY_WAS_CANCELLED;

extern const int CANNOT_DROP_FUNCTION;
extern const int CANNOT_ASSIGN_ALTER;
extern const int CANNOT_CREATE_RECURSIVE_FUNCTION;

extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
extern const int UNABLE_TO_SKIP_UNUSED_SHARDS;
extern const int TOO_MANY_ROWS;
extern const int TOO_LARGE_STRING_SIZE;
extern const int TOO_LARGE_ARRAY_SIZE;

extern const int METADATA_VERSION_CHANGED;
extern const int METADATA_MISMATCH;
}

void MetadataUpdater::handleCreateStream(
    const cluster::CreateStreamRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto stream_info = request_data.desc.stream.string();
    LOG_INFO(logger, "Start CreateStream {{{}}}", stream_info);
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End CreateStream {{{}}}, took={}ms", stream_info, stopwatch.elapsedMilliseconds()); });

    const auto cid = request_header.correlationID();

    StoragePtr storage;
    auto res = executeWithRetry([this, &storage, &request_data]() {
        auto [new_storage, err_code] = doHandleCreateStream(request_data);
        if (err_code == DB::ErrorCodes::OK)
            storage.swap(new_storage);
        return err_code;
    });

    if (res != DB::ErrorCodes::OK)
    {
        handleFailedRequest(cid, sn, res, fmt::format("Failed to create stream: {{{}}}", stream_info));
        return;
    }

    if (!storage)
    {
        /// This can happen when `if_not_exists` is true and the stream already exists.
        /// Persist applied sequence for no-op create to avoid replaying the same request after restart.
        res = executeWithRetry(
            [this, &sn] { return meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn)).error_code; });
        if (res != DB::ErrorCodes::OK)
        {
            handleFailedRequest(
                cid, sn, res, fmt::format("Failed to mark create stream no-op as applied: {{{}}}", stream_info));
            return;
        }

        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
        return;
    }

    /// Save stream definition to MetaDB
    res = executeWithRetry([this, &request_data, &sn]() {
        auto err = meta_store->getMetaDB().saveStream(request_data.desc, cluster::AppliedSequence(sn));
        if (!err.hasError())
        {
            /// Cleanup pending request after successful save
            meta_store->getMetaDB().deletePendingRequest(sn);
        }
        return err.error_code;
    });

    if (res != DB::ErrorCodes::OK)
    {
        handleFailedRequest(cid, sn, res, fmt::format("Failed to commit created stream definition: {{{}}}", stream_info));
        return;
    }

    /// We will need to delay startup until we commit metadata to metadb since during startup we may need access the metadab
    /// for the configuration.
    try
    {
        storage->startup();
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Failed to start up stream {{{}}}, error={}", stream_info, e.message());
        meta_store->ackProposal(request_header.correlationID(), sn, e.code(), std::string{e.message()});
        return;
    }
    catch (...)
    {
        tryLogCurrentException(logger, fmt::format("Failed to start up stream {{{}}}", stream_info));
        meta_store->ackProposal(request_header.correlationID(), sn, getCurrentExceptionCode(), /*error_message=*/"");
        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

std::pair<StoragePtr, int32_t> MetadataUpdater::doHandleCreateStream(const cluster::protocol::CreateStreamRequestData & request_data) const
{
    const auto & database = request_data.desc.stream.ns;

    /// Calculate data path
    auto db = DatabaseCatalog::instance().tryGetDatabase(database);
    if (!db)
    {
        LOG_ERROR(logger, "Failed to create stream because Database {} doesn't exist", database);
        return {nullptr, DB::ErrorCodes::UNKNOWN_DATABASE};
    }

    ASTPtr ast = nullptr;
    {
        const auto & settings = global_context->getSettingsRef();
        ParserCreateQuery parser;
        const char * pos = request_data.sql_ddl.data();
        String error_message;

        ast = tryParseQuery(
            parser,
            pos,
            pos + request_data.sql_ddl.size(),
            error_message,
            /* hilite = */ false,
            "from metadata updater",
            /* allow_multi_statements = */ false,
            0,
            settings.max_parser_depth);

        if (!ast)
        {
            LOG_ERROR(logger, "Failed to parse stream creation query={} error={}", request_data.sql_ddl, error_message);
            return {nullptr, DB::ErrorCodes::SYNTAX_ERROR};
        }
    }

    auto * create = ast->as<ASTCreateQuery>();
    if (!create)
    {
        LOG_ERROR(logger, "Not a stream creation query={}", request_data.sql_ddl);
        return {nullptr, DB::ErrorCodes::LOGICAL_ERROR};
    }

    if (auto table_storage = db->tryGetTable(request_data.desc.stream.name, global_context); table_storage)
    {
        if (create->if_not_exists)
        {
            if (table_storage->getStorageID().uuid == request_data.desc.stream.id)
            {
                /// Return nullptr for no new stream is created; and OK status since if_not_exists is specified.
                return {nullptr, DB::ErrorCodes::OK};
            }

            LOG_ERROR(
                logger,
                "Failed to create stream because stream {}.{} already exists with a different UUID: request_uuid={} existing_uuid={}",
                database,
                request_data.desc.stream.name,
                DB::toString(request_data.desc.stream.id),
                DB::toString(table_storage->getStorageID().uuid));
            return {nullptr, DB::ErrorCodes::STREAM_ALREADY_EXISTS};
        }

        LOG_ERROR(logger, "Failed to create stream because stream {}.{} already exists", database, request_data.desc.stream.name);
        return {nullptr, DB::ErrorCodes::STREAM_ALREADY_EXISTS};
    }

    auto data_path = db->getTableDataPath(*create);

    auto local_context = Context::createCopy(global_context);
    CurrentThread::QueryScope query_scope(local_context);
    auto [table_name, storage]
        = createTableFromAST(*create, database, data_path, local_context, false, request_data.desc.data_version, /*attach=*/false);

    /// Setup UUID
    auto storage_id = storage->getStorageID();
    storage_id.uuid = request_data.desc.stream.id;
    storage->renameInMemory(storage_id);

    db->attachTable(global_context, request_data.desc.stream.name, storage, data_path);

    /// issue-739, first change the in memory create query to avoid race
    const auto & new_create_query = parseCreateQueryFromAST(ast, database, table_name);
    storage->setInMemoryCreateQuery(new_create_query);

    /// for timeplus external stream, we need to add dependencies to the target stream
    /// same logic as `InterpreterCreateQuery::createTable` after `DoCreateTable`
    if (storage->getName() == "ExternalStream")
    {
        QualifiedTableName qualified_name{database, table_name};
        TableNamesSet loading_dependencies = getDependenciesSetFromCreateQuery(global_context, qualified_name, ast);
        if (!loading_dependencies.empty())
            DatabaseCatalog::instance().addLoadingDependencies(qualified_name, std::move(loading_dependencies));
    }

    return {std::move(storage), DB::ErrorCodes::OK};
}

void MetadataUpdater::handleDeleteStream(
    const cluster::DeleteStreamRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto stream_info = request_data.stream.string();
    LOG_INFO(logger, "Start DeleteStream {{{}}}", stream_info);
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End DeleteStream {{{}}}, took={}ms", stream_info, stopwatch.elapsedMilliseconds()); });

    auto [_, storage] = DatabaseCatalog::instance().tryGetByUUID(request_data.stream.id);
    if (!storage)
    {
        handleFailedRequest(
            request_header.correlationID(),
            sn,
            DB::ErrorCodes::UNKNOWN_STREAM,
            fmt::format("Stream with UUID={} doesn't exist", DB::toString(request_data.stream.id)));
        return;
    }

    if (auto storage_id = storage->getStorageID();
        storage_id.database_name != request_data.stream.ns || storage_id.table_name != request_data.stream.name)
    {
        handleFailedRequest(
            request_header.correlationID(),
            sn,
            DB::ErrorCodes::UNKNOWN_STREAM,
            fmt::format(
                "Stream with UUID={} has the different name {}.{}",
                DB::toString(request_data.stream.id),
                storage_id.database_name,
                storage_id.table_name));
        return;
    }

    /// Try to commit delete stream to metadb until success or failed with unretriable error
    /// First, delete stream from the meta db
    auto res = executeWithRetry([this, &request_data, &sn] {
        auto err = meta_store->getMetaDB().deleteStream(request_data.stream.ns, request_data.stream.name, cluster::AppliedSequence(sn));
        return err.error_code;
    });

    if (res != DB::ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(), sn, res, fmt::format("Failed to commit delete stream to metadb: stream={{{}}}", stream_info));
        return;
    }

    /// Then, delete it from memory and clean up the data in background
    res = executeWithRetry([this, &request_data] { return doHandleDeleteStream(request_data); });
    /// Deleting from memory and cleanup should always succeed. Keeping the error handling is for safety.
    chassert(res == DB::ErrorCodes::OK);
    if (res != DB::ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(), sn, res, fmt::format("Failed to delete stream from memory: stream={{{}}}", stream_info));
        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

int32_t MetadataUpdater::doHandleDeleteStream(const cluster::protocol::DeleteStreamRequestData & request_data) const
{
    const auto & uuid_ = request_data.stream.id;
    auto [_, storage] = DatabaseCatalog::instance().tryGetByUUID(uuid_);
    if (!storage)
    {
        LOG_INFO(logger, "Stream has already been deleted");
        return DB::ErrorCodes::OK;
    }

    storage->flushAndShutdown(/*dropping=*/true);

    const auto & database_name = request_data.stream.ns;
    const auto & table_name = request_data.stream.name;
    StorageID storage_id(database_name, table_name, uuid_);

    /// 1) Create a `drop` file under metadata folder with sql definition in it,
    /// then we can leverage existing table delete logic to delete historical
    /// data store
    String table_metadata_path_drop;
    {
        auto create_query = storage->getInMemoryCreateQuery();
        table_metadata_path_drop = DatabaseCatalog::instance().getPathForDroppedMetadata(storage_id);
        WriteBufferFromFile out(table_metadata_path_drop, create_query->getQuery().size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(create_query->getQuery(), out);
        out.next();
        out.close();
    }

    {
        /// 2) Remove mapping from memory
        DatabasePtr db = DatabaseCatalog::instance().tryGetDatabase(database_name);
        db->detachTable(global_context, table_name);
        db->setDetachedTableNotInUseForce(uuid_);
    }

    /// for timeplus external stream, we need to remove dependencies to the target stream
    /// same logic as `InterpreterDropQuery::executeToTableImpl` before `database->dropTable`
    if (storage->getName() == "ExternalStream")
    {
        QualifiedTableName qualified_name{database_name, table_name};
        DatabaseCatalog::instance().tryRemoveLoadingDependencies(
            storage_id,
            global_context->getSettingsRef().check_table_dependencies,
            /*is_drop_database=*/true);
    }

    /// 3) Remove NativeLog and historical data async.
    ///
    /// When StorageStream is dropped async in background, StorageStream::drop() will be invoked, in that function
    /// we will clean its associated NativeLog and historical data all together.
    ///
    /// It is possible that system shutdown before we have a chance to cleanup NativeLog / historical data, it is fine and we
    /// will pick up the deletion because
    /// a. The stream is gone in Metastore (deleted).
    /// b. We dropped a mark file in historical data store in step 1) above, so the existing historical store deletion logic will pick it up
    /// c. During system boot, since the stream doesn't exist in metadb, the on disk NativeLog for the stream will be deleted during
    ///    (Replicated)LogManager loads the logs
    ///
    /// Note: we moved over `storage` to avoid increasing the ref-count which is important since background cleanup depends on the ref count
    /// Only the ref count drops to 1, it then starts GC the storage object. Moving over make the GC starts asap
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(storage_id, std::move(storage), table_metadata_path_drop, /*ignore_delay=*/true);

    return DB::ErrorCodes::OK;
}

/// Load stream descriptor metadata from metadb until success or permanent error is encountered
cluster::CallResultV<cluster::protocol::StreamDescriptorPtr>
MetadataUpdater::loadStreamDescriptor(const cluster::Stream & stream, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    auto stream_info = stream.string();
    cluster::CallResultV<cluster::protocol::StreamDescriptorPtr> stream_desc;

    auto res = executeWithRetry([this, &stream_desc, &stream] {
        stream_desc = meta_store->getMetaDB().getStream(stream.ns, stream.name);
        return stream_desc.err.error_code;
    });

    if (!stream_desc.hasError() && stream_desc.result->stream.id != stream.id)
    {
        stream_desc.err.error_code = DB::ErrorCodes::METADATA_MISMATCH;
        stream_desc.err.error_message
            = fmt::format("Stream UUID mismatch for stream={{{}}}: got UUID={}", stream_info, DB::toString(stream_desc.result->stream.id));

        res = stream_desc.err.error_code;
    }

    if (res != ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(),
            sn,
            res,
            fmt::format(
                "Failed to load stream descriptor when updating stream settings: stream={{{}}} error={}",
                stream_info,
                stream_desc.err.error_message));
    }

    return stream_desc;
}

int MetadataUpdater::updateStreamAndCommit(
    const cluster::RequestHeader & request_header,
    uint64_t sn,
    uint32_t version_before_update,
    cluster::protocol::StreamDescriptor & desc,
    std::function<int(cluster::protocol::StreamDescriptor &)> update) const
{
    auto stream_info = desc.stream.string();

    /// Check version first
    if (version_before_update != cluster::Nulls::NullVersion && desc.data_version != version_before_update)
    {
        LOG_ERROR(
            logger,
            "Failed to update stream due to version changed: stream={{{}}} data_version={} version_before_update={}",
            stream_info,
            desc.data_version,
            version_before_update);

        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(
            request_header.correlationID(),
            sn,
            ErrorCodes::METADATA_VERSION_CHANGED,
            fmt::format(
                "Failed to update stream due to version changed: stream={{{}}} data_version={} version_before_update={}",
                stream_info,
                desc.data_version,
                version_before_update));

        return ErrorCodes::METADATA_VERSION_CHANGED;
    }

    /// try to upgrade in-memory representation until success or failed with unretriable error
    auto res = executeWithRetry([&update, &desc] { return update(desc); });
    if (res != ErrorCodes::OK)
    {
        handleFailedRequest(request_header.correlationID(), sn, res, fmt::format("Failed to update stream: stream={{{}}}", stream_info));
        return res;
    }

    /// Try to commit updated desc to metadb until success or failed with unretriable error
    res = executeWithRetry(
        [this, &desc, &sn] { return meta_store->getMetaDB().saveStream(desc, cluster::AppliedSequence(sn)).error_code; });
    if (res != ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(), sn, res, fmt::format("Failed to commit updated stream: stream={{{}}}", stream_info));
        return res;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");

    return ErrorCodes::OK;
}

void MetadataUpdater::handleUpdateStreamSettings(
    const cluster::UpdateStreamSettingsRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();

    auto stream_info = request_data.stream.string();

    LOG_INFO(logger, "Start UpdateStreamSettings {{{}}}", stream_info);
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End UpdateStreamSettings {{{}}}, took={}ms", stream_info, stopwatch.elapsedMilliseconds()); });

    auto stream_desc = loadStreamDescriptor(request_data.stream, request_header, sn);
    if (stream_desc.hasError())
        return;

    updateStreamAndCommit(
        request_header, sn, request_data.version_before_update, *stream_desc.result, [&, this](cluster::protocol::StreamDescriptor & desc) {
            return doHandleUpdateStreamSettings(request_data, desc);
        });
}

int MetadataUpdater::doHandleUpdateStreamSettings(
    const cluster::protocol::UpdateStreamSettingsRequestData & request_data, cluster::protocol::StreamDescriptor & desc) const
{
    bool log_store_setting_changed = 0;

    /// Merge the changes
    /// flush settings
    for (const auto & [k, v] : request_data.flush_settings)
    {
        if (k == "flush_messages")
        {
            desc.flush_messages = v;
            log_store_setting_changed = true;
        }
        else if (k == "flush_ms")
        {
            desc.flush_ms = v;
            log_store_setting_changed = true;
        }
    }

    /// retention settings
    for (const auto & [k, v] : request_data.retention_settings)
    {
        if (k == "retention_bytes")
        {
            desc.retention_bytes = encodeLogstoreRetentionForMetastore(v);
            log_store_setting_changed = true;
        }
        else if (k == "retention_ms")
        {
            desc.retention_ms = encodeLogstoreRetentionForMetastore(v);
            log_store_setting_changed = true;
        }
    }

    /// new sql def
    if (!request_data.sql_def.empty())
    {
        desc.sql_def = request_data.sql_def;

        StorageID table_id{desc.stream.ns, desc.stream.name, desc.stream.id};
        auto table = DatabaseCatalog::instance().tryGetTable(table_id, global_context);
        if (table)
        {
            if (log_store_setting_changed)
            {
                if (auto * stream = table->as<StorageStream>())
                {
                    std::unordered_map<std::string, int32_t> flush_settings
                        = {{"flush_messages", desc.flush_messages}, {"flush_ms", desc.flush_ms}};

                    const auto retention_bytes_setting = decodeLogstoreRetentionForRuntime(desc.retention_bytes);
                    const auto retention_ms_setting = decodeLogstoreRetentionForRuntime(desc.retention_ms);
                    std::unordered_map<std::string, int64_t> retention_settings
                        = {{"retention_bytes", retention_bytes_setting}, {"retention_ms", retention_ms_setting}};

                    stream->alterLogSettings(flush_settings, retention_settings);
                }
            }

            /// Update the in memory metadata
            auto new_metadata = table->getInMemoryMetadata();
            updateInMemoryMetadataForStream(table, table_id, desc, new_metadata);

            if (auto * mv = table->as<StorageMaterializedView>())
                mv->onAlterQuerySettings();
        }
        else
        {
            LOG_ERROR(logger, "'{}' is not found", table_id.getNameForLogs());
            return DB::ErrorCodes::UNKNOWN_STREAM;
        }
    }

    desc.data_version++;
    desc.last_modify_timestamp_ms = request_data.last_modified;
    desc.last_modified_by = request_data.modified_by;

    /// TODO: more error check before return, we also support update the sql def.
    return DB::ErrorCodes::OK;
}

void MetadataUpdater::handleAlterStreamSchema(
    const cluster::UpdateStreamSchemaRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();

    auto stream_info = request_data.stream.string();

    LOG_INFO(logger, "Start UpdateStreamSchema {{{}}}", stream_info);
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End UpdateStreamSchema {{{}}}, took={}ms", stream_info, stopwatch.elapsedMilliseconds()); });

    auto stream_desc = loadStreamDescriptor(request_data.stream, request_header, sn);
    if (stream_desc.hasError())
        return;

    updateStreamAndCommit(
        request_header, sn, request_data.version_before_update, *stream_desc.result, [&, this](cluster::protocol::StreamDescriptor & desc) {
            return doHandleAlterStreamSchema(request_data, desc);
        });
}

int MetadataUpdater::doHandleAlterStreamSchema(
    const cluster::protocol::UpdateStreamSchemaRequestData & request_data, cluster::protocol::StreamDescriptor & desc) const
{
    if (!request_data.sql_def.empty())
        desc.sql_def = request_data.sql_def;

    StorageID table_id{desc.stream.ns, desc.stream.name, desc.stream.id};
    auto table = DatabaseCatalog::instance().tryGetTable(table_id, global_context);
    if (table)
    {
        /// Update the in memory metadata
        auto new_metadata = table->getInMemoryMetadata();

        if (!request_data.alter_commands.empty())
        {
            AlterCommands alter_commands;
            alter_commands.reserve(request_data.alter_commands.size());
            for (const auto & alter_command : request_data.alter_commands)
            {
                if (auto command = AlterCommand::parse(alter_command); command)
                {
                    alter_commands.push_back(std::move(*command));
                    try
                    {
                        alter_commands.validate(table, global_context);
                    }
                    catch (const DB::Exception & ex)
                    {
                        /// validation may fail because schema was changed by previous commits and can never be applied so ignore the alter command
                        LOG_ERROR(logger, "Ignore invalid alter stream schema command: cmd={} what={}", alter_command, ex.what());
                        return DB::ErrorCodes::OK;
                    }
                }
            }

            alter_commands.prepare(new_metadata);
            alter_commands.apply(new_metadata, global_context);

            {
                std::unique_lock<std::timed_mutex> alter_lock;
                table->applyAlterCommandsToAllShards(alter_commands, global_context, alter_lock);
            }
            updateInMemoryMetadataForStream(table, table_id, desc, new_metadata);

            /// The MV schema (columns) is now updated in updateMetadataByCreateQuery() when the SELECT query changes.
            if (auto * mv = table->as<StorageMaterializedView>())
                mv->onAlterQuery();
        }
        else
        {
            LOG_ERROR(logger, "schema change command is empty, table={}", table_id.getNameForLogs());
            return DB::ErrorCodes::LOGICAL_ERROR;
        }
    }
    else
    {
        LOG_ERROR(logger, "'{}' is not found", table_id.getNameForLogs());
        return DB::ErrorCodes::UNKNOWN_STREAM;
    }

    desc.data_version++;
    desc.last_modify_timestamp_ms = request_data.last_modified;
    desc.last_modified_by = request_data.modified_by;

    return DB::ErrorCodes::OK;
}

void MetadataUpdater::updateInMemoryMetadataForStream(
    StoragePtr table,
    const StorageID & table_id,
    const cluster::protocol::StreamDescriptor & desc,
    StorageInMemoryMetadata & new_metadata) const
{
    auto updated_ast = getCreateTableFromStreamDescription(desc, global_context);
    updateMetadataByCreateQuery(updated_ast, new_metadata, table, global_context, /*only_update_metadata=*/false);
    new_metadata.setVersion(desc.data_version + 1);
    table->setInMemoryMetadata(new_metadata);

    /// Update the new AST after applying the memory metadata and other changes
    {
        auto new_create_query = parseCreateQueryFromAST(updated_ast, table_id.database_name, table_id.table_name);
        table->setInMemoryCreateQuery(new_create_query);
    }
}

void MetadataUpdater::handleRenameStream(
    const cluster::RenameStreamRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto stream_info = request_data.doString();

    LOG_INFO(logger, "Start RenameStream {{{}}}", stream_info);
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End RenameStream {{{}}}, took={}ms", stream_info, stopwatch.elapsedMilliseconds()); });

    auto db = DatabaseCatalog::instance().tryGetDatabase(request_data.ns);
    if (!db)
    {
        LOG_ERROR(logger, "Failed to rename stream {} because database {} doesn't exist", request_data.stream, request_data.ns);
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::UNKNOWN_DATABASE, "");
        return;
    }

    if (db->isTableExist(request_data.new_stream, global_context))
    {
        LOG_ERROR(
            logger,
            "Failed to rename stream {} because {}.{} already exists",
            request_data.stream,
            request_data.ns,
            request_data.new_stream);
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::STREAM_ALREADY_EXISTS, "");
        return;
    }

    auto table = db->tryGetTable(request_data.stream, global_context);
    if (!table)
    {
        LOG_ERROR(
            logger, "Failed to rename stream {} because Stream {}.{} not found", request_data.stream, request_data.ns, request_data.stream);
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::RESOURCE_NOT_FOUND, "");
        return;
    }

    auto result = meta_store->getStreamLocal(request_data.ns, request_data.stream, /*versions_requested=*/1);
    if (result.hasError())
    {
        LOG_ERROR(
            logger,
            "Failed to rename stream {} when get stream descriptor from MetaDB, error={}",
            request_data.stream,
            result.err.string());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, result.err.error_code, std::move(result.err.error_message));
        return;
    }
    if (result.result.empty())
    {
        LOG_ERROR(logger, "Failed to rename stream because {} not found in MetaDB", request_data.stream);
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::RESOURCE_NOT_FOUND, "");
        return;
    }

    auto & desc = result.result.back();
    desc->stream.name = request_data.new_stream;
    desc->last_modify_timestamp_ms = DB::UTCMilliseconds::now();

    /// we first update the in memory metadata, then update the meta db(follow the alterStreamSchema logic)
    auto ret = doHandleRenameStream(request_data, desc, db, table);
    if (ret != DB::ErrorCodes::OK)
    {
        LOG_ERROR(logger, "Failed to rename stream in memory, error={}", ret);
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, ret, "");
        return;
    }

    desc->data_version++;
    auto rename_err = meta_store->getMetaDB().renameStream(request_data.stream, *desc, cluster::AppliedSequence(sn));
    if (rename_err.hasError())
    {
        LOG_ERROR(logger, "Failed to rename stream in MetaDB, error={}", rename_err.string());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, rename_err.error_code, std::move(rename_err.error_message));
        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, "");
}

/// We already know this is the same database instance
/// No exchange support needed
/// No dictionary handling needed
int MetadataUpdater::doHandleRenameStream(
    const cluster::protocol::RenameStreamRequestData & request_data,
    cluster::protocol::StreamDescriptorPtr & desc,
    DatabasePtr db,
    StoragePtr table) const
{
    const auto & old_name = request_data.stream;
    const auto & new_name = request_data.new_stream;
    const auto & database = request_data.ns;

    DB::DatabaseAtomic & atomic_db = dynamic_cast<DatabaseAtomic &>(*db);

    try
    {
        table->checkTableCanBeRenamed(global_context);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "Rename stream {}.{} Failed: {}", database, old_name, e.message());
        return e.code();
    }

    auto old_table_id = table->getStorageID();
    DB::StorageID new_table_id{database, new_name, old_table_id.uuid};

    try
    {
        table->preRename(new_table_id);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "Rename stream {}.{} Failed: {}", database, old_name, e.message());
        return e.code();
    }

    table->renameInMemory(new_table_id);

    auto inmemory_metadata = table->getInMemoryMetadata();
    updateInMemoryMetadataForStream(table, old_table_id, *desc, inmemory_metadata);

    atomic_db.renameTableInMemory(old_name, new_name, /*table_data_path=*/"", table);

    return ErrorCodes::OK;
}
}
