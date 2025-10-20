#include <Interpreters/Apply/MetadataUpdater.h>

/// Removed: #include <cstring> - no longer needed after memcpy removal

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/AssignMaterializedViewRequest.h>
#include <Cluster/Requests/CreateDatabaseRequest.h>
#include <Cluster/Requests/DeleteDatabaseRequest.h>

#include <Interpreters/Context.h>
#include <Interpreters/executeSelectQuery.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/LogLevels.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Daemon/BaseDaemon.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int NODE_NOT_EXISTS;
extern const int CANNOT_READ_ALL_DATA;

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

extern const int INVALID_DATA;
extern const int AMBIGUOUS_FORMAT_SCHEMA;
extern const int FORMAT_SCHEMA_ALREADY_EXISTS;
extern const int UNKNOWN_FORMAT_SCHEMA;
extern const int UNKNOWN_FORMAT_SCHEMA_TYPE;

extern const int UNKNOWN_AGGREGATE_FUNCTION;

extern const int PYTHON_PIP_ERROR;
extern const int PYTHON_EXECUTE_ERROR;
extern const int PYTHON_IMPORT_ERROR;
}

MetadataUpdater::MetadataUpdater(const ContextMutablePtr & global_context_)
    : global_context(global_context_)
    , poller(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 1)
    , logger(getLogger("MetadataUpdater"))
{
}

MetadataUpdater::~MetadataUpdater()
{
    shutdown();
}

void MetadataUpdater::startup()
{
    if (started.test_and_set())
        return;

    meta_store = &Globals::getMetaStore();

    poller.scheduleOrThrow([this]() { backgroundPoll(); });
}

void MetadataUpdater::shutdown()
{
    if (stopped.test_and_set())
        return;

    poller.wait();

    LOG_INFO(logger, "Stopped");
}

void MetadataUpdater::backgroundPoll()
{
    setThreadName("MetadataUpdater");

    /// Wait for MetaStore to be ready before starting to poll
    while (!meta_store->isReady() && !stopped.test())
    {
        LOG_INFO(logger, "Waiting for MetaStore to be ready...");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    if (stopped.test())
        return;
        
    LOG_DEBUG(logger, "MetaStore is ready, starting metadata polling");

    /// 1. Direct use of LocalMetaQueue without distributed consensus
    /// 2. No data copying - entries passed directly from queue to apply()
    /// 3. Simplified sequence tracking without epochs (epoch always = 1)
    /// 4. No network overhead - all operations are local
    /// 5. Queue cleanup handled automatically in apply() function
    /// 6. Reduced timeout for local operations (10ms vs 1000ms)

    constexpr int64_t max_wait_ms = 10; /// Local operations - 10ms is plenty
    constexpr uint64_t max_fetch_bytes = 1024 * 1024;

    auto sn = meta_store->lastAppliedSequence();
    if (sn.hasError())
    {
        if (sn.err.error_code == DB::ErrorCodes::RESOURCE_NOT_FOUND)
        {
            LOG_INFO(logger, "No previous applied sequence found, starting from 1");
            next_sn = 1;
        }
        else
        {
            LOG_ERROR(logger, "MetadataUpdater failed to get last applied sn, error={}", sn.err.string());
            return;
        }
    }
    else
    {
        LOG_DEBUG(logger, "MetadataUpdater starts with last applied sequence={{{}}}", sn.result.string());
        next_sn = std::max(sn.result.sn + 1, int64_t(1)); /// Start from 1 if no previous state
    }

    while (!stopped.test())
    {
        try
        {
            /// No fetch_hint or FetchIsolation needed for local queue
            auto fetch_result = meta_store->getLocalMetaQueue()->fetch(next_sn, max_fetch_bytes, max_wait_ms);

            if (!fetch_result.hasError() && fetch_result.result && !fetch_result.result->entries.empty())
            {
                const auto & entries = fetch_result.result->entries;
                auto last_applied = entries.back()->sn;
                auto new_next_sn = last_applied + 1;

                LOG_INFO(logger, "Applying {} entries, sn range [{}, {}]", entries.size(), next_sn, last_applied);

                apply(entries);

                next_sn = new_next_sn;
            }
            else if (fetch_result.hasError())
            {
                LOG_ERROR(logger, "Failed to fetch from LocalMetaQueue at sn={}: {}", next_sn, fetch_result.errorString());
                /// Sleep a bit on error to avoid tight loop
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        catch (...)
        {
            DB::tryLogCurrentException(logger, fmt::format("Failed to process metadata at sn={}", next_sn));
        }
    }
}

/// Replay the DDL in records if the current node is not meta primary and call up layer
/// application code to apply the DDL to update the in-memory presentation
void MetadataUpdater::apply(cluster::EntryPtrs entries)
{
    /// We need make sure every record is successfully applied
    for (const auto & entry : entries)
    {
        uint64_t sn = entry->sn;

        if (!entry->dataEntry() || entry->dataEmpty())
        {
            LOG_INFO(logger, "Skipping {} entry at sn={}", entry->typeString(), sn);

            /// Progress the applied sn since it is a no-op
            /// No need to track "applied" - just cleanup pending and notify client
            meta_store->getMetaDB().deletePendingRequest(sn);
            meta_store->ackProposal(entry->correlation_id, sn, 0, "");
            continue;
        }

        cluster::RequestHeader request_header;
        cluster::RequestAndSize req_with_size;

        try
        {
            req_with_size = cluster::Request::parseWithHeader(entry->data.stringView(), request_header);
        }
        catch (const Exception & ex)
        {
            auto errcode = ex.code();
            if (errcode == ErrorCodes::CANNOT_READ_ALL_DATA || errcode == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            {
                LOG_ERROR(
                    logger,
                    "Failed to parse meta request at sn={} opcode={} request_version={} error={} probably because schema changes",
                    sn,
                    request_header.requestOpCode(),
                    request_header.requestVersion(),
                    ex.message());
            }
            else
            {
                /// We may have inconsistent metadata which is very hard to fix
                /// Still need swallow it to skip this request, otherwise it is a dead loop
                LOG_ERROR(
                    logger,
                    "Failed to parse meta request at sn={} opcode={} request_version={} error={}",
                    sn,
                    request_header.requestOpCode(),
                    request_header.requestVersion(),
                    ex.message());
            }
            continue;
        }

        LOG_INFO(logger, "Applying {} at sn={}", magic_enum::enum_name(request_header.requestOpCode()), sn);

        switch (request_header.requestOpCode())
        {
            case cluster::protocol::OpCode::CreateStream:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateStreamRequest>(req_with_size.request);
                handleCreateStream(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteStream:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteStreamRequest>(req_with_size.request);
                handleDeleteStream(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::AlterStreamSettings:
            {
                auto req = std::dynamic_pointer_cast<cluster::UpdateStreamSettingsRequest>(req_with_size.request);
                handleUpdateStreamSettings(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::AlterStreamSchema:
            {
                auto req = std::dynamic_pointer_cast<cluster::UpdateStreamSchemaRequest>(req_with_size.request);
                handleAlterStreamSchema(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::RenameStream:
            {
                auto req = std::dynamic_pointer_cast<cluster::RenameStreamRequest>(req_with_size.request);
                handleRenameStream(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::CreateUserDefinedFunction:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateUserDefinedFunctionRequest>(req_with_size.request);
                handleCreateUserDefinedFunction(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteUserDefinedFunction:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteUserDefinedFunctionRequest>(req_with_size.request);
                handleDeleteUserDefinedFunction(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::CreateFormatSchema:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateFormatSchemaRequest>(req_with_size.request);
                handleCreateFormatSchema(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteFormatSchema:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteFormatSchemaRequest>(req_with_size.request);
                handleDeleteFormatSchema(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DropFormatSchemaCache:
            {
                auto req = std::dynamic_pointer_cast<cluster::DropFormatSchemaCacheRequest>(req_with_size.request);
                handleDropFormatSchemaCache(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::ChangeLogLevel:
            {
                auto req = std::dynamic_pointer_cast<cluster::ChangeLogLevelRequest>(req_with_size.request);
                handleChangeLogLevel(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::PipPythonPackage:
            {
                auto req = std::dynamic_pointer_cast<cluster::PipPythonPackageRequest>(req_with_size.request);
                handlePipPythonPackage(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::CreateAccessEntity:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateAccessEntityRequest>(req_with_size.request);
                handleCreateAccessEntity(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteAccessEntity:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteAccessEntityRequest>(req_with_size.request);
                handleDeleteAccessEntity(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::AlterAccessEntity:
            {
                auto req = std::dynamic_pointer_cast<cluster::UpdateAccessEntityRequest>(req_with_size.request);
                handleUpdateAccessEntity(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::CreateDatabase:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateDatabaseRequest>(req_with_size.request);
                handleCreateDatabase(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteDatabase:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteDatabaseRequest>(req_with_size.request);
                handleDeleteDatabase(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::CreateDisk:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateDiskRequest>(req_with_size.request);
                handleCreateDisk(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteDisk:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteDiskRequest>(req_with_size.request);
                handleDeleteDisk(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::CreateStoragePolicy:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateStoragePolicyRequest>(req_with_size.request);
                handleCreateStoragePolicy(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteStoragePolicy:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteStoragePolicyRequest>(req_with_size.request);
                handleDeleteStoragePolicy(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::AssignMaterializedView:
            {
                auto req = std::dynamic_pointer_cast<cluster::AssignMaterializedViewRequest>(req_with_size.request);
                handleAssignMaterializedView(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::CreateAlert:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateAlertRequest>(req_with_size.request);
                handleCreateAlert(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteAlert:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteAlertRequest>(req_with_size.request);
                handleDeleteAlert(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::CreateTask:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateTaskRequest>(req_with_size.request);
                handleCreateTask(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteTask:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteTaskRequest>(req_with_size.request);
                handleDeleteTask(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::AlterTask:
            {
                auto req = std::dynamic_pointer_cast<cluster::AlterTaskRequest>(req_with_size.request);
                handleAlterTask(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::CreateNamedCollection:
            {
                auto req = std::dynamic_pointer_cast<cluster::CreateNamedCollectionRequest>(req_with_size.request);
                handleCreateNamedCollection(*req, request_header, sn);
                break;
            }
            case cluster::protocol::OpCode::DeleteNamedCollection:
            {
                auto req = std::dynamic_pointer_cast<cluster::DeleteNamedCollectionRequest>(req_with_size.request);
                handleDeleteNamedCollection(*req, request_header, sn);
                break;
            }
            default:
            {
                LOG_ERROR(logger, "{} is not implemented", magic_enum::enum_name(request_header.requestOpCode()));
                break;
            }
        }
    }

    ///Cleanup after successful apply batch
    if (!entries.empty() && meta_store->getLocalMetaQueue())
    {
        auto last_sn = entries.back()->sn;
        LOG_DEBUG(logger, "Cleaning up LocalMetaQueue entries up to sn={}", last_sn);

        /// Erase processed entries from queue up to last applied
        meta_store->getLocalMetaQueue()->eraseUpTo(last_sn);

        /// Clean up old pending requests from MetaDB (best effort)
        auto cleanup_err = meta_store->getMetaDB().cleanupOldPendingRequests(last_sn);
        if (cleanup_err.hasError())
        {
            LOG_WARNING(logger, "Failed to cleanup pending requests up to sn={}: {}", last_sn, cleanup_err.string());
        }
        else
        {
            LOG_INFO(logger, "Successfully cleaned up pending requests up to sn={}", last_sn);
        }
    }
    else
    {
        LOG_INFO(
            logger,
            "Skipping LocalMetaQueue cleanup: entries.empty()={}, hasLocalMetaQueue()={}",
            entries.empty(),
            meta_store->getLocalMetaQueue() != nullptr);
    }
}

void MetadataUpdater::onRecovery()
{
    auto sn = meta_store->lastAppliedSequence();
    if (sn.hasError())
    {
        LOG_ERROR(logger, "MetadataUpdater failed to get last applied sn on recovery, {}", sn.err.string());
        return;
    }

    /// When no previous applied sequence exists, loadAppliedSequence returns sn = 0
    if (sn.result.sn == 0)
    {
        LOG_INFO(logger, "No previous applied sequence on recovery (first startup), starting from sequence 1");
        next_sn = 1;
        return;
    }

    if (sn.result.sn + 1 != next_sn)
    {
        LOG_INFO(logger, "Reset fetch next_sn from {} to {}", next_sn, sn.result.sn + 1);
        next_sn = sn.result.sn + 1;
    }
}

void MetadataUpdater::handleCreateDatabase(
    const cluster::CreateDatabaseRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto & database_desc = *request_data.database;
    LOG_INFO(logger, "Handling CreateDatabase with database={{{}}}", database_desc.string());

    std::string error_message;
    auto execute_result = DB::ErrorCodes::OK;
    try
    {
        /// Execute DDL to create database
        auto context = Context::createCopy(global_context);
        context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        executeNonInsertQuery(database_desc.sql_def, context, /*callback=*/{}, /*internal=*/true);

        /// Persistent database descriptor when DDL is succeeded.
        if (auto res = meta_store->getMetaDB().saveDatabase(database_desc, cluster::AppliedSequence(sn)); res.hasError())
        {
            LOG_ERROR(logger, "Failed to save database to metaDB: name={} error={}", database_desc.name, res.error_code);
            execute_result = res.error_code;
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "Failed to create database, error={}", e.message());
        execute_result = e.code();
        error_message = fmt::format("Failed to create database, error={}", e.message());
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to execute create database");
        execute_result = DB::ErrorCodes::UNKNOWN_EXCEPTION;
    }

    if (execute_result != DB::ErrorCodes::OK)
    {
        LOG_ERROR(logger, "Failed to create database: database={{{}}} error_code={}", database_desc.string(), execute_result);
        meta_store->getMetaDB().deletePendingRequest(sn);
    }

    meta_store->ackProposal(request_header.correlationID(), sn, execute_result, std::move(error_message));
}

void MetadataUpdater::handleDeleteDatabase(
    const cluster::DeleteDatabaseRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto & name = request_data.name;
    const auto & ddl_query = request_data.ddl_query;

    LOG_INFO(logger, "Handling DeleteDatabase with database={} ddl_query={}", name, ddl_query);

    std::string error_message;
    auto execute_result = DB::ErrorCodes::OK;
    try
    {
        /// Execute DDL to delete database
        auto context = Context::createCopy(global_context);
        context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        executeNonInsertQuery(ddl_query, context, /*callback=*/{}, /*internal=*/true);

        /// Persistent database descriptor when DDL is succeeded.
        if (auto res = meta_store->getMetaDB().deleteDatabase(name, cluster::AppliedSequence(sn)); res.hasError())
        {
            LOG_ERROR(logger, "Failed to delete database from MetaDB: name={} {}", name, res.string());

            execute_result = res.error_code;
            error_message.swap(res.error_message);
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "Failed to execute DDL query: error={}", e.message());
        execute_result = e.code();
        error_message = e.message();
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to execute DDL query");
        execute_result = DB::ErrorCodes::UNKNOWN_EXCEPTION;
    }

    if (execute_result != DB::ErrorCodes::OK)
    {
        LOG_ERROR(logger, "Failed to delete database: database={} ddl_query={} error_code={}", name, ddl_query, execute_result);
        meta_store->getMetaDB().deletePendingRequest(sn);
    }

    meta_store->ackProposal(request_header.correlationID(), sn, execute_result, std::move(error_message));
}

void MetadataUpdater::handleAssignMaterializedView(
    cluster::AssignMaterializedViewRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    std::optional<uint64_t> data_version;
    auto assignment_result = meta_store->getMetaDB().getMaterializedViewAssignment(request.data().assignment.mv);
    /// The mv stored in the metastore may have been deleted. We need to check the uuid of the new mv.
    if (!assignment_result.hasError() && assignment_result.result->mv == request.data().assignment.mv)
        data_version = assignment_result.result->data_version;

    auto & req_assignment = request.data().assignment;
    if (data_version)
    {
        if (req_assignment.data_version != data_version)
        {
            LOG_ERROR(
                logger,
                "Stale MaterializedView assignment {}, current data_version={}, request data_version={}",
                request.string(),
                data_version.value(),
                req_assignment.data_version);

            meta_store->ackProposal(
                request_header.correlationID(),
                sn,
                ErrorCodes::METADATA_VERSION_CHANGED,
                fmt::format(
                    "Stale MaterializedView assignment {}, current data_version={}, request data_version={}",
                    request.string(),
                    data_version.value(),
                    req_assignment.data_version));

            return;
        }
        else
        {
            /// Bump up the version
            ++req_assignment.data_version;
        }
    }

    /// Persistent database descriptor when DDL is succeeded.
    if (auto err = meta_store->getMetaDB().saveMaterializedViewAssignment(req_assignment, cluster::AppliedSequence(sn)); err.hasError())
    {
        /// Better handle error
        LOG_ERROR(logger, "Failed to commit MaterializedView assignment {} to MetaDB, {}", request.string(), err.string());
        meta_store->ackProposal(request_header.correlationID(), sn, err.error_code, std::move(err.error_message));

        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

bool MetadataUpdater::isPermanentError(int error_code)
{
    static std::unordered_set<int> permanent_errors = {
        DB::ErrorCodes::UNKNOWN_STREAM,
        DB::ErrorCodes::UNKNOWN_FUNCTION,
        DB::ErrorCodes::UNKNOWN_IDENTIFIER,
        DB::ErrorCodes::UNKNOWN_TYPE,
        DB::ErrorCodes::UNKNOWN_DATABASE,
        DB::ErrorCodes::UNKNOWN_EXCEPTION,
        DB::ErrorCodes::UNKNOWN_DISK,
        DB::ErrorCodes::INVALID_DISK,

        DB::ErrorCodes::NOT_IMPLEMENTED,
        DB::ErrorCodes::UNSUPPORTED,
        DB::ErrorCodes::UNSUPPORTED_PARAMETER,
        DB::ErrorCodes::TYPE_MISMATCH,
        DB::ErrorCodes::RESOURCE_NOT_FOUND,
        DB::ErrorCodes::RESOURCE_NOT_INITED,
        DB::ErrorCodes::BAD_ARGUMENTS,
        DB::ErrorCodes::BAD_REQUEST_PARAMETER,
        DB::ErrorCodes::UDF_INVALID_NAME,
        DB::ErrorCodes::UNSUPPORTED_METHOD,
        DB::ErrorCodes::INCORRECT_DATA,
        DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        DB::ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
        DB::ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW,
        DB::ErrorCodes::ARGUMENT_OUT_OF_BOUND,
        DB::ErrorCodes::UNABLE_TO_SKIP_UNUSED_SHARDS,
        DB::ErrorCodes::TOO_MANY_ROWS,
        DB::ErrorCodes::TOO_LARGE_STRING_SIZE,
        DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE,
        DB::ErrorCodes::HAVE_DEPENDENT_OBJECTS,

        DB::ErrorCodes::UDF_INTERNAL_ERROR,
        DB::ErrorCodes::SYNTAX_ERROR,
        DB::ErrorCodes::LOGICAL_ERROR,
        DB::ErrorCodes::INCORRECT_QUERY,

        DB::ErrorCodes::STREAM_ALREADY_EXISTS,
        DB::ErrorCodes::FUNCTION_ALREADY_EXISTS,
        DB::ErrorCodes::DIRECTORY_DOESNT_EXIST,
        DB::ErrorCodes::QUERY_WAS_CANCELLED,

        DB::ErrorCodes::CANNOT_DROP_FUNCTION,
        DB::ErrorCodes::CANNOT_ASSIGN_ALTER,
        DB::ErrorCodes::CANNOT_CREATE_RECURSIVE_FUNCTION,

        DB::ErrorCodes::INVALID_DATA,
        DB::ErrorCodes::FORMAT_SCHEMA_ALREADY_EXISTS,
        DB::ErrorCodes::AMBIGUOUS_FORMAT_SCHEMA,
        DB::ErrorCodes::UNKNOWN_FORMAT_SCHEMA,
        DB::ErrorCodes::UNKNOWN_FORMAT_SCHEMA_TYPE,

        DB::ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,

        DB::ErrorCodes::PYTHON_PIP_ERROR,
        DB::ErrorCodes::PYTHON_EXECUTE_ERROR,
        DB::ErrorCodes::PYTHON_IMPORT_ERROR,
    };

    return permanent_errors.contains(error_code);
}

/// Execute `func` and check the return ErrorCode. Retry when error code is not OK unless retry limit is reached or meet a PermanentError.
/// Return the error code of the last execution of `func`. Caller is required to check the return value. If it is not OK, usually call `handledFailedRequest`
/// to commit/ack the failure and quit the rest steps of the request handling.
int MetadataUpdater::executeWithRetry(std::function<int()> func) const
{
    using namespace std::literals;
    constexpr auto retry_delay = 1s;
    constexpr int force_retry_error_code = -1;

    int err_code = force_retry_error_code;
    const uint64_t max_executions = meta_store->getConfig().metadata_max_retries;
    for (uint64_t i = 0; i < max_executions; ++i)
    {
        try
        {
            err_code = func();
        }
        catch (const DB::Exception & e)
        {
            err_code = e.code();
            LOG_ERROR(logger, "Exception on metadata updater execution: {}", e.displayText());
        }
        catch (...)
        {
            err_code = force_retry_error_code;
            tryLogCurrentException(logger, "Exception on metadata updater execution");
        }

        if (err_code == DB::ErrorCodes::OK)
            break;

        if (isPermanentError(err_code))
            break;

        std::this_thread::sleep_for(retry_delay);
        LOG_WARNING(logger, "Retry metadata updater execution: error={}", err_code);
    }

    if (err_code == force_retry_error_code)
        err_code = DB::ErrorCodes::UNKNOWN_EXCEPTION;

    return err_code;
}

/// Commit and ack metadata request on error.
void MetadataUpdater::handleFailedRequest(uint64_t correlation_id, uint64_t sn, int err_code, std::string && err_msg) const
{
    LOG_ERROR(logger, "Metadata update failed: {}: correlation_id=0x{:x} sn={} err_code={}", err_msg, correlation_id, sn, err_code);
    /// Just cleanup pending request after failure
    meta_store->getMetaDB().deletePendingRequest(sn);
    meta_store->ackProposal(correlation_id, sn, err_code, std::move(err_msg));
}

void MetadataUpdater::cleanupPendingRequestIfNeeded(uint64_t sn) const
{
    // Delete the pending request after successful apply
    {
        auto err = meta_store->getMetaDB().deletePendingRequest(sn);
        if (err.hasError() && !err.isNotFound())
        {
            LOG_DEBUG(logger, "Failed to delete pending request for sn={}: {}", sn, err.string());
        }
    }
}
}
