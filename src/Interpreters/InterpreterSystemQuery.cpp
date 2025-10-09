#include <Interpreters/InterpreterSystemQuery.h>
#include <Common/DNSResolver.h>
#include <Common/ActionLock.h>
#include <Common/typeid_cast.h>
#include <Common/SymbolIndex.h>
#include <Common/escapeForFileName.h>
#include <Common/ShellCommand.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/QueryViewsLog.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/AsynchronousInsertLog.h>
#include <IO/S3/BlobStorageLogWriter.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <IO/copyData.h>
#include <Access/ContextAccess.h>
#include <Access/Common/AllowedClientHosts.h>
#include <Disks/IStoragePolicy.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageURL.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/ThreadFuzzer.h>
#include <csignal>

/// proton: starts
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/DropFormatSchemaCacheRequest.h>
#include <Storages/Stream/StorageStream.h>
/// proton: ends

#if USE_AWS_S3
#include <IO/S3/Client.h>
#endif

#include "config.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_KILL;
    extern const int NOT_IMPLEMENTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int STREAM_WAS_NOT_DROPPED;
    /// proton: starts
    extern const int CANNOT_CREATE_TASK;
    extern const int CANNOT_DROP_FORMAT_SCHEMA_CACHE;
    /// proton: ends
}


namespace ActionLocks
{
    extern StorageActionBlockType PartsMerge;
    extern StorageActionBlockType PartsFetch;
    extern StorageActionBlockType PartsSend;
    extern StorageActionBlockType ReplicationQueue;
    extern StorageActionBlockType DistributedSend;
    extern StorageActionBlockType PartsTTLMerge;
    extern StorageActionBlockType PartsMove;
}


namespace
{

ExecutionStatus getOverallExecutionStatusOfCommands()
{
    return ExecutionStatus(0);
}

/// Consequently tries to execute all commands and generates final exception message for failed commands
template <typename Callable, typename ... Callables>
ExecutionStatus getOverallExecutionStatusOfCommands(Callable && command, Callables && ... commands)
{
    ExecutionStatus status_head(0);
    try
    {
        command();
    }
    catch (...)
    {
        status_head = ExecutionStatus::fromCurrentException();
    }

    ExecutionStatus status_tail = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);

    auto res_status = status_head.code != 0 ? status_head.code : status_tail.code;
    auto res_message = status_head.message + (status_tail.message.empty() ? "" : ("\n" + status_tail.message));

    return ExecutionStatus(res_status, res_message);
}

/// Consequently tries to execute all commands and throws exception with info about failed commands
template <typename ... Callables>
void executeCommandsAndThrowIfError(Callables && ... commands)
{
    auto status = getOverallExecutionStatusOfCommands(std::forward<Callables>(commands)...);
    if (status.code != 0)
        throw Exception::createDeprecated(status.message, status.code);
}


AccessType getRequiredAccessType(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return AccessType::SYSTEM_MERGES;
    else if (action_type == ActionLocks::PartsFetch)
        return AccessType::SYSTEM_FETCHES;
    else if (action_type == ActionLocks::PartsSend)
        return AccessType::SYSTEM_REPLICATED_SENDS;
    else if (action_type == ActionLocks::ReplicationQueue)
        return AccessType::SYSTEM_REPLICATION_QUEUES;
    else if (action_type == ActionLocks::DistributedSend)
        return AccessType::SYSTEM_DISTRIBUTED_SENDS;
    else if (action_type == ActionLocks::PartsTTLMerge)
        return AccessType::SYSTEM_TTL_MERGES;
    else if (action_type == ActionLocks::PartsMove)
        return AccessType::SYSTEM_MOVES;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown action type: {}", std::to_string(action_type));
}
}

/// Implements SYSTEM [START|STOP] <something action from ActionLocks>
void InterpreterSystemQuery::startStopAction(StorageActionBlockType action_type, bool start)
{
    auto manager = getContext()->getActionLocksManager();
    manager->cleanExpired();

    auto access = getContext()->getAccess();
    auto required_access_type = getRequiredAccessType(action_type);

    if (volume_ptr && action_type == ActionLocks::PartsMerge)
    {
        access->checkAccess(required_access_type);
        volume_ptr->setAvoidMergesUserOverride(!start);
    }
    else if (table_id)
    {
        access->checkAccess(required_access_type, table_id.database_name, table_id.table_name);
        auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        if (table)
        {
            if (start)
            {
                manager->remove(table, action_type);
                table->onActionLockRemove(action_type);
            }
            else
                manager->add(table, action_type);
        }
    }
    else
    {
        for (auto & elem : DatabaseCatalog::instance().getDatabases())
        {
            for (auto iterator = elem.second->getTablesIterator(getContext()); iterator->isValid(); iterator->next())
            {
                StoragePtr table = iterator->table();
                if (!table)
                    continue;

                if (!access->isGranted(required_access_type, elem.first, iterator->name()))
                {
                    LOG_INFO(log, "Access {} denied, skipping {}.{}", toString(required_access_type), elem.first, iterator->name());
                    continue;
                }

                if (start)
                {
                    manager->remove(table, action_type);
                    table->onActionLockRemove(action_type);
                }
                else
                    manager->add(table, action_type);
            }
        }
    }
}


InterpreterSystemQuery::InterpreterSystemQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_->clone()), log(getLogger("InterpreterSystemQuery"))
{
}


BlockIO InterpreterSystemQuery::execute()
{
    auto & query = query_ptr->as<ASTSystemQuery &>();

    using Type = ASTSystemQuery::Type;

    /// Use global context with fresh system profile settings
    auto system_context = Context::createCopy(getContext()->getGlobalContext());
    system_context->setSetting("profile", getContext()->getSystemProfileName());

    /// Make canonical query for simpler processing
    if (query.type == Type::RELOAD_DICTIONARY)
    {
        if (query.database)
            query.setTable(query.getDatabase() + "." + query.getTable());
    }
    else if (query.table)
    {
        table_id = getContext()->resolveStorageID(StorageID(query.getDatabase(), query.getTable()), Context::ResolveOrdinary);
    }


    volume_ptr = {};
    if (!query.storage_policy.empty() && !query.volume.empty())
        volume_ptr = getContext()->getStoragePolicy(query.storage_policy)->getVolumeByName(query.volume);

    switch (query.type)
    {
        case Type::SHUTDOWN:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            if (kill(0, SIGTERM))
                throwFromErrno("System call kill(0, SIGTERM) failed", ErrorCodes::CANNOT_KILL);
            break;
        }
        case Type::KILL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            /// Exit with the same code as it is usually set by shell when process is terminated by SIGKILL.
            /// It's better than doing 'raise' or 'kill', because they have no effect for 'init' process (with pid = 0, usually in Docker).
            LOG_INFO(log, "Exit immediately as the SYSTEM KILL command has been issued.");
            _exit(128 + SIGKILL);
            // break; /// unreachable
        }
        case Type::SUSPEND:
        {
            getContext()->checkAccess(AccessType::SYSTEM_SHUTDOWN);
            auto command = fmt::format("kill -STOP {0} && sleep {1} && kill -CONT {0}", getpid(), query.seconds);
            LOG_DEBUG(log, "Will run {}", command);
            auto res = ShellCommand::execute(command);
            res->in.close();
            WriteBufferFromOwnString out;
            copyData(res->out, out);
            copyData(res->err, out);
            if (!out.str().empty())
                LOG_DEBUG(log, "The command {} returned output: {}", command, out.str());
            res->wait();
            break;
        }
        case Type::DROP_DNS_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_DNS_CACHE);
            DNSResolver::instance().dropCache();
            /// Reinitialize clusters to update their resolved_addresses
            /// system_context->reloadClusterConfig();
            break;
        }
        case Type::DROP_MARK_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context->dropMarkCache();
            break;
        case Type::DROP_UNCOMPRESSED_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context->dropUncompressedCache();
            break;
        case Type::DROP_INDEX_MARK_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MARK_CACHE);
            system_context->dropIndexMarkCache();
            break;
        case Type::DROP_INDEX_UNCOMPRESSED_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_UNCOMPRESSED_CACHE);
            system_context->dropIndexUncompressedCache();
            break;
        case Type::DROP_MMAP_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_MMAP_CACHE);
            system_context->dropMMappedFileCache();
            break;
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_COMPILED_EXPRESSION_CACHE);
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->reset();
            break;
#endif
#if USE_AWS_S3
        case Type::DROP_S3_CLIENT_CACHE:
            getContext()->checkAccess(AccessType::SYSTEM_DROP_S3_CLIENT_CACHE);
            S3::ClientCacheRegistry::instance().clearCacheForAll();
            break;
#endif

        case Type::DROP_FILESYSTEM_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_FILESYSTEM_CACHE);

            if (query.filesystem_cache_name.empty())
            {
                auto caches = FileCacheFactory::instance().getAll();
                for (const auto & [_, cache_data] : caches)
                    cache_data->cache->removeAllReleasable();
            }
            else
            {
                auto cache = FileCacheFactory::instance().getByName(query.filesystem_cache_name).cache;
                cache->removeAllReleasable();
            }
            break;
        }
        case Type::DROP_SCHEMA_CACHE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_DROP_SCHEMA_CACHE);
            std::unordered_set<String> caches_to_drop;
            if (query.schema_cache_storage.empty())
                caches_to_drop = {"FILE", "S3", "HDFS", "URL"};
            else
                caches_to_drop = {query.schema_cache_storage};

            if (caches_to_drop.contains("FILE"))
                StorageFile::getSchemaCache(getContext()).clear();
#if USE_AWS_S3
            if (caches_to_drop.contains("S3"))
                StorageS3::getSchemaCache(getContext()).clear();
#endif
#if USE_HDFS
            if (caches_to_drop.contains("HDFS"))
                StorageHDFS::getSchemaCache(getContext()).clear();
#endif
            if (caches_to_drop.contains("URL"))
                StorageURL::getSchemaCache(getContext()).clear();
            break;
        }
        case Type::DROP_FORMAT_SCHEMA_CACHE:
        {
            const auto & current_context = getContext();
            current_context->checkAccess(AccessType::SYSTEM_DROP_FORMAT_SCHEMA_CACHE);
            std::unordered_set<String> caches_to_drop;
            if (query.schema_cache_format.empty())
                caches_to_drop = {"Protobuf"};
            else
                caches_to_drop = {query.schema_cache_format};

            /// proton: starts
            auto timeout_ms = current_context->getSettingsRef().query_timeout_sec * 1000;

            auto & metastore = Globals::getMetaStore();
            auto drop_cache_req = std::make_shared<cluster::DropFormatSchemaCacheRequest>(
                    std::move(caches_to_drop), current_context->getNodeID(), timeout_ms, /*request_version_=*/1);
            auto system_command_resp = metastore.dropFormatSchemaCache(std::move(drop_cache_req));
            if (system_command_resp->hasError())
            {
                const auto & err = system_command_resp->error();
                throw Exception(
                    ErrorCodes::CANNOT_DROP_FORMAT_SCHEMA_CACHE,
                    "Failed to drop format schema cache for {}: error_code={} error_message={}",
                    drop_cache_req->data().doString(),
                    err.error_code,
                    err.error_message);
            }
            /// proton: ends
            break;
        }
        case Type::RELOAD_DICTIONARY:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);

            auto & external_dictionaries_loader = system_context->getExternalDictionariesLoader();
            external_dictionaries_loader.reloadDictionary(query.getTable(), getContext());

            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_DICTIONARIES:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_DICTIONARY);
            executeCommandsAndThrowIfError(
                [&] { system_context->getExternalDictionariesLoader().reloadAllTriedToLoad(); },
                [&] { system_context->getEmbeddedDictionaries().reload(); }
            );
            ExternalDictionariesLoader::resetAll();
            break;
        }
        case Type::RELOAD_MODEL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto & external_models_loader = system_context->getExternalModelsLoader();
            external_models_loader.reloadModel(query.target_model);
            break;
        }
        case Type::RELOAD_MODELS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_MODEL);

            auto & external_models_loader = system_context->getExternalModelsLoader();
            external_models_loader.reloadAllTriedToLoad();
            break;
        }
        case Type::RELOAD_FUNCTION:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_FUNCTION);
            LOG_INFO(log, "External function is loaded from MetaStore, Reload function is deprecated.");
            break;
        }
        case Type::RELOAD_FUNCTIONS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_FUNCTION);
            LOG_INFO(log, "External function is loaded from MetaStore, Reload function is deprecated.");
            break;
        }
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_EMBEDDED_DICTIONARIES);
            system_context->getEmbeddedDictionaries().reload();
            break;
        case Type::RELOAD_CONFIG:
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            system_context->reloadConfig();
            break;
        case Type::RELOAD_SYMBOLS:
        {
#if defined(__ELF__) && !defined(OS_FREEBSD)
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_SYMBOLS);
            SymbolIndex::reload();
            break;
#else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SYSTEM RELOAD SYMBOLS is not supported on current platform");
#endif
        }
        case Type::STOP_MERGES:
            startStopAction(ActionLocks::PartsMerge, false);
            break;
        case Type::START_MERGES:
            startStopAction(ActionLocks::PartsMerge, true);
            break;
        case Type::STOP_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, false);
            break;
        case Type::START_TTL_MERGES:
            startStopAction(ActionLocks::PartsTTLMerge, true);
            break;
        case Type::STOP_MOVES:
            startStopAction(ActionLocks::PartsMove, false);
            break;
        case Type::START_MOVES:
            startStopAction(ActionLocks::PartsMove, true);
            break;
        case Type::STOP_FETCHES:
            startStopAction(ActionLocks::PartsFetch, false);
            break;
        case Type::START_FETCHES:
            startStopAction(ActionLocks::PartsFetch, true);
            break;
        case Type::STOP_REPLICATED_SENDS:
            startStopAction(ActionLocks::PartsSend, false);
            break;
        case Type::START_REPLICATED_SENDS:
            startStopAction(ActionLocks::PartsSend, true);
            break;
        case Type::STOP_REPLICATION_QUEUES:
            startStopAction(ActionLocks::ReplicationQueue, false);
            break;
        case Type::START_REPLICATION_QUEUES:
            startStopAction(ActionLocks::ReplicationQueue, true);
            break;
        case Type::STOP_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, false);
            break;
        case Type::START_DISTRIBUTED_SENDS:
            startStopAction(ActionLocks::DistributedSend, true);
            break;
        case Type::RESTART_DISK:
            restartDisk(query.disk);
        case Type::FLUSH_LOGS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_FLUSH_LOGS);
            executeCommandsAndThrowIfError(
                [&] { if (auto query_log = getContext()->getQueryLog()) query_log->flush(true); },
                [&] { if (auto part_log = getContext()->getPartLog("")) part_log->flush(true); },
                [&] { if (auto query_thread_log = getContext()->getQueryThreadLog()) query_thread_log->flush(true); },
                [&] { if (auto trace_log = getContext()->getTraceLog()) trace_log->flush(true); },
                [&] { if (auto text_log = getContext()->getTextLog()) text_log->flush(true); },
                [&] { if (auto metric_log = getContext()->getMetricLog()) metric_log->flush(true); },
                [&] { if (auto asynchronous_metric_log = getContext()->getAsynchronousMetricLog()) asynchronous_metric_log->flush(true); },
                [&] { if (auto opentelemetry_span_log = getContext()->getOpenTelemetrySpanLog()) opentelemetry_span_log->flush(true); },
                [&] { if (auto query_views_log = getContext()->getQueryViewsLog()) query_views_log->flush(true); },
                [&] { if (auto session_log = getContext()->getSessionLog()) session_log->flush(true); },
                [&] { if (auto processors_profile_log = getContext()->getProcessorsProfileLog()) processors_profile_log->flush(true); },
                [&] { if (auto cache_log = getContext()->getFilesystemCacheLog()) cache_log->flush(true); },
                [&] { if (auto asynchronous_insert_log = getContext()->getAsynchronousInsertLog()) asynchronous_insert_log->flush(true); }
            );
            break;
        }
        case Type::STOP_LISTEN_QUERIES:
        case Type::START_LISTEN_QUERIES:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not supported yet", query.type);
        case Type::STOP_THREAD_FUZZER:
            getContext()->checkAccess(AccessType::SYSTEM_THREAD_FUZZER);
            ThreadFuzzer::stop();
            break;
        case Type::START_THREAD_FUZZER:
            getContext()->checkAccess(AccessType::SYSTEM_THREAD_FUZZER);
            ThreadFuzzer::start();
            break;
        /// proton: starts
        case Type::PAUSE_STREAM:
        {
            executePauseStream(query);
            break;
        }
        case Type::RESUME_STREAM:
        {
            executeResumeStream(query);
            break;
        }
        case Type::RECOVER_STREAM:
        {
            executeRecoverStream(query);
            break;
        }
        case Type::PAUSE_MATERIALIZED_VIEW:
        case Type::RESUME_MATERIALIZED_VIEW:
        case Type::ABORT_MATERIALIZED_VIEW:
        case Type::RECOVER_MATERIALIZED_VIEW:
        {
            executeMaterializedViewAdmission(query);
            break;
        }
        case Type::SET_LOG_LEVEL:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            executeSetLogLevel(query);
            break;
        }
        case Type::SHOW_LOGGERS:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            return executeShowLoggers(query);
        }
        case Type::PAUSE_TASK:
        {
            executePauseTask(query);
            break;
        }
        case Type::RESUME_TASK:
        {
            executeResumeTask(query);
            break;
        }
        case Type::INSTALL_PYTHON_PACKAGE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            executeInstallPythonPackage(query);
#if USE_PYTHON_UDF
            break;
#endif
        }
        case Type::UNINSTALL_PYTHON_PACKAGE:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            executeUninstallPythonPackage(query);
#if USE_PYTHON_UDF
            break;
#endif
        }
        case Type::LIST_PYTHON_PACKAGES:
        {
            getContext()->checkAccess(AccessType::SYSTEM_RELOAD_CONFIG);
            return executeListPythonPackages(query);
        }
        /// proton: ends
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of SYSTEM query");
    }

    return BlockIO();
}

void InterpreterSystemQuery::flushDistributed(ASTSystemQuery &)
{
    getContext()->checkAccess(AccessType::SYSTEM_FLUSH_DISTRIBUTED, table_id);

    if (auto * storage_distributed = dynamic_cast<StorageDistributed *>(DatabaseCatalog::instance().getTable(table_id, getContext()).get()))
        storage_distributed->flushClusterNodesAllData(getContext());
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Stream {} is not distributed", table_id.getNameForLogs());
}

void InterpreterSystemQuery::restartDisk(String & name)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_DISK);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SYSTEM RESTART DISK is not supported");
}


AccessRightsElements InterpreterSystemQuery::getRequiredAccessForDDLOnCluster() const
{
    const auto & query = query_ptr->as<const ASTSystemQuery &>();
    using Type = ASTSystemQuery::Type;
    AccessRightsElements required_access;

    switch (query.type)
    {
        case Type::SHUTDOWN: [[fallthrough]];
        case Type::KILL: [[fallthrough]];
        case Type::SUSPEND:
        {
            required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
            break;
        }
        case Type::DROP_DNS_CACHE: [[fallthrough]];
        case Type::DROP_MARK_CACHE: [[fallthrough]];
        case Type::DROP_MMAP_CACHE: [[fallthrough]];
#if USE_EMBEDDED_COMPILER
        case Type::DROP_COMPILED_EXPRESSION_CACHE: [[fallthrough]];
#endif
        case Type::DROP_UNCOMPRESSED_CACHE:
        case Type::DROP_INDEX_MARK_CACHE:
        case Type::DROP_INDEX_UNCOMPRESSED_CACHE:
        case Type::DROP_FILESYSTEM_CACHE:
        case Type::DROP_SCHEMA_CACHE:
        case Type::DROP_FORMAT_SCHEMA_CACHE:
#if USE_AWS_S3
        case Type::DROP_S3_CLIENT_CACHE:
#endif
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_CACHE);
            break;
        }
        case Type::RELOAD_DICTIONARY: [[fallthrough]];
        case Type::RELOAD_DICTIONARIES: [[fallthrough]];
        case Type::RELOAD_EMBEDDED_DICTIONARIES:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_DICTIONARY);
            break;
        }
        case Type::RELOAD_MODEL: [[fallthrough]];
        case Type::RELOAD_MODELS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_MODEL);
            break;
        }
        case Type::RELOAD_FUNCTION: [[fallthrough]];
        case Type::RELOAD_FUNCTIONS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_FUNCTION);
            break;
        }
        case Type::RELOAD_CONFIG:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_CONFIG);
            break;
        }
        case Type::RELOAD_SYMBOLS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_SYMBOLS);
            break;
        }
        case Type::STOP_MERGES: [[fallthrough]];
        case Type::START_MERGES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MERGES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_TTL_MERGES: [[fallthrough]];
        case Type::START_TTL_MERGES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES);
            else
                required_access.emplace_back(AccessType::SYSTEM_TTL_MERGES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_MOVES: [[fallthrough]];
        case Type::START_MOVES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_MOVES);
            else
                required_access.emplace_back(AccessType::SYSTEM_MOVES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_FETCHES: [[fallthrough]];
        case Type::START_FETCHES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_FETCHES);
            else
                required_access.emplace_back(AccessType::SYSTEM_FETCHES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_DISTRIBUTED_SENDS: [[fallthrough]];
        case Type::START_DISTRIBUTED_SENDS:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_DISTRIBUTED_SENDS, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_REPLICATED_SENDS: [[fallthrough]];
        case Type::START_REPLICATED_SENDS:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATED_SENDS, query.getDatabase(), query.getTable());
            break;
        }
        case Type::STOP_REPLICATION_QUEUES: [[fallthrough]];
        case Type::START_REPLICATION_QUEUES:
        {
            if (!query.table)
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES);
            else
                required_access.emplace_back(AccessType::SYSTEM_REPLICATION_QUEUES, query.getDatabase(), query.getTable());
            break;
        }
        case Type::DROP_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_DROP_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::RESTORE_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTORE_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::SYNC_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_SYNC_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::RESTART_REPLICA:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::RESTART_REPLICAS:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_REPLICA);
            break;
        }
        case Type::WAIT_LOADING_PARTS:
        {
            required_access.emplace_back(AccessType::SYSTEM_WAIT_LOADING_PARTS, query.getDatabase(), query.getTable());
            break;
        }
        case Type::FLUSH_DISTRIBUTED:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_DISTRIBUTED, query.getDatabase(), query.getTable());
            break;
        }
        case Type::FLUSH_LOGS:
        {
            required_access.emplace_back(AccessType::SYSTEM_FLUSH_LOGS);
            break;
        }
        case Type::RESTART_DISK:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTART_DISK);
            break;
        }
        case Type::STOP_LISTEN_QUERIES: break;
        case Type::START_LISTEN_QUERIES: break;
        case Type::STOP_THREAD_FUZZER: break;
        case Type::START_THREAD_FUZZER: break;
        case Type::UNKNOWN: break;
        /// proton : starts
        case Type::PAUSE_STREAM:
        case Type::RESUME_STREAM:
        case Type::PAUSE_MATERIALIZED_VIEW:
        case Type::RESUME_MATERIALIZED_VIEW:
        case Type::ABORT_MATERIALIZED_VIEW:
        {
            required_access.emplace_back(AccessType::SYSTEM_SHUTDOWN);
            break;
        }
        case Type::RECOVER_STREAM:
        case Type::RECOVER_MATERIALIZED_VIEW:
        {
            required_access.emplace_back(AccessType::SYSTEM_RESTORE_REPLICA, query.getDatabase(), query.getTable());
            break;
        }
        case Type::SET_LOG_LEVEL:
        case Type::SHOW_LOGGERS:
        case Type::INSTALL_PYTHON_PACKAGE:
        case Type::UNINSTALL_PYTHON_PACKAGE:
        case Type::LIST_PYTHON_PACKAGES:
        {
            required_access.emplace_back(AccessType::SYSTEM_RELOAD_CONFIG);
            break;
        }
        case Type::PAUSE_TASK:
        case Type::RESUME_TASK:
        {
            break;
        }
        /// proton : ends
        case Type::END: break;
    }
    return required_access;
}

void InterpreterSystemQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr) const
{
    elem.query_kind = "System";
}

/// proton: starts
void InterpreterSystemQuery::updateTaskStatus(String database, const String & task_name, uint32_t new_status)
{
    if (database.empty())
        database = getContext()->getCurrentDatabase();

    getContext()->checkAccess(AccessType::SYSTEM_TASK, database, task_name);

    const auto & meta_store = Globals::getMetaStore();
    auto req = std::make_shared<cluster::GetTaskRequest>(
        database,
        task_name,
        /*versions_requested=*/1,
        meta_store.nodeID(),
        /*consistent_read=*/false,
        /*timeout_ms=*/10000,
        /*request_version=*/1);

    auto & metastore = Globals::getMetaStore();
    auto resp = metastore.getTask(std::move(req));
    if (resp->hasError())
        throw Exception(
            resp->error().error_code, "Failed to load task {} in database {}, error={{{}}}", task_name, database, resp->error().string());

    if (resp->data().descs.size() != 1)
        throw Exception(
            resp->error().error_code,
            "Get invalid number of TaskDescriptor: database={} task={} "
            "descriptors={}",
            database,
            task_name,
            resp->data().descs.size());

    auto & desc = resp->data().descs[0];
    if (desc->status == new_status)
        return;

    auto alter_task_req = std::make_shared<cluster::AlterTaskRequest>(
        desc->ns,
        desc->name,
        desc->id,
        desc->data_version,
        new_status,
        getContext()->getUserName(),
        meta_store.nodeID(),
        /*timeout_ms=*/10000,
        /*request_version=*/1);

    auto alter_task_resp = metastore.alterTask(std::move(alter_task_req));
    if (alter_task_resp->hasError())
    {
        const auto & err = alter_task_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_CREATE_TASK,
            "Failed to set task status: database={} task={} "
            "new_status={} error={{{}}}",
            database,
            task_name,
            new_status,
            err.error_message);
    }
}

void InterpreterSystemQuery::executePauseTask(const ASTSystemQuery & system)
{
    updateTaskStatus(system.getDatabase(), system.getTable(), 1);
}

void InterpreterSystemQuery::executeResumeTask(const ASTSystemQuery & system)
{
    updateTaskStatus(system.getDatabase(), system.getTable(), 0);
}
/// proton: ends

}
