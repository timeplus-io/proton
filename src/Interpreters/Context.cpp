#include <map>
#include <optional>
#include <memory>
#include <Poco/Base64Encoder.h>
#include <Poco/Base64Decoder.h>
#include <Poco/UUID.h>
#include <Poco/Util/Application.h>
#include <Common/Macros.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/Throttler.h>
#include <Common/thread_local_rng.h>
#include <Common/FieldVisitorToString.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/callOnce.h>
#include <Common/SharedLockGuard.h>
#include <Coordination/KeeperDispatcher.h>
#include <Core/BackgroundSchedulePool.h>
#include <Formats/FormatFactory.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/CompressionCodecSelector.h>
#include <Storages/StorageS3Settings.h>
#include <Disks/DiskLocal.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ThreadPoolReader.h>
#include <IO/SynchronousReader.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Core/Settings.h>
#include <Core/SettingsQuirks.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/QuotaUsage.h>
#include <Access/User.h>
#include <Access/SettingsProfile.h>
#include <Access/SettingsProfilesInfo.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/GSSAcceptor.h>
#include <Backups/BackupFactory.h>
#include <Dictionaries/Embedded/GeoDictionariesLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Functions/UserDefined/ExternalUserDefinedFunctionsLoader.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/createUserDefinedSQLObjectsLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Interpreters/TraceCollector.h>
#include <IO/UncompressedCache.h>
#include <IO/MMappedFileCache.h>
#include <IO/WriteSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/StackTrace.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/logger_useful.h>
#include <base/EnumReflection.h>
#include <Common/RemoteHostFilter.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/MergeTree/MergeTreeMetadataCache.h>
#include <Interpreters/Lemmatizers.h>
#include <Interpreters/TransactionLog.h>
#include <filesystem>

#if USE_ROCKSDB
#include <rocksdb/table.h>
#endif

/// proton: starts
#include <Access/Authentication.h>
#include <Coordination/MetaStoreDispatcher.h>
#include <Core/SettingsUtil.h>
#include <Interpreters/Streaming/MetaStoreJSONConfigRepository.h>
#include <KafkaLog/KafkaWALPool.h>
#include <base/getFQDNOrHostName.h>
#include <Common/ProtonCommon.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/regex.hpp>
#include <boost/algorithm/string/split.hpp>
/// proton: ends

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event ContextLock;
    extern const Event ContextLockWaitMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric ContextLockWait;
    extern const Metric BackgroundMovePoolTask;
    extern const Metric BackgroundSchedulePoolTask;
    extern const Metric BackgroundBufferFlushSchedulePoolTask;
    extern const Metric BackgroundDistributedSchedulePoolTask;
    extern const Metric BackgroundMessageBrokerSchedulePoolTask;
    extern const Metric BackgroundMergesAndMutationsPoolTask;
    extern const Metric BackgroundFetchesPoolTask;
    extern const Metric BackgroundCommonPoolTask;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BAD_GET;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_STREAM;
    extern const int STREAM_ALREADY_EXISTS;
    extern const int THERE_IS_NO_SESSION;
    extern const int THERE_IS_NO_QUERY;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int STREAM_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_READ_METHOD;
    extern const int NOT_IMPLEMENTED;
    /// proton: starts
    extern const int ACCESS_DENIED;
    extern const int INVALID_POLL_ID;
    extern const int UNKNOWN_USER;
    /// proton: ends
}

/** Set of known objects (environment), that could be used in query.
  * Shared (global) part. Order of members (especially, order of destruction) is very important.
  */
struct ContextSharedPart : boost::noncopyable
{
    Poco::Logger * log = &Poco::Logger::get("Context");

    /// For access of most of shared objects.
    mutable ContextSharedMutex mutex;
    /// Separate mutex for access of dictionaries. Separate mutex to avoid locks when server doing request to itself.
    mutable std::mutex embedded_dictionaries_mutex;
    mutable std::mutex external_dictionaries_mutex;
    mutable std::mutex external_user_defined_executable_functions_mutex;
    mutable std::mutex external_models_mutex;
    /// Separate mutex for storage policies. During server startup we may
    /// initialize some important storages (system logs with MergeTree engine)
    /// under context lock.
    mutable std::mutex storage_policies_mutex;
    /// Separate mutex for re-initialization of zookeeper session. This operation could take a long time and must not interfere with another operations.
    mutable std::mutex zookeeper_mutex;

    mutable zkutil::ZooKeeperPtr zookeeper TSA_GUARDED_BY(zookeeper_mutex);                 /// Client for ZooKeeper.
    ConfigurationPtr zookeeper_config TSA_GUARDED_BY(zookeeper_mutex);                      /// Stores zookeeper configs

#if USE_NURAFT
    mutable std::mutex keeper_dispatcher_mutex;
    mutable std::shared_ptr<KeeperDispatcher> keeper_dispatcher TSA_GUARDED_BY(keeper_dispatcher_mutex);

    /// proton: starts.
    mutable std::mutex metastore_dispatcher_mutex;
    mutable std::shared_ptr<MetaStoreDispatcher> metastore_dispatcher TSA_GUARDED_BY(metastore_dispatcher_mutex);
    /// proton: ends.

#endif
    mutable std::mutex auxiliary_zookeepers_mutex;
    mutable std::map<String, zkutil::ZooKeeperPtr> auxiliary_zookeepers TSA_GUARDED_BY(auxiliary_zookeepers_mutex);    /// Map for auxiliary ZooKeeper clients.
    ConfigurationPtr auxiliary_zookeepers_config;           /// Stores auxiliary zookeepers configs

    /// No lock required for interserver_io_host, interserver_io_port, interserver_scheme modified only during initialization
    String interserver_io_host;                             /// The host name by which this server is available for other servers.
    UInt16 interserver_io_port = 0;                         /// and port.
    String interserver_scheme;                              /// http or https
    MultiVersion<InterserverCredentials> interserver_io_credentials;

    String path TSA_GUARDED_BY(mutex);                                            /// Path to the data directory, with a slash at the end.
    String flags_path TSA_GUARDED_BY(mutex);                                      /// Path to the directory with some control flags for server maintenance.
    String user_files_path TSA_GUARDED_BY(mutex);                                 /// Path to the directory with user provided files, usable by 'file' table function.
    String dictionaries_lib_path TSA_GUARDED_BY(mutex);                           /// Path to the directory with user provided binaries and libraries for external dictionaries.
    String user_scripts_path TSA_GUARDED_BY(mutex);                               /// Path to the directory with user provided scripts.
    ConfigurationPtr config TSA_GUARDED_BY(mutex);                                /// Global configuration settings.
    String tmp_path TSA_GUARDED_BY(mutex);                                        /// Path to the temporary files that occur when processing the request.
    mutable VolumePtr tmp_volume TSA_GUARDED_BY(mutex);                           /// Volume for the the temporary files that occur when processing the request.

    /// FIXME(yokofly): This value is currently unused and will be removed in the commit referenced below on May 3, 2022.
    /// See: https://github.com/ClickHouse/ClickHouse/commit/5257ce31f86cd3852a8a30343596ad029c56b083#diff-c7c4cea868f661341c1e9866836dc34c1c88723f9f33b4e09db530c2ea074036L2843-R2854
    /// Therefore, I did not add TSA_GUARDED_BY.
    mutable VolumePtr backups_volume;                       /// Volume for all the backups.

    mutable std::unique_ptr<EmbeddedDictionaries> embedded_dictionaries TSA_GUARDED_BY(embedded_dictionaries_mutex);    /// Metrica's dictionaries. Have lazy initialization.
    mutable std::unique_ptr<ExternalDictionariesLoader> external_dictionaries_loader TSA_GUARDED_BY(external_dictionaries_mutex);
    /// proton: starts
    /// mutable std::unique_ptr<ExternalUserDefinedFunctionsLoader> external_user_defined_executable_functions_loader TSA_GUARDED_BY(external_user_defined_executable_functions_mutex);
    /// proton: ends
    mutable std::unique_ptr<ExternalModelsLoader> external_models_loader TSA_GUARDED_BY(external_models_mutex);

    ExternalLoaderXMLConfigRepository * external_models_config_repository TSA_GUARDED_BY(external_models_mutex) = nullptr;
    scope_guard models_repository_guard TSA_GUARDED_BY(external_models_mutex);

    ExternalLoaderXMLConfigRepository * external_dictionaries_config_repository TSA_GUARDED_BY(external_dictionaries_mutex) = nullptr;
    scope_guard dictionaries_xmls TSA_GUARDED_BY(external_dictionaries_mutex);

    /// proton: starts
    Streaming::MetaStoreJSONConfigRepository * user_defined_executable_functions_config_repository TSA_GUARDED_BY(external_user_defined_executable_functions_mutex) = nullptr;
    /// proton: ends
    scope_guard user_defined_executable_functions_xmls TSA_GUARDED_BY(external_user_defined_executable_functions_mutex);

    mutable OnceFlag user_defined_sql_objects_loader_initialized;
    mutable std::unique_ptr<IUserDefinedSQLObjectsLoader> user_defined_sql_objects_loader;

#if USE_NLP
    mutable OnceFlag synonyms_extensions_initialized;
    mutable std::optional<SynonymsExtensions> synonyms_extensions;

    mutable OnceFlag lemmatizers_initialized;
    mutable std::optional<Lemmatizers> lemmatizers;
#endif

    String default_profile_name;                            /// Default profile name used for default values.
    String system_profile_name;                             /// Profile used by system processes
    String buffer_profile_name;                             /// Profile used by Buffer engine for flushing to the underlying
    std::unique_ptr<AccessControl> access_control TSA_GUARDED_BY(mutex);
    mutable UncompressedCachePtr uncompressed_cache TSA_GUARDED_BY(mutex);        /// The cache of decompressed blocks.
    mutable MarkCachePtr mark_cache TSA_GUARDED_BY(mutex);                        /// Cache of marks in compressed files.
    mutable OnceFlag load_marks_threadpool_initialized;
    mutable std::unique_ptr<ThreadPool> load_marks_threadpool; /// Threadpool for loading marks cache.
    mutable UncompressedCachePtr index_uncompressed_cache TSA_GUARDED_BY(mutex);  /// The cache of decompressed blocks for MergeTree indices.
    mutable MarkCachePtr index_mark_cache TSA_GUARDED_BY(mutex);                  /// Cache of marks in compressed files of MergeTree indices.
    mutable MMappedFileCachePtr mmap_cache TSA_GUARDED_BY(mutex); /// Cache of mmapped files to avoid frequent open/map/unmap/close and to reuse from several threads.
    ProcessList process_list;                               /// Executing queries at the moment.
    GlobalOvercommitTracker global_overcommit_tracker;
    MergeList merge_list;                                   /// The list of executable merge (for (Replicated)?MergeTree)
    ConfigurationPtr users_config TSA_GUARDED_BY(mutex);                          /// Config with the users, profiles and quotas sections.
    InterserverIOHandler interserver_io_handler;            /// Handler for interserver communication.

    OnceFlag part_commit_pool_initialized;
    mutable std::unique_ptr<ThreadPool> part_commit_pool; /// proton: A thread pool that can build part and commit in background (used for Stream table engine)
    OnceFlag buffer_flush_schedule_pool_initialized;
    mutable std::unique_ptr<BackgroundSchedulePool> buffer_flush_schedule_pool; /// A thread pool that can do background flush for Buffer tables.
    OnceFlag schedule_pool_initialized;
    mutable std::unique_ptr<BackgroundSchedulePool> schedule_pool;    /// A thread pool that can run different jobs in background (used in replicated tables)
    OnceFlag distributed_schedule_pool_initialized;
    mutable std::unique_ptr<BackgroundSchedulePool> distributed_schedule_pool; /// A thread pool that can run different jobs in background (used for distributed sends)
    OnceFlag message_broker_schedule_pool_initialized;
    mutable std::unique_ptr<BackgroundSchedulePool> message_broker_schedule_pool; /// A thread pool that can run different jobs in background (used for message brokers, like RabbitMQ and Kafka)

    mutable OnceFlag readers_initialized;
    mutable std::unique_ptr<IAsynchronousReader> asynchronous_remote_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> asynchronous_local_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> synchronous_local_fs_reader;

    mutable OnceFlag threadpool_writer_initialized;
    mutable std::unique_ptr<ThreadPool> threadpool_writer;

    mutable ThrottlerPtr remote_read_throttler;             /// A server-wide throttler for remote IO reads
    mutable ThrottlerPtr remote_write_throttler;            /// A server-wide throttler for remote IO writes

    MultiVersion<Macros> macros;                            /// Substitutions extracted from config.
    /// Rules for selecting the compression settings, depending on the size of the part.
    mutable std::unique_ptr<CompressionCodecSelector> compression_codec_selector TSA_GUARDED_BY(mutex);
    /// Storage disk chooser for MergeTree engines
    mutable std::shared_ptr<const DiskSelector> merge_tree_disk_selector TSA_GUARDED_BY(storage_policies_mutex);
    /// Storage policy chooser for MergeTree engines
    mutable std::shared_ptr<const StoragePolicySelector> merge_tree_storage_policy_selector TSA_GUARDED_BY(storage_policies_mutex);

    /// proton: starts. remove `replicated` and add `stream`
    std::optional<StreamSettings> stream_settings TSA_GUARDED_BY(mutex);       /// Settings of Stream* engines.
    /// proton: ends.

    std::atomic_size_t max_stream_size_to_drop = 50000000000lu; /// Protects MergeTree tables from accidental DROP (50GB by default)
    std::atomic_size_t max_partition_size_to_drop = 50000000000lu; /// Protects MergeTree partitions from accidental DROP (50GB by default)
    /// No lock required for format_schema_path modified only during initialization
    String format_schema_path;                              /// Path to a directory that contains schema files used by input formats.
    mutable OnceFlag action_locks_manager_initialized;
    ActionLocksManagerPtr action_locks_manager;             /// Set of storages' action lockers
    OnceFlag system_logs_initialized;
    std::unique_ptr<SystemLogs> system_logs TSA_GUARDED_BY(mutex);                /// Used to log queries and operations on parts
    std::optional<StorageS3Settings> storage_s3_settings TSA_GUARDED_BY(mutex);   /// Settings of S3 storage
    std::vector<String> warnings TSA_GUARDED_BY(mutex);                           /// Store warning messages about server configuration.

    /// Background executors for *MergeTree tables
    /// Has background executors for MergeTree tables been initialized?
    mutable ContextSharedMutex background_executors_mutex;
    bool is_background_executors_initialized TSA_GUARDED_BY(background_executors_mutex) = false;
    MergeMutateBackgroundExecutorPtr merge_mutate_executor TSA_GUARDED_BY(background_executors_mutex);
    OrdinaryBackgroundExecutorPtr moves_executor TSA_GUARDED_BY(background_executors_mutex);
    OrdinaryBackgroundExecutorPtr fetch_executor TSA_GUARDED_BY(background_executors_mutex);
    OrdinaryBackgroundExecutorPtr common_executor TSA_GUARDED_BY(background_executors_mutex);

    RemoteHostFilter remote_host_filter TSA_GUARDED_BY(mutex); /// Allowed URL from config.xml

    /// No lock required for trace_collector modified only during initialization
    std::optional<TraceCollector> trace_collector;        /// Thread collecting traces from threads executing queries

    /// Clusters for distributed tables
    /// Initialized on demand (on distributed storages initialization) since Settings should be initialized
    mutable std::mutex clusters_mutex;                       /// Guards clusters and clusters_config
    std::shared_ptr<Clusters> clusters TSA_GUARDED_BY(clusters_mutex);
    ConfigurationPtr clusters_config TSA_GUARDED_BY(clusters_mutex);                        /// Stores updated configs

    /// No lock required for async_insert_queue modified only during initialization
    std::shared_ptr<AsynchronousInsertQueue> async_insert_queue;

    std::map<String, UInt16> server_ports;

    std::atomic<bool> shutdown_called = false;

    Stopwatch uptime_watch TSA_GUARDED_BY(mutex);

    /// No lock required for application_type modified only during initialization
    Context::ApplicationType application_type = Context::ApplicationType::SERVER;

    /// No lock required for config_reload_callback modified only during initialization
    Context::ConfigReloadCallback config_reload_callback;

    bool is_server_completely_started TSA_GUARDED_BY(mutex) = false;

#if USE_ROCKSDB
    /// Global merge tree metadata cache, stored in rocksdb.
    MergeTreeMetadataCachePtr merge_tree_metadata_cache;
#endif

    ContextSharedPart()
        : access_control(std::make_unique<AccessControl>())
        , global_overcommit_tracker(&process_list)
        , macros(std::make_unique<Macros>())
    {
        /// TODO: make it singleton (?)
        static std::atomic<size_t> num_calls{0};
        if (++num_calls > 1)
        {
            std::cerr << "Attempting to create multiple ContextShared instances. Stack trace:\n" << StackTrace().toString();
            std::cerr.flush();
            std::terminate();
        }
    }


    ~ContextSharedPart()
    {
        /// Wait for thread pool for background reads and writes,
        /// since it may use per-user MemoryTracker which will be destroyed here.
        if (asynchronous_remote_fs_reader)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing remote fs threadpool reader");
                asynchronous_remote_fs_reader->wait();
                asynchronous_remote_fs_reader.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        if (asynchronous_local_fs_reader)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing local fs threadpool reader");
                asynchronous_local_fs_reader->wait();
                asynchronous_local_fs_reader.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        if (synchronous_local_fs_reader)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing local fs threadpool reader");
                synchronous_local_fs_reader->wait();
                synchronous_local_fs_reader.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        if (threadpool_writer)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing threadpool writer");
                threadpool_writer->wait();
                threadpool_writer.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        if (load_marks_threadpool)
        {
            try
            {
                LOG_DEBUG(log, "Destructing marks loader");
                load_marks_threadpool->wait();
                load_marks_threadpool.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        try
        {
            shutdown();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void setConfig(const ConfigurationPtr & config_value)
    {
        if (!config_value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Set nullptr config is invalid");

        std::lock_guard lock(mutex);
        config = config_value;
        access_control->setExternalAuthenticatorsConfig(*config_value);
    }

    const Poco::Util::AbstractConfiguration & getConfigRefWithLock(const std::lock_guard<ContextSharedMutex> &) const TSA_REQUIRES(this->mutex)
    {
        return config ? *config : Poco::Util::Application::instance().config();
    }

    const Poco::Util::AbstractConfiguration & getConfigRef() const
    {
        SharedLockGuard lock(mutex);
        return config ? *config : Poco::Util::Application::instance().config();
    }

    /** Perform a complex job of destroying objects in advance.
      */
    void shutdown() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        bool is_shutdown_called = shutdown_called.exchange(true);
        if (is_shutdown_called)
            return;

        /// Stop periodic reloading of the configuration files.
        /// This must be done first because otherwise the reloading may pass a changed config
        /// to some destroyed parts of ContextSharedPart.
        if (external_dictionaries_loader)
            external_dictionaries_loader->enablePeriodicUpdates(false);
        if (user_defined_sql_objects_loader)
            user_defined_sql_objects_loader->stopWatching();

        if (external_models_loader)
            external_models_loader->enablePeriodicUpdates(false);

        Session::shutdownNamedSessions();

        /**  After system_logs have been shut down it is guaranteed that no system table gets created or written to.
          *  Note that part changes at shutdown won't be logged to part log.
          */
        if (system_logs)
            system_logs->shutdown();

        DatabaseCatalog::shutdown();

        if (merge_mutate_executor)
            merge_mutate_executor->wait();
        if (fetch_executor)
            fetch_executor->wait();
        if (moves_executor)
            moves_executor->wait();
        if (common_executor)
            common_executor->wait();

        TransactionLog::shutdownIfAny();

        std::unique_ptr<SystemLogs> delete_system_logs;
        std::unique_ptr<IUserDefinedSQLObjectsLoader> delete_user_defined_sql_objects_loader;
        std::unique_ptr<EmbeddedDictionaries> delete_embedded_dictionaries;
        std::unique_ptr<ExternalDictionariesLoader> delete_external_dictionaries_loader;
        /// proton: starts
        /// std::unique_ptr<ExternalUserDefinedExecutableFunctionsLoader> delete_external_user_defined_executable_functions_loader;
        /// proton: ends
        std::unique_ptr<ExternalModelsLoader> delete_external_models_loader;
        std::unique_ptr<ThreadPool> delete_part_commit_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_buffer_flush_schedule_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_schedule_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_distributed_schedule_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_message_broker_schedule_pool;
        std::unique_ptr<AccessControl> delete_access_control;

        {
            std::lock_guard lock(mutex);

            /** Compiled expressions stored in cache need to be destroyed before destruction of static objects.
              * Because CHJIT instance can be static object.
              */
#if USE_EMBEDDED_COMPILER
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->reset();
#endif

            /// Preemptive destruction is important, because these objects may have a refcount to ContextShared (cyclic reference).
            /// TODO: Get rid of this.

            /// Dictionaries may be required:
            /// - for storage shutdown (during final flush of the Buffer engine)
            /// - before storage startup (because of some streaming of, i.e. Kafka, to
            ///   the table with materialized column that has dictGet)
            ///
            /// So they should be created before any storages and preserved until storages will be terminated.
            ///
            /// But they cannot be created before storages since they may required table as a source,
            /// but at least they can be preserved for storage termination.
            dictionaries_xmls.reset();
            user_defined_executable_functions_xmls.reset();
            models_repository_guard.reset();

            delete_system_logs = std::move(system_logs);
            delete_user_defined_sql_objects_loader = std::move(user_defined_sql_objects_loader);
            delete_embedded_dictionaries = std::move(embedded_dictionaries);
            delete_external_dictionaries_loader = std::move(external_dictionaries_loader);
            /// proton: starts
            /// delete_external_user_defined_executable_functions_loader = std::move(external_user_defined_executable_functions_loader);
            /// proton: ends
            delete_external_models_loader = std::move(external_models_loader);
            delete_buffer_flush_schedule_pool = std::move(buffer_flush_schedule_pool);
            delete_schedule_pool = std::move(schedule_pool);
            delete_distributed_schedule_pool = std::move(distributed_schedule_pool);
            delete_message_broker_schedule_pool = std::move(message_broker_schedule_pool);
            delete_part_commit_pool = std::move(part_commit_pool);
            delete_access_control = std::move(access_control);

            /// Stop trace collector if any
            trace_collector.reset();
            /// Stop zookeeper connection
            zookeeper.reset();

#if USE_ROCKSDB
            /// Shutdown merge tree metadata cache
            if (merge_tree_metadata_cache)
            {
                merge_tree_metadata_cache->shutdown();
                merge_tree_metadata_cache.reset();
            }
#endif
        }

        /// Can be removed without context lock
        delete_system_logs.reset();
        delete_user_defined_sql_objects_loader.reset();
        delete_embedded_dictionaries.reset();
        delete_external_dictionaries_loader.reset();
        /// proton: starts
        /// delete_external_user_defined_executable_functions_loader.reset();
        /// proton: ends
        delete_external_models_loader.reset();
        delete_buffer_flush_schedule_pool.reset();
        delete_schedule_pool.reset();
        delete_distributed_schedule_pool.reset();
        delete_message_broker_schedule_pool.reset();
        delete_part_commit_pool.reset();
        delete_access_control.reset();

        total_memory_tracker.resetOvercommitTracker();
    }

    bool hasTraceCollector() const
    {
        return trace_collector.has_value();
    }

    void initializeTraceCollector(std::shared_ptr<TraceLog> trace_log)
    {
        if (!trace_log)
            return;
        if (hasTraceCollector())
            return;

        trace_collector.emplace(std::move(trace_log));
    }

    void addWarningMessage(const String & message) TSA_REQUIRES(mutex)
    {
        /// A warning goes both: into server's log; stored to be placed in `system.warnings` table.
        log->warning(message);
        warnings.push_back(message);
    }
};

void ContextSharedMutex::lockImpl()
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    Stopwatch watch;
    Base::lockImpl();
    ProfileEvents::increment(ProfileEvents::ContextLockWaitMicroseconds, watch.elapsedMicroseconds());
}

void ContextSharedMutex::lockSharedImpl()
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    Stopwatch watch;
    Base::lockSharedImpl();
    ProfileEvents::increment(ProfileEvents::ContextLockWaitMicroseconds, watch.elapsedMicroseconds());
}

ContextData::ContextData() = default;
ContextData::ContextData(const ContextData &) = default;

Context::Context() = default;
Context::Context(const Context & rhs) : ContextData(rhs), std::enable_shared_from_this<Context>(rhs) {}

SharedContextHolder::SharedContextHolder(SharedContextHolder &&) noexcept = default;
SharedContextHolder & SharedContextHolder::operator=(SharedContextHolder &&) noexcept = default;
SharedContextHolder::SharedContextHolder() = default;
SharedContextHolder::~SharedContextHolder() = default;
SharedContextHolder::SharedContextHolder(std::unique_ptr<ContextSharedPart> shared_context)
    : shared(std::move(shared_context)) {}

void SharedContextHolder::reset() { shared.reset(); }

ContextMutablePtr Context::createGlobal(ContextSharedPart * shared_part)
{
    auto res = std::shared_ptr<Context>(new Context);
    res->shared = shared_part;
    return res;
}

void Context::initGlobal()
{
    assert(!global_context_instance);
    global_context_instance = shared_from_this();
    DatabaseCatalog::init(shared_from_this());
}

SharedContextHolder Context::createShared()
{
    return SharedContextHolder(std::make_unique<ContextSharedPart>());
}

ContextMutablePtr Context::createCopy(const ContextPtr & other)
{
    /// Ported the PR 'fix race in context::createcopy' from https://github.com/ClickHouse/ClickHouse/pull/49663/files
    /// Tests associated with this PR were not ported. The related tests require additional functions which are not utilized in proton now.

    SharedLockGuard lock(other->mutex);
    return std::shared_ptr<Context>(new Context(*other));
}

ContextMutablePtr Context::createCopy(const ContextWeakPtr & other)
{
    auto ptr = other.lock();
    if (!ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't copy an expired context");
    return createCopy(ptr);
}

ContextMutablePtr Context::createCopy(const ContextMutablePtr & other)
{
    return createCopy(std::const_pointer_cast<const Context>(other));
}

Context::~Context() = default;

InterserverIOHandler & Context::getInterserverIOHandler() { return shared->interserver_io_handler; }

ProcessList & Context::getProcessList() { return shared->process_list; }
const ProcessList & Context::getProcessList() const { return shared->process_list; }
OvercommitTracker * Context::getGlobalOvercommitTracker() const { return &shared->global_overcommit_tracker; }
MergeList & Context::getMergeList() { return shared->merge_list; }
const MergeList & Context::getMergeList() const { return shared->merge_list; }

String Context::resolveDatabase(const String & database_name) const
{
    String res = database_name.empty() ? getCurrentDatabase() : database_name;
    if (res.empty())
        throw Exception("Default database is not selected", ErrorCodes::UNKNOWN_DATABASE);
    return res;
}

String Context::getPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->path;
}

String Context::getFlagsPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->flags_path;
}

String Context::getUserFilesPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->user_files_path;
}

String Context::getDictionariesLibPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->dictionaries_lib_path;
}

String Context::getUserScriptsPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->user_scripts_path;
}

Strings Context::getWarnings() const
{
    Strings common_warnings;
    {
        SharedLockGuard lock(shared->mutex);
        common_warnings = shared->warnings;
    }
    for (const auto & setting : settings)
    {
        if (setting.isValueChanged() && setting.isObsolete())
        {
            common_warnings.emplace_back("Some obsolete setting is changed. "
                                         "Check 'select * from system.settings where changed' and read the changelog.");
            break;
        }
    }
    return common_warnings;
}

VolumePtr Context::getTemporaryVolume() const
{
    std::lock_guard lock(shared->mutex);
    return shared->tmp_volume;
}

void Context::setPath(const String & path)
{
    std::lock_guard lock(shared->mutex);

    shared->path = path;

    if (shared->tmp_path.empty() && !shared->tmp_volume)
        shared->tmp_path = shared->path + "tmp/";

    if (shared->flags_path.empty())
        shared->flags_path = shared->path + "flags/";

    if (shared->user_files_path.empty())
        shared->user_files_path = shared->path + "user_files/";

    if (shared->dictionaries_lib_path.empty())
        shared->dictionaries_lib_path = shared->path + "dictionaries_lib/";

    if (shared->user_scripts_path.empty())
        shared->user_scripts_path = shared->path + "user_scripts/";
}

VolumePtr Context::setTemporaryStorage(const String & path, const String & policy_name)
{
    VolumePtr volume{};
    if (policy_name.empty())
    {
        std::lock_guard lock(shared->mutex);

        shared->tmp_path = path;
        if (!shared->tmp_path.ends_with('/'))
            shared->tmp_path += '/';

        auto disk = std::make_shared<DiskLocal>("_tmp_default", shared->tmp_path, 0);
        volume = std::make_shared<SingleDiskVolume>("_tmp_default", disk, 0);
    }
    else
    {
        std::lock_guard storage_policies_lock(shared->storage_policies_mutex);

        StoragePolicyPtr tmp_policy = getStoragePolicySelector(storage_policies_lock)->get(policy_name);
        if (tmp_policy->getVolumes().size() != 1)
             throw Exception("Policy " + policy_name + " is used temporary files, such policy should have exactly one volume",
                             ErrorCodes::NO_ELEMENTS_IN_CONFIG);
        volume = tmp_policy->getVolume(0);
    }

    std::lock_guard lock(shared->mutex);

    shared->tmp_volume = std::move(volume);
    if (shared->tmp_volume->getDisks().empty())
         throw Exception("No disks volume for temporary files", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    return shared->tmp_volume;
}

void Context::setFlagsPath(const String & path)
{
    std::lock_guard lock(shared->mutex);
    shared->flags_path = path;
}

void Context::setUserFilesPath(const String & path)
{
    std::lock_guard lock(shared->mutex);
    shared->user_files_path = path;
}

void Context::setDictionariesLibPath(const String & path)
{
    std::lock_guard lock(shared->mutex);
    shared->dictionaries_lib_path = path;
}

void Context::setUserScriptsPath(const String & path)
{
    std::lock_guard lock(shared->mutex);
    shared->user_scripts_path = path;
}

void Context::addWarningMessage(const String & msg) const
{
    std::lock_guard lock(shared->mutex);
    shared->addWarningMessage(msg);
}

void Context::setConfig(const ConfigurationPtr & config)
{
    shared->setConfig(config);
}

const Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    return shared->getConfigRef();
}


AccessControl & Context::getAccessControl()
{
    SharedLockGuard lock(shared->mutex);
    return *shared->access_control;
}

const AccessControl & Context::getAccessControl() const
{
    SharedLockGuard lock(shared->mutex);
    return *shared->access_control;
}

void Context::setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);
    shared->access_control->setExternalAuthenticatorsConfig(config);
}

std::unique_ptr<GSSAcceptorContext> Context::makeGSSAcceptorContext() const
{
    SharedLockGuard lock(shared->mutex);
    return std::make_unique<GSSAcceptorContext>(shared->access_control->getExternalAuthenticators().getKerberosParams());
}

void Context::setUsersConfig(const ConfigurationPtr & config)
{
    std::lock_guard lock(shared->mutex);
    shared->users_config = config;
    shared->access_control->setUsersConfig(*shared->users_config);
}

ConfigurationPtr Context::getUsersConfig()
{
    SharedLockGuard lock(shared->mutex);
    return shared->users_config;
}

void Context::setUser(const UUID & user_id_)
{
    std::lock_guard lock(mutex);

    user_id = user_id_;

    access = getAccessControl().getContextAccess(
        user_id_, /* current_roles = */ {}, /* use_default_roles = */ true, settings, current_database, client_info);

    auto user = access->getUser();

    current_roles = std::make_shared<std::vector<UUID>>(user->granted_roles.findGranted(user->default_roles));

    auto default_profile_info = access->getDefaultProfileInfo();
    settings_constraints_and_current_profiles = default_profile_info->getConstraintsAndProfileIDs();
    applySettingsChangesWithLock(default_profile_info->settings, lock);

    if (!user->default_database.empty())
        setCurrentDatabaseWithLock(user->default_database, lock);
}

std::shared_ptr<const User> Context::getUser() const
{
    return getAccess()->getUser();
}

String Context::getUserName() const
{
    return getAccess()->getUserName();
}

std::optional<UUID> Context::getUserID() const
{
    SharedLockGuard lock(mutex);
    return user_id;
}

/// proton: starts
String Context::getPasswordByUserName(const String & user_name) const
{
    if (auto id = getAccessControl().find<User>(user_name))
        if (auto user = getAccessControl().tryRead<User>(*id))
        {
            switch (user->auth_data.getType())
            {
                case AuthenticationType::NO_PASSWORD:
                    return "";
                case AuthenticationType::PLAINTEXT_PASSWORD:
                case AuthenticationType::DOUBLE_SHA1_PASSWORD:
                case AuthenticationType::SHA256_PASSWORD:
                {
                    const auto & password_hash = user->auth_data.getPasswordHashBinary();
                    return String(password_hash.data(), password_hash.data() + password_hash.size());
                }
                /// TODO: Support LDAP and Kerberos authentication type
                case AuthenticationType::LDAP:
                    throw Authentication::Require<BasicCredentials>("Proton LDAP Authentication: " + user->auth_data.getLDAPServerName());
                case AuthenticationType::KERBEROS:
                    throw Authentication::Require<GSSAcceptorContext>(user->auth_data.getKerberosRealm());
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown authentication type: {}", user->auth_data.getType());
            }
        }

    return "";
}

void Context::setUserByName(const String & user_name)
{
    if (auto id = getAccessControl().find<User>(user_name))
        setUser(*id);
    else
        throw Exception(ErrorCodes::UNKNOWN_USER, "User {} doesn't exist", user_name);
}
/// proton: ends

void Context::setQuotaKey(String quota_key_)
{
    std::lock_guard lock(mutex);
    client_info.quota_key = std::move(quota_key_);
}


void Context::setCurrentRolesWithLock(const std::vector<UUID> & current_roles_, const std::lock_guard<ContextSharedMutex> & lock)
{
    if (current_roles ? (*current_roles == current_roles_) : current_roles_.empty())
       return;
    current_roles = std::make_shared<std::vector<UUID>>(current_roles_);
    calculateAccessRightsWithLock(lock);
}

void Context::setCurrentRoles(const std::vector<UUID> & current_roles_)
{
    std::lock_guard lock(mutex);
    setCurrentRolesWithLock(current_roles_, lock);
}

void Context::setCurrentRolesDefault()
{
    auto user = getUser();
    setCurrentRoles(user->granted_roles.findGranted(user->default_roles));
}

boost::container::flat_set<UUID> Context::getCurrentRoles() const
{
    return getRolesInfo()->current_roles;
}

boost::container::flat_set<UUID> Context::getEnabledRoles() const
{
    return getRolesInfo()->enabled_roles;
}

std::shared_ptr<const EnabledRolesInfo> Context::getRolesInfo() const
{
    return getAccess()->getRolesInfo();
}

void Context::calculateAccessRightsWithLock(const std::lock_guard<ContextSharedMutex> & lock)
{
    if (user_id)
        access = getAccessControl().getContextAccess(
            *user_id,
            current_roles ? *current_roles : std::vector<UUID>{},
            /* use_default_roles = */ false,
            settings,
            current_database,
            client_info);
}


template <typename... Args>
void Context::checkAccessImpl(const Args &... args) const
{
    return getAccess()->checkAccess(args...);
}

void Context::checkAccess(const AccessFlags & flags) const { return checkAccessImpl(flags); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database) const { return checkAccessImpl(flags, database); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, const std::string_view & table) const { return checkAccessImpl(flags, database, table); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, const std::string_view & table, const std::string_view & column) const { return checkAccessImpl(flags, database, table, column); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return checkAccessImpl(flags, database, table, columns); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, const std::string_view & table, const Strings & columns) const { return checkAccessImpl(flags, database, table, columns); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName()); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, std::string_view column) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), column); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::vector<std::string_view> & columns) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const Strings & columns) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns); }
void Context::checkAccess(const AccessRightsElement & element) const { return checkAccessImpl(element); }
void Context::checkAccess(const AccessRightsElements & elements) const { return checkAccessImpl(elements); }


std::shared_ptr<const ContextAccess> Context::getAccess() const
{
    SharedLockGuard lock(mutex);
    return access ? access : ContextAccess::getFullAccess();
}

ASTPtr Context::getRowPolicyFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const
{
    SharedLockGuard lock(mutex);
    auto row_filter_of_initial_user = row_policies_of_initial_user ? row_policies_of_initial_user->getFilter(database, table_name, filter_type) : nullptr;
    return getAccess()->getRowPolicyFilter(database, table_name, filter_type, row_filter_of_initial_user);
}

void Context::enableRowPoliciesOfInitialUser()
{
    std::lock_guard lock(mutex);
    row_policies_of_initial_user = nullptr;
    if (client_info.initial_user == client_info.current_user)
        return;
    auto initial_user_id = getAccessControl().find<User>(client_info.initial_user);
    if (!initial_user_id)
        return;
    row_policies_of_initial_user = getAccessControl().tryGetDefaultRowPolicies(*initial_user_id);
}


std::shared_ptr<const EnabledQuota> Context::getQuota() const
{
    return getAccess()->getQuota();
}


std::optional<QuotaUsage> Context::getQuotaUsage() const
{
    return getAccess()->getQuotaUsage();
}


void Context::setCurrentProfileWithLock(const String & profile_name, const std::lock_guard<ContextSharedMutex> & lock)
{
    try
    {
        UUID profile_id = getAccessControl().getID<SettingsProfile>(profile_name);
        setCurrentProfileWithLock(profile_id, lock);
    }
    catch (Exception & e)
    {
        e.addMessage(", while trying to set settings profile {}", profile_name);
        throw;
    }
}

void Context::setCurrentProfileWithLock(const UUID & profile_id, const std::lock_guard<ContextSharedMutex> & lock)
{
    auto profile_info = getAccessControl().getSettingsProfileInfo(profile_id);
    checkSettingsConstraintsWithLock(profile_info->settings);
    applySettingsChangesWithLock(profile_info->settings, lock);
    settings_constraints_and_current_profiles = profile_info->getConstraintsAndProfileIDs(settings_constraints_and_current_profiles);
}

void Context::setCurrentProfile(const String & profile_name)
{
    std::lock_guard lock(mutex);
    setCurrentProfileWithLock(profile_name, lock);
}

void Context::setCurrentProfile(const UUID & profile_id)
{
    std::lock_guard lock(mutex);
    setCurrentProfileWithLock(profile_id, lock);
}

std::vector<UUID> Context::getCurrentProfiles() const
{
    SharedLockGuard lock(mutex);
    return settings_constraints_and_current_profiles->current_profiles;
}

std::vector<UUID> Context::getEnabledProfiles() const
{
    SharedLockGuard lock(mutex);
    return settings_constraints_and_current_profiles->enabled_profiles;
}


const Scalars & Context::getScalars() const
{
    return scalars;
}


const Block & Context::getScalar(const String & name) const
{
    auto it = scalars.find(name);
    if (scalars.end() == it)
    {
        // This should be a logical error, but it fails the sql_fuzz test too
        // often, so 'bad arguments' for now.
        throw Exception("Scalar " + backQuoteIfNeed(name) + " doesn't exist (internal bug)", ErrorCodes::BAD_ARGUMENTS);
    }
    return it->second;
}

const Block * Context::tryGetSpecialScalar(const String & name) const
{
    auto it = special_scalars.find(name);
    if (special_scalars.end() == it)
        return nullptr;
    return &it->second;
}

Tables Context::getExternalTables() const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    SharedLockGuard lock(mutex);

    Tables res;
    for (const auto & table : external_tables_mapping)
        res[table.first] = table.second->getTable();

    auto query_context_ptr = query_context.lock();
    auto session_context_ptr = session_context.lock();
    if (query_context_ptr && query_context_ptr.get() != this)
    {
        Tables buf = query_context_ptr->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    else if (session_context_ptr && session_context_ptr.get() != this)
    {
        Tables buf = session_context_ptr->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    return res;
}


void Context::addExternalTable(const String & table_name, TemporaryTableHolder && temporary_table)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    std::lock_guard lock(mutex);
    if (external_tables_mapping.end() != external_tables_mapping.find(table_name))
        throw Exception("Temporary stream " + backQuoteIfNeed(table_name) + " already exists.", ErrorCodes::STREAM_ALREADY_EXISTS);
    external_tables_mapping.emplace(table_name, std::make_shared<TemporaryTableHolder>(std::move(temporary_table)));
}


std::shared_ptr<TemporaryTableHolder> Context::removeExternalTable(const String & table_name)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    std::shared_ptr<TemporaryTableHolder> holder;
    {
        std::lock_guard lock(mutex);
        auto iter = external_tables_mapping.find(table_name);
        if (iter == external_tables_mapping.end())
            return {};
        holder = iter->second;
        external_tables_mapping.erase(iter);
    }
    return holder;
}


void Context::addScalar(const String & name, const Block & block)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have scalars");

    scalars[name] = block;
}


void Context::addSpecialScalar(const String & name, const Block & block)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have local scalars");

    special_scalars[name] = block;
}


bool Context::hasScalar(const String & name) const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have scalars");

    return scalars.contains(name);
}


void Context::addQueryAccessInfo(
    const String & quoted_database_name,
    const String & full_quoted_table_name,
    const Names & column_names,
    const String & projection_name,
    const String & view_name)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have query access info");

    std::lock_guard<std::mutex> lock(query_access_info.mutex);
    query_access_info.databases.emplace(quoted_database_name);
    query_access_info.tables.emplace(full_quoted_table_name);
    for (const auto & column_name : column_names)
        query_access_info.columns.emplace(full_quoted_table_name + "." + backQuoteIfNeed(column_name));
    if (!projection_name.empty())
        query_access_info.projections.emplace(full_quoted_table_name + "." + backQuoteIfNeed(projection_name));
    if (!view_name.empty())
        query_access_info.views.emplace(view_name);
}

void Context::addQueryFactoriesInfo(QueryLogFactories factory_type, const String & created_object) const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have query factories info");

    std::lock_guard lock(query_factories_info.mutex);

    switch (factory_type)
    {
        case QueryLogFactories::AggregateFunction:
            query_factories_info.aggregate_functions.emplace(created_object);
            break;
        case QueryLogFactories::AggregateFunctionCombinator:
            query_factories_info.aggregate_function_combinators.emplace(created_object);
            break;
        case QueryLogFactories::Database:
            query_factories_info.database_engines.emplace(created_object);
            break;
        case QueryLogFactories::DataType:
            query_factories_info.data_type_families.emplace(created_object);
            break;
        case QueryLogFactories::Dictionary:
            query_factories_info.dictionaries.emplace(created_object);
            break;
        case QueryLogFactories::Format:
            query_factories_info.formats.emplace(created_object);
            break;
        case QueryLogFactories::Function:
            query_factories_info.functions.emplace(created_object);
            break;
        case QueryLogFactories::Storage:
            query_factories_info.storages.emplace(created_object);
            break;
        case QueryLogFactories::TableFunction:
            query_factories_info.table_functions.emplace(created_object);
    }
}


StoragePtr Context::executeTableFunction(const ASTPtr & table_expression)
{
    auto hash = table_expression->getTreeHash();
    String key = toString(hash.first) + '_' + toString(hash.second);

    StoragePtr & res = table_function_results[key];

    if (!res)
    {
        TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_expression, shared_from_this());
        res = table_function_ptr->execute(table_expression, shared_from_this(), table_function_ptr->getName());

        /// Since ITableFunction::parseArguments() may change table_expression, i.e.:
        ///
        ///     remote('127.1', system.one) -> remote('127.1', 'system.one'),
        ///
        auto new_hash = table_expression->getTreeHash();
        if (hash != new_hash)
        {
            key = toString(new_hash.first) + '_' + toString(new_hash.second);
            table_function_results[key] = res;
        }

        return res;
    }

    return res;
}


void Context::addViewSource(const StoragePtr & storage)
{
    if (view_source)
        throw Exception(
            "Temporary view source storage " + backQuoteIfNeed(view_source->getName()) + " already exists.", ErrorCodes::STREAM_ALREADY_EXISTS);
    view_source = storage;
}


StoragePtr Context::getViewSource() const
{
    return view_source;
}

Settings Context::getSettings() const
{
    SharedLockGuard lock(mutex);
    return settings;
}


void Context::setSettings(const Settings & settings_)
{
    std::lock_guard lock(mutex);
    auto old_readonly = settings.readonly;
    auto old_allow_ddl = settings.allow_ddl;
    auto old_allow_introspection_functions = settings.allow_introspection_functions;

    settings = settings_;

    if ((settings.readonly != old_readonly) || (settings.allow_ddl != old_allow_ddl) || (settings.allow_introspection_functions != old_allow_introspection_functions))
        calculateAccessRightsWithLock(lock);
}


void Context::setSettingWithLock(std::string_view name, const String & value, const std::lock_guard<ContextSharedMutex> & lock)
{
    if (name == "profile")
    {
        setCurrentProfileWithLock(value, lock);
        return;
    }
    settings.set(name, value);

    if (name == "readonly" || name == "allow_ddl" || name == "allow_introspection_functions")
        calculateAccessRightsWithLock(lock);
}


void Context::setSettingWithLock(std::string_view name, const Field & value, const std::lock_guard<ContextSharedMutex> & lock)
{
    if (name == "profile")
    {
        setCurrentProfileWithLock(value.safeGet<String>(), lock);
        return;
    }
    settings.set(name, value);

    if (name == "readonly" || name == "allow_ddl" || name == "allow_introspection_functions")
        calculateAccessRightsWithLock(lock);
}

void Context::applySettingChangeWithLock(const SettingChange & change, const std::lock_guard<ContextSharedMutex> & lock)
{
    try
    {
        setSettingWithLock(change.name, change.value, lock);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format(
                         "in attempt to set the value of setting '{}' to {}",
                         change.name, applyVisitor(FieldVisitorToString(), change.value)));
        throw;
    }
}

void Context::applySettingsChangesWithLock(const SettingsChanges & changes, const std::lock_guard<ContextSharedMutex> & lock)
{
    for (const SettingChange & change : changes)
        applySettingChangeWithLock(change, lock);
    applySettingsQuirks(settings);
}

void Context::setSetting(std::string_view name, const String & value)
{
    std::lock_guard lock(mutex);
    setSettingWithLock(name, value, lock);
}

void Context::setSetting(std::string_view name, const Field & value)
{
    std::lock_guard lock(mutex);
    setSettingWithLock(name, value, lock);
}

void Context::applySettingChange(const SettingChange & change)
{
    try
    {
        setSetting(change.name, change.value);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("in attempt to set the value of setting '{}' to {}",
                                 change.name, applyVisitor(FieldVisitorToString(), change.value)));
        throw;
    }
}


void Context::applySettingsChanges(const SettingsChanges & changes)
{
    std::lock_guard lock(mutex);
    applySettingsChangesWithLock(changes, lock);
}

void Context::checkSettingsConstraintsWithLock(const SettingChange & change) const
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(settings, change);
}

void Context::checkSettingsConstraintsWithLock(const SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(settings, changes);
}

void Context::checkSettingsConstraintsWithLock(SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(settings, changes);
}

void Context::clampToSettingsConstraintsWithLock(SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.clamp(settings, changes);
}


void Context::checkSettingsConstraints(const SettingChange & change) const
{
    SharedLockGuard lock(mutex);
    checkSettingsConstraintsWithLock(change);
}

void Context::checkSettingsConstraints(const SettingsChanges & changes) const
{
    SharedLockGuard lock(mutex);
    getSettingsConstraintsAndCurrentProfiles()->constraints.check(settings, changes);
}

void Context::checkSettingsConstraints(SettingsChanges & changes) const
{
    SharedLockGuard lock(mutex);
    checkSettingsConstraintsWithLock(changes);
}

void Context::clampToSettingsConstraints(SettingsChanges & changes) const
{
    SharedLockGuard lock(mutex);
    clampToSettingsConstraintsWithLock(changes);
}

void Context::resetSettingsToDefaultValue(const std::vector<String> & names)
{
    std::lock_guard lock(mutex);
    for (const String & name: names)
        settings.setDefaultValue(name);
}

std::shared_ptr<const SettingsConstraintsAndProfileIDs> Context::getSettingsConstraintsAndCurrentProfilesWithLock() const
{
    if (settings_constraints_and_current_profiles)
        return settings_constraints_and_current_profiles;
    static auto no_constraints_or_profiles = std::make_shared<SettingsConstraintsAndProfileIDs>(getAccessControl());
    return no_constraints_or_profiles;
}

std::shared_ptr<const SettingsConstraintsAndProfileIDs> Context::getSettingsConstraintsAndCurrentProfiles() const
{
    SharedLockGuard lock(mutex);
    return getSettingsConstraintsAndCurrentProfilesWithLock();
}

String Context::getCurrentDatabase() const
{
    SharedLockGuard lock(mutex);
    return current_database;
}


String Context::getInitialQueryId() const
{
    return client_info.initial_query_id;
}


void Context::setCurrentDatabaseNameInGlobalContext(const String & name)
{
    if (!isGlobalContext())
        throw Exception("Cannot set current database for non global context, this method should be used during server initialization",
                        ErrorCodes::LOGICAL_ERROR);
    std::lock_guard lock(mutex);

    if (!current_database.empty())
        throw Exception("Default database name cannot be changed in global context without server restart",
                        ErrorCodes::LOGICAL_ERROR);

    current_database = name;
}

void Context::setCurrentDatabaseWithLock(const String & name, const std::lock_guard<ContextSharedMutex> & lock)
{
    DatabaseCatalog::instance().assertDatabaseExists(name);
    current_database = name;
    calculateAccessRightsWithLock(lock);
}

void Context::setCurrentDatabase(const String & name)
{
    std::lock_guard lock(mutex);
    setCurrentDatabaseWithLock(name, lock);
}

void Context::setCurrentQueryId(const String & query_id)
{
    /// Generate random UUID, but using lower quality RNG,
    ///  because Poco::UUIDGenerator::generateRandom method is using /dev/random, that is very expensive.
    /// NOTE: Actually we don't need to use UUIDs for query identifiers.
    /// We could use any suitable string instead.
    union
    {
        char bytes[16];
        struct
        {
            UInt64 a;
            UInt64 b;
        } words;
        UUID uuid{};
    } random;

    random.words.a = thread_local_rng(); //-V656
    random.words.b = thread_local_rng(); //-V656

    if (client_info.client_trace_context.trace_id != UUID())
    {
        // Use the OpenTelemetry trace context we received from the client, and
        // create a new span for the query.
        query_trace_context = client_info.client_trace_context;
        query_trace_context.span_id = thread_local_rng();
    }
    else if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        // If this is an initial query without any parent OpenTelemetry trace, we
        // might start the trace ourselves, with some configurable probability.
        std::bernoulli_distribution should_start_trace{
            settings.opentelemetry_start_trace_probability};

        if (should_start_trace(thread_local_rng))
        {
            // Use the randomly generated default query id as the new trace id.
            query_trace_context.trace_id = random.uuid;
            query_trace_context.span_id = thread_local_rng();
            // Mark this trace as sampled in the flags.
            query_trace_context.trace_flags = 1;
        }
    }

    String query_id_to_set = query_id;
    if (query_id_to_set.empty())    /// If the user did not submit his query_id, then we generate it ourselves.
    {
        /// Use protected constructor.
        struct QueryUUID : Poco::UUID
        {
            QueryUUID(const char * bytes, Poco::UUID::Version version)
                : Poco::UUID(bytes, version) {}
        };

        query_id_to_set = QueryUUID(random.bytes, Poco::UUID::UUID_RANDOM).toString();
    }

    client_info.current_query_id = query_id_to_set;

    if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        client_info.initial_query_id = client_info.current_query_id;
}

void Context::killCurrentQuery()
{
    if (process_list_elem)
    {
        process_list_elem->cancelQuery(true);
    }
}

String Context::getDefaultFormat() const
{
    return default_format.empty() ? "TabSeparated" : default_format;
}


void Context::setDefaultFormat(const String & name)
{
    default_format = name;
}

MultiVersion<Macros>::Version Context::getMacros() const
{
    return shared->macros.get();
}

void Context::setMacros(std::unique_ptr<Macros> && macros)
{
    shared->macros.set(std::move(macros));
}

ContextMutablePtr Context::getQueryContext() const
{
    auto ptr = query_context.lock();
    if (!ptr) throw Exception("There is no query or query context has expired", ErrorCodes::THERE_IS_NO_QUERY);
    return ptr;
}

bool Context::isInternalSubquery() const
{
    auto ptr = query_context.lock();
    return ptr && ptr.get() != this;
}

ContextMutablePtr Context::getSessionContext() const
{
    auto ptr = session_context.lock();
    if (!ptr) throw Exception("There is no session or session context has expired", ErrorCodes::THERE_IS_NO_SESSION);
    return ptr;
}

ContextMutablePtr Context::getGlobalContext() const
{
    auto ptr = global_context.lock();
    if (!ptr) throw Exception("There is no global context or global context has expired", ErrorCodes::LOGICAL_ERROR);
    return ptr;
}

ContextMutablePtr Context::getBufferContext() const
{
    if (!buffer_context) throw Exception("There is no buffer context", ErrorCodes::LOGICAL_ERROR);
    return buffer_context;
}


const EmbeddedDictionaries & Context::getEmbeddedDictionaries() const
{
    return getEmbeddedDictionariesImpl(false);
}

EmbeddedDictionaries & Context::getEmbeddedDictionaries()
{
    return getEmbeddedDictionariesImpl(false);
}


const ExternalDictionariesLoader & Context::getExternalDictionariesLoader() const
{
    return const_cast<Context *>(this)->getExternalDictionariesLoader();
}

ExternalDictionariesLoader & Context::getExternalDictionariesLoader()
{
    std::lock_guard lock(shared->external_dictionaries_mutex);
    return getExternalDictionariesLoaderWithLock(lock);
}

ExternalDictionariesLoader & Context::getExternalDictionariesLoaderWithLock(const std::lock_guard<std::mutex> &) TSA_REQUIRES(shared->external_dictionaries_mutex)
{
    if (!shared->external_dictionaries_loader)
        shared->external_dictionaries_loader =
            std::make_unique<ExternalDictionariesLoader>(getGlobalContext());
    return *shared->external_dictionaries_loader;
}

const ExternalUserDefinedFunctionsLoader & Context::getExternalUserDefinedExecutableFunctionsLoader() const
{
    return const_cast<Context *>(this)->getExternalUserDefinedExecutableFunctionsLoader();
}

ExternalUserDefinedFunctionsLoader & Context::getExternalUserDefinedExecutableFunctionsLoader()
{
    std::lock_guard lock(shared->external_user_defined_executable_functions_mutex);
    return getExternalUserDefinedExecutableFunctionsLoaderWithLock(lock);
}

ExternalUserDefinedFunctionsLoader & Context::getExternalUserDefinedExecutableFunctionsLoaderWithLock(const std::lock_guard<std::mutex> &) TSA_REQUIRES(shared->external_user_defined_executable_functions_mutex)
{
    return ExternalUserDefinedFunctionsLoader::instance(getGlobalContextInstance());

///    if (!shared->external_user_defined_executable_functions_loader)
///        shared->external_user_defined_executable_functions_loader =
///            std::make_unique<ExternalUserDefinedExecutableFunctionsLoader>(getGlobalContext());
///    return *shared->external_user_defined_executable_functions_loader;
}

const ExternalModelsLoader & Context::getExternalModelsLoader() const
{
    return const_cast<Context *>(this)->getExternalModelsLoader();
}

ExternalModelsLoader & Context::getExternalModelsLoader()
{
    std::lock_guard lock(shared->external_models_mutex);
    return getExternalModelsLoaderWithLock(lock);
}

ExternalModelsLoader & Context::getExternalModelsLoaderWithLock(const std::lock_guard<std::mutex> &) TSA_REQUIRES(shared->external_models_mutex)
{
    if (!shared->external_models_loader)
        shared->external_models_loader =
            std::make_unique<ExternalModelsLoader>(getGlobalContext());
    return *shared->external_models_loader;
}

void Context::loadOrReloadModels(const Poco::Util::AbstractConfiguration & config)
{
    auto patterns_values = getMultipleValuesFromConfig(config, "", "models_config");
    std::unordered_set<std::string> patterns(patterns_values.begin(), patterns_values.end());

    std::lock_guard lock(shared->external_models_mutex);

    auto & external_models_loader = getExternalModelsLoaderWithLock(lock);

    if (shared->external_models_config_repository)
    {
        shared->external_models_config_repository->updatePatterns(patterns);
        external_models_loader.reloadConfig(shared->external_models_config_repository->getName());
        return;
    }

    auto app_path = getPath();
    auto config_path = getConfigRef().getString("config-file", "config.xml");
    auto repository = std::make_unique<ExternalLoaderXMLConfigRepository>(app_path, config_path, patterns);
    shared->external_models_config_repository = repository.get();
    shared->models_repository_guard = external_models_loader.addConfigRepository(std::move(repository));
}

EmbeddedDictionaries & Context::getEmbeddedDictionariesImpl(const bool throw_on_error) const
{
    std::lock_guard lock(shared->embedded_dictionaries_mutex);

    if (!shared->embedded_dictionaries)
    {
        auto geo_dictionaries_loader = std::make_unique<GeoDictionariesLoader>();

        shared->embedded_dictionaries = std::make_unique<EmbeddedDictionaries>(
            std::move(geo_dictionaries_loader),
            getGlobalContext(),
            throw_on_error);
    }

    return *shared->embedded_dictionaries;
}


void Context::tryCreateEmbeddedDictionaries(const Poco::Util::AbstractConfiguration & config) const
{
    if (!config.getBool("dictionaries_lazy_load", true))
        static_cast<void>(getEmbeddedDictionariesImpl(true));
}

void Context::loadOrReloadDictionaries(const Poco::Util::AbstractConfiguration & config)
{
    bool dictionaries_lazy_load = config.getBool("dictionaries_lazy_load", true);
    auto patterns_values = getMultipleValuesFromConfig(config, "", "dictionaries_config");
    std::unordered_set<std::string> patterns(patterns_values.begin(), patterns_values.end());

    std::lock_guard lock(shared->external_dictionaries_mutex);

    auto & external_dictionaries_loader = getExternalDictionariesLoaderWithLock(lock);
    external_dictionaries_loader.enableAlwaysLoadEverything(!dictionaries_lazy_load);

    if (shared->external_dictionaries_config_repository)
    {
        shared->external_dictionaries_config_repository->updatePatterns(patterns);
        external_dictionaries_loader.reloadConfig(shared->external_dictionaries_config_repository->getName());
        return;
    }

    auto app_path = getPath();
    auto config_path = getConfigRef().getString("config-file", "config.xml");
    auto repository = std::make_unique<ExternalLoaderXMLConfigRepository>(app_path, config_path, patterns);
    shared->external_dictionaries_config_repository = repository.get();
    shared->dictionaries_xmls = external_dictionaries_loader.addConfigRepository(std::move(repository));
}

/// proton: starts
void Context::loadOrReloadUserDefinedExecutableFunctions()
{
    std::lock_guard lock(shared->external_user_defined_executable_functions_mutex);
    auto & external_user_defined_executable_functions_loader_ = getExternalUserDefinedExecutableFunctionsLoaderWithLock(lock);
    if (shared->user_defined_executable_functions_config_repository)
    {
        external_user_defined_executable_functions_loader_.reloadConfig(
            shared->user_defined_executable_functions_config_repository->getName());
        return;
    }
    auto repository = std::make_unique<Streaming::MetaStoreJSONConfigRepository>(getMetaStoreDispatcher(), ProtonConsts::UDF_METASTORE_NAMESPACE);
    shared->user_defined_executable_functions_config_repository = repository.get();
    shared->user_defined_executable_functions_xmls
        = external_user_defined_executable_functions_loader_.addConfigRepository(std::move(repository));
}

Streaming::MetaStoreJSONConfigRepository * Context::getMetaStoreJSONConfigRepository() const
{
    std::lock_guard lock(shared->external_user_defined_executable_functions_mutex);
    if (!shared->user_defined_executable_functions_config_repository)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MetaStoreJSONConfigRepository must be created first");

    return shared->user_defined_executable_functions_config_repository;
}
/// proton: ends

const IUserDefinedSQLObjectsLoader & Context::getUserDefinedSQLObjectsLoader() const
{
    callOnce(shared->user_defined_sql_objects_loader_initialized, [&] {
        shared->user_defined_sql_objects_loader = createUserDefinedSQLObjectsLoader(getGlobalContext());
    });

    SharedLockGuard lock(shared->mutex);
    return *shared->user_defined_sql_objects_loader;
}

IUserDefinedSQLObjectsLoader & Context::getUserDefinedSQLObjectsLoader()
{
    callOnce(shared->user_defined_sql_objects_loader_initialized, [&] {
        shared->user_defined_sql_objects_loader = createUserDefinedSQLObjectsLoader(getGlobalContext());
    });

    SharedLockGuard lock(shared->mutex);
    return *shared->user_defined_sql_objects_loader;
}

#if USE_NLP

SynonymsExtensions & Context::getSynonymsExtensions() const
{
    callOnce(shared->synonyms_extensions_initialized, [&] {
        shared->synonyms_extensions.emplace(getConfigRef());
    });

    return *shared->synonyms_extensions;
}

Lemmatizers & Context::getLemmatizers() const
{
    callOnce(shared->lemmatizers_initialized, [&] {
        shared->lemmatizers.emplace(getConfigRef());
    });

    return *shared->lemmatizers;
}
#endif

void Context::setProgressCallback(ProgressCallback callback)
{
    /// Callback is set to a session or to a query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    progress_callback = callback;
}

ProgressCallback Context::getProgressCallback() const
{
    return progress_callback;
}


void Context::setProcessListElement(ProcessList::Element * elem)
{
    /// Set to a session or query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    process_list_elem = elem;
}

ProcessList::Element * Context::getProcessListElement() const
{
    return process_list_elem;
}


void Context::setUncompressedCache(size_t max_size_in_bytes)
{
    std::lock_guard lock(shared->mutex);

    if (shared->uncompressed_cache)
        throw Exception("Uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->uncompressed_cache = std::make_shared<UncompressedCache>(max_size_in_bytes);
}


UncompressedCachePtr Context::getUncompressedCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->uncompressed_cache;
}


void Context::dropUncompressedCache() const
{
    std::lock_guard lock(shared->mutex);
    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();
}


void Context::setMarkCache(size_t cache_size_in_bytes)
{
    std::lock_guard lock(shared->mutex);

    if (shared->mark_cache)
        throw Exception("Mark cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->mark_cache = std::make_shared<MarkCache>(cache_size_in_bytes);
}

MarkCachePtr Context::getMarkCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->mark_cache;
}

void Context::dropMarkCache() const
{
    std::lock_guard lock(shared->mutex);
    if (shared->mark_cache)
        shared->mark_cache->reset();
}

ThreadPool & Context::getLoadMarksThreadpool() const
{
    callOnce(shared->load_marks_threadpool_initialized, [&] {
        const auto & config = getConfigRef();
        auto pool_size = config.getUInt(".load_marks_threadpool_pool_size", 50);
        auto queue_size = config.getUInt(".load_marks_threadpool_queue_size", 1000000);
        shared->load_marks_threadpool = std::make_unique<ThreadPool>(pool_size, pool_size, queue_size);
    });

    return *shared->load_marks_threadpool;
}

void Context::setIndexUncompressedCache(size_t max_size_in_bytes)
{
    std::lock_guard lock(shared->mutex);

    if (shared->index_uncompressed_cache)
        throw Exception("Index uncompressed cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->index_uncompressed_cache = std::make_shared<UncompressedCache>(max_size_in_bytes);
}


UncompressedCachePtr Context::getIndexUncompressedCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->index_uncompressed_cache;
}


void Context::dropIndexUncompressedCache() const
{
    std::lock_guard lock(shared->mutex);
    if (shared->index_uncompressed_cache)
        shared->index_uncompressed_cache->reset();
}


void Context::setIndexMarkCache(size_t cache_size_in_bytes)
{
    std::lock_guard lock(shared->mutex);

    if (shared->index_mark_cache)
        throw Exception("Index mark cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->index_mark_cache = std::make_shared<MarkCache>(cache_size_in_bytes);
}

MarkCachePtr Context::getIndexMarkCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->index_mark_cache;
}

void Context::dropIndexMarkCache() const
{
    std::lock_guard lock(shared->mutex);
    if (shared->index_mark_cache)
        shared->index_mark_cache->reset();
}


void Context::setMMappedFileCache(size_t cache_size_in_num_entries)
{
    std::lock_guard lock(shared->mutex);

    if (shared->mmap_cache)
        throw Exception("Mapped file cache has been already created.", ErrorCodes::LOGICAL_ERROR);

    shared->mmap_cache = std::make_shared<MMappedFileCache>(cache_size_in_num_entries);
}

MMappedFileCachePtr Context::getMMappedFileCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->mmap_cache;
}

void Context::dropMMappedFileCache() const
{
    std::lock_guard lock(shared->mutex);
    if (shared->mmap_cache)
        shared->mmap_cache->reset();
}


void Context::dropCaches() const
{
    std::lock_guard lock(shared->mutex);

    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();

    if (shared->mark_cache)
        shared->mark_cache->reset();

    if (shared->index_uncompressed_cache)
        shared->index_uncompressed_cache->reset();

    if (shared->index_mark_cache)
        shared->index_mark_cache->reset();

    if (shared->mmap_cache)
        shared->mmap_cache->reset();
}

BackgroundSchedulePool & Context::getBufferFlushSchedulePool() const
{
    callOnce(shared->buffer_flush_schedule_pool_initialized, [&] {
        shared->buffer_flush_schedule_pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_buffer_flush_schedule_pool_size,
            CurrentMetrics::BackgroundBufferFlushSchedulePoolTask,
            "BgBufSchPool");
    });
    return *shared->buffer_flush_schedule_pool;
}

BackgroundTaskSchedulingSettings Context::getBackgroundProcessingTaskSchedulingSettings() const
{
    BackgroundTaskSchedulingSettings task_settings;

    const auto & config = getConfigRef();
    task_settings.thread_sleep_seconds = config.getDouble("background_processing_pool_thread_sleep_seconds", 10);
    task_settings.thread_sleep_seconds_random_part = config.getDouble("background_processing_pool_thread_sleep_seconds_random_part", 1.0);
    task_settings.thread_sleep_seconds_if_nothing_to_do = config.getDouble("background_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
    task_settings.task_sleep_seconds_when_no_work_min = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_min", 10);
    task_settings.task_sleep_seconds_when_no_work_max = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_max", 600);
    task_settings.task_sleep_seconds_when_no_work_multiplier = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
    task_settings.task_sleep_seconds_when_no_work_random_part = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);
    return task_settings;
}

BackgroundTaskSchedulingSettings Context::getBackgroundMoveTaskSchedulingSettings() const
{
    BackgroundTaskSchedulingSettings task_settings;

    const auto & config = getConfigRef();
    task_settings.thread_sleep_seconds = config.getDouble("background_move_processing_pool_thread_sleep_seconds", 10);
    task_settings.thread_sleep_seconds_random_part = config.getDouble("background_move_processing_pool_thread_sleep_seconds_random_part", 1.0);
    task_settings.thread_sleep_seconds_if_nothing_to_do = config.getDouble("background_move_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
    task_settings.task_sleep_seconds_when_no_work_min = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_min", 10);
    task_settings.task_sleep_seconds_when_no_work_max = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_max", 600);
    task_settings.task_sleep_seconds_when_no_work_multiplier = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
    task_settings.task_sleep_seconds_when_no_work_random_part = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);

    return task_settings;
}

BackgroundSchedulePool & Context::getSchedulePool() const
{
    callOnce(shared->schedule_pool_initialized, [&] {
        shared->schedule_pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_schedule_pool_size,
            CurrentMetrics::BackgroundSchedulePoolTask,
            "BgSchPool");
    });
    return *shared->schedule_pool;
}

BackgroundSchedulePool & Context::getDistributedSchedulePool() const
{
    callOnce(shared->distributed_schedule_pool_initialized, [&] {
        shared->distributed_schedule_pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_distributed_schedule_pool_size,
            CurrentMetrics::BackgroundDistributedSchedulePoolTask,
            "BgDistSchPool");
    });
    return *shared->distributed_schedule_pool;
}

BackgroundSchedulePool & Context::getMessageBrokerSchedulePool() const
{
    callOnce(shared->message_broker_schedule_pool_initialized, [&] {
        shared->message_broker_schedule_pool = std::make_unique<BackgroundSchedulePool>(
            settings.background_message_broker_schedule_pool_size,
            CurrentMetrics::BackgroundMessageBrokerSchedulePoolTask,
            "BgMBSchPool");
    });
    return *shared->message_broker_schedule_pool;
}

ThrottlerPtr Context::getRemoteReadThrottler() const
{
    std::lock_guard lock(mutex);
    if (!shared->remote_read_throttler)
        shared->remote_read_throttler = std::make_shared<Throttler>(
            settings.max_remote_read_network_bandwidth_for_server);

    return shared->remote_read_throttler;
}

ThrottlerPtr Context::getRemoteWriteThrottler() const
{
    std::lock_guard lock(mutex);
    if (!shared->remote_write_throttler)
        shared->remote_write_throttler = std::make_shared<Throttler>(
            settings.max_remote_write_network_bandwidth_for_server);

    return shared->remote_write_throttler;
}

bool Context::hasDistributedDDL() const
{
    return getConfigRef().has("distributed_ddl");
}

void Context::initializeKeeperDispatcher([[maybe_unused]] bool start_async) const
{
#if USE_NURAFT
    std::lock_guard lock(shared->keeper_dispatcher_mutex);

    if (shared->keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize Keeper multiple times");

    const auto & config = getConfigRef();
    if (config.has("keeper_server"))
    {
        bool is_standalone_app = getApplicationType() == ApplicationType::KEEPER;
        if (start_async)
        {
            assert(!is_standalone_app);
            LOG_INFO(shared->log, "Connected to ZooKeeper (or Keeper) before internal Keeper start or we don't depend on our Keeper cluster, "
                     "will wait for Keeper asynchronously");
        }
        else
        {
            LOG_INFO(shared->log, "Cannot connect to ZooKeeper (or Keeper) before internal Keeper start, "
                     "will wait for Keeper synchronously");
        }

        shared->keeper_dispatcher = std::make_shared<KeeperDispatcher>();
        shared->keeper_dispatcher->initialize(config, is_standalone_app, start_async);
    }
#endif
}

#if USE_NURAFT
std::shared_ptr<KeeperDispatcher> & Context::getKeeperDispatcher() const
{
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (!shared->keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Keeper must be initialized before requests");

    return shared->keeper_dispatcher;
}
#endif

void Context::shutdownKeeperDispatcher() const
{
#if USE_NURAFT
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (shared->keeper_dispatcher)
    {
        shared->keeper_dispatcher->shutdown();
        shared->keeper_dispatcher.reset();
    }
#endif
}

/// proton: starts.
void Context::initializeMetaStoreDispatcher() const
{
#if USE_NURAFT
    std::lock_guard lock(shared->metastore_dispatcher_mutex);

    if (shared->metastore_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize MetaStoreServer multiple times");

    const auto & config = getConfigRef();
    if (config.has("metastore_server"))
    {
        shared->metastore_dispatcher = std::make_shared<MetaStoreDispatcher>();
        shared->metastore_dispatcher->initialize(config, getApplicationType() == ApplicationType::METASTORE);
    }
#endif
}

#if USE_ROCKSDB
MergeTreeMetadataCachePtr Context::getMergeTreeMetadataCache() const
{
    auto cache = tryGetMergeTreeMetadataCache();
    if (!cache)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Merge tree metadata cache is not initialized, please add config merge_tree_metadata_cache in config.xml and restart");
    return cache;
}

MergeTreeMetadataCachePtr Context::tryGetMergeTreeMetadataCache() const
{
    return shared->merge_tree_metadata_cache;
}
#endif

#if USE_NURAFT
std::shared_ptr<MetaStoreDispatcher> & Context::getMetaStoreDispatcher() const
{
    std::lock_guard lock(shared->metastore_dispatcher_mutex);
    if (!shared->metastore_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MetaStoreServer must be initialized before requests");

    return shared->metastore_dispatcher;
}
#endif

void Context::shutdownMetaStoreDispatcher() const
{
#if USE_NURAFT
    std::lock_guard lock(shared->metastore_dispatcher_mutex);
    if (shared->metastore_dispatcher)
    {
        shared->metastore_dispatcher->shutdown();
        shared->metastore_dispatcher.reset();
    }
#endif
}
/// proton: ends.

void Context::updateKeeperConfiguration([[maybe_unused]] const Poco::Util::AbstractConfiguration & config)
{
#if USE_NURAFT
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (!shared->keeper_dispatcher)
        return;

    shared->keeper_dispatcher->updateConfiguration(config);
#endif
}

InterserverCredentialsPtr Context::getInterserverCredentials()
{
    return shared->interserver_io_credentials.get();
}

void Context::updateInterserverCredentials(const Poco::Util::AbstractConfiguration & config)
{
    auto credentials = InterserverCredentials::make(config, "interserver_http_credentials");
    shared->interserver_io_credentials.set(std::move(credentials));
}

void Context::setInterserverIOAddress(const String & host, UInt16 port)
{
    shared->interserver_io_host = host;
    shared->interserver_io_port = port;
}

std::pair<String, UInt16> Context::getInterserverIOAddress() const
{
    if (shared->interserver_io_host.empty() || shared->interserver_io_port == 0)
        throw Exception("Parameter 'interserver_http(s)_port' required for replication is not specified in configuration file.",
                        ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    return { shared->interserver_io_host, shared->interserver_io_port };
}

void Context::setInterserverScheme(const String & scheme)
{
    shared->interserver_scheme = scheme;
}

String Context::getInterserverScheme() const
{
    return shared->interserver_scheme;
}

void Context::setRemoteHostFilter(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);
    shared->remote_host_filter.setValuesFromConfig(config);
}

const RemoteHostFilter & Context::getRemoteHostFilter() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->remote_host_filter;
}

UInt16 Context::getTCPPort() const
{
    const auto & config = getConfigRef();
    return config.getInt("tcp_port", DBMS_DEFAULT_PORT);
}

std::optional<UInt16> Context::getTCPPortSecure() const
{
    const auto & config = getConfigRef();
    if (config.has("tcp_port_secure"))
        return config.getInt("tcp_port_secure");
    return {};
}

void Context::registerServerPort(String port_name, UInt16 port)
{
    shared->server_ports.emplace(std::move(port_name), port);
}

UInt16 Context::getServerPort(const String & port_name) const
{
    auto it = shared->server_ports.find(port_name);
    if (it == shared->server_ports.end())
        throw Exception(ErrorCodes::BAD_GET, "There is no port named {}", port_name);
    else
        return it->second;
}

std::shared_ptr<Cluster> Context::getCluster(const std::string & cluster_name) const
{
    if (auto res = tryGetCluster(cluster_name))
        return res;
    throw Exception("Requested cluster '" + cluster_name + "' not found", ErrorCodes::BAD_GET);
}


std::shared_ptr<Cluster> Context::tryGetCluster(const std::string & cluster_name) const
{
    return getClusters()->getCluster(cluster_name);
}


void Context::reloadClusterConfig() const
{
    while (true)
    {
        ConfigurationPtr cluster_config;
        {
            std::lock_guard lock(shared->clusters_mutex);
            cluster_config = shared->clusters_config;
        }

        const auto & config = cluster_config ? *cluster_config : getConfigRef();
        auto new_clusters = std::make_shared<Clusters>(config, settings);

        {
            std::lock_guard lock(shared->clusters_mutex);
            if (shared->clusters_config.get() == cluster_config.get())
            {
                shared->clusters = std::move(new_clusters);
                return;
            }

            // Clusters config has been suddenly changed, recompute clusters
        }
    }
}


std::shared_ptr<Clusters> Context::getClusters() const
{
    std::lock_guard lock(shared->clusters_mutex);
    if (!shared->clusters)
    {
        const auto & config = shared->clusters_config ? *shared->clusters_config : getConfigRef();
        shared->clusters = std::make_shared<Clusters>(config, settings);
    }

    return shared->clusters;
}

/// On repeating calls updates existing clusters and adds new clusters, doesn't delete old clusters
void Context::setClustersConfig(const ConfigurationPtr & config, bool /*enable_discovery*/, const String & config_name)
{
    std::lock_guard lock(shared->clusters_mutex);

    /// Do not update clusters if this part of config wasn't changed.
    if (shared->clusters && isSameConfiguration(*config, *shared->clusters_config, config_name))
        return;

    auto old_clusters_config = shared->clusters_config;
    shared->clusters_config = config;

    if (!shared->clusters)
        shared->clusters = std::make_shared<Clusters>(*shared->clusters_config, settings, config_name);
    else
        shared->clusters->updateClusters(*shared->clusters_config, settings, config_name, old_clusters_config);
}


void Context::setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster)
{
    std::lock_guard lock(shared->clusters_mutex);

    if (!shared->clusters)
        throw Exception("Clusters are not set", ErrorCodes::LOGICAL_ERROR);

    shared->clusters->setCluster(cluster_name, cluster);
}


void Context::initializeSystemLogs()
{
    callOnce(shared->system_logs_initialized, [&] {
        auto system_logs = std::make_unique<SystemLogs>(getGlobalContext(), getConfigRef());
        std::lock_guard lock(shared->mutex);
        shared->system_logs = std::move(system_logs);
    });
}

void Context::initializeTraceCollector()
{
    shared->initializeTraceCollector(getTraceLog());
}

#if USE_ROCKSDB
void Context::initializeMergeTreeMetadataCache(const String & dir, size_t size)
{
    shared->merge_tree_metadata_cache = MergeTreeMetadataCache::create(dir, size);
}
#endif

bool Context::hasTraceCollector() const
{
    return shared->hasTraceCollector();
}


std::shared_ptr<QueryLog> Context::getQueryLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_log;
}

std::shared_ptr<QueryThreadLog> Context::getQueryThreadLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_thread_log;
}

std::shared_ptr<QueryViewsLog> Context::getQueryViewsLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_views_log;
}

std::shared_ptr<PartLog> Context::getPartLog(const String & part_database) const
{
    SharedLockGuard lock(shared->mutex);

    /// No part log or system logs are shutting down.
    if (!shared->system_logs)
        return {};

    /// Will not log operations on system tables (including part_log itself).
    /// It doesn't make sense and not allow to destruct PartLog correctly due to infinite logging and flushing,
    /// and also make troubles on startup.
    if (part_database == DatabaseCatalog::SYSTEM_DATABASE)
        return {};

    return shared->system_logs->part_log;
}


std::shared_ptr<TraceLog> Context::getTraceLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->trace_log;
}


std::shared_ptr<TextLog> Context::getTextLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->text_log;
}


std::shared_ptr<MetricLog> Context::getMetricLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->metric_log;
}


std::shared_ptr<AsynchronousMetricLog> Context::getAsynchronousMetricLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->asynchronous_metric_log;
}

/// proton: starts.
std::shared_ptr<PipelineMetricLog> Context::getPipelineMetricLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->pipeline_metric_log;
}
/// proton: ends.

std::shared_ptr<OpenTelemetrySpanLog> Context::getOpenTelemetrySpanLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->opentelemetry_span_log;
}

std::shared_ptr<SessionLog> Context::getSessionLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->session_log;
}


std::shared_ptr<ZooKeeperLog> Context::getZooKeeperLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->zookeeper_log;
}


std::shared_ptr<ProcessorsProfileLog> Context::getProcessorsProfileLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->processors_profile_log;
}

std::shared_ptr<TransactionsInfoLog> Context::getTransactionsInfoLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->transactions_info_log;
}

std::shared_ptr<FilesystemCacheLog> Context::getFilesystemCacheLog() const
{
    SharedLockGuard lock(shared->mutex);
    if (!shared->system_logs)
        return {};

    return shared->system_logs->cache_log;
}

CompressionCodecPtr Context::chooseCompressionCodec(size_t part_size, double part_size_ratio) const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->compression_codec_selector)
    {
        constexpr auto config_name = "compression";
        const auto & config = shared->getConfigRefWithLock(lock);

        if (config.has(config_name))
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>(config, "compression");
        else
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>();
    }

    return shared->compression_codec_selector->choose(part_size, part_size_ratio);
}


DiskPtr Context::getDisk(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto disk_selector = getDiskSelector(lock);

    return disk_selector->get(name);
}

StoragePolicyPtr Context::getStoragePolicy(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto policy_selector = getStoragePolicySelector(lock);

    return policy_selector->get(name);
}


DisksMap Context::getDisksMap() const
{
    std::lock_guard lock(shared->storage_policies_mutex);
    return getDisksMap(lock);
}

DisksMap Context::getDisksMap(std::lock_guard<std::mutex> & lock) const
{
    return getDiskSelector(lock)->getDisksMap();
}

StoragePoliciesMap Context::getPoliciesMap() const
{
    std::lock_guard lock(shared->storage_policies_mutex);
    return getStoragePolicySelector(lock)->getPoliciesMap();
}

DiskSelectorPtr Context::getDiskSelector(std::lock_guard<std::mutex> & /* lock */) const TSA_REQUIRES(shared->storage_policies_mutex)
{
    if (!shared->merge_tree_disk_selector)
    {
        constexpr auto config_name = "storage_configuration.disks";
        const auto & config = getConfigRef();

        auto disk_selector = std::make_shared<DiskSelector>();
        disk_selector->initialize(config, config_name, shared_from_this());
        shared->merge_tree_disk_selector = disk_selector;
    }

    return shared->merge_tree_disk_selector;
}

StoragePolicySelectorPtr Context::getStoragePolicySelector(std::lock_guard<std::mutex> & lock) const TSA_REQUIRES(shared->storage_policies_mutex)
{
    if (!shared->merge_tree_storage_policy_selector)
    {
        constexpr auto config_name = "storage_configuration.policies";
        const auto & config = getConfigRef();

        shared->merge_tree_storage_policy_selector = std::make_shared<StoragePolicySelector>(config, config_name, getDiskSelector(lock));
    }

    return shared->merge_tree_storage_policy_selector;
}


void Context::updateStorageConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    {
        std::lock_guard lock(shared->storage_policies_mutex);

        if (shared->merge_tree_disk_selector)
            shared->merge_tree_disk_selector
                = shared->merge_tree_disk_selector->updateFromConfig(config, "storage_configuration.disks", shared_from_this());

        if (shared->merge_tree_storage_policy_selector)
        {
            try
            {
                shared->merge_tree_storage_policy_selector = shared->merge_tree_storage_policy_selector->updateFromConfig(
                    config, "storage_configuration.policies", shared->merge_tree_disk_selector);
            }
            catch (Exception & e)
            {
                LOG_ERROR(
                    shared->log, "An error has occurred while reloading storage policies, storage policies were not applied: {}", e.message());
            }
        }
    }

    {
        std::lock_guard lock(shared->mutex);
        if (shared->storage_s3_settings)
            shared->storage_s3_settings->loadFromConfig("s3", config, getSettingsRef());
    }
}

/// proton: starts. remove `merge tree` and add `stream`
/// Priority: Declared < Configured < Specified
const MergeTreeSettings & Context::getMergeTreeSettings() const
{
    return getStreamSettings();
}

const StreamSettings & Context::getStreamSettings() const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->stream_settings)
    {
        const auto & config = shared->getConfigRefWithLock(lock);
        StreamSettings settings;
        /// Apply configured stream settings.
        settings.applyChanges(loadSettingChangesFromConfig<ConfigurableStreamSettingsTraits>("settings.stream", config));
        shared->stream_settings.emplace(settings);
    }

    return *shared->stream_settings;
}

void Context::applyGlobalSettingsFromConfig()
{
    std::lock_guard lock(shared->mutex);
    const auto & config = shared->getConfigRefWithLock(lock);
    settings.applyChanges(loadSettingChangesFromConfig<ConfigurableSettingsTraits>("settings.global", config));
}
/// proton: ends.

const StorageS3Settings & Context::getStorageS3Settings() const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->storage_s3_settings)
    {
        const auto & config = shared->getConfigRefWithLock(lock);
        shared->storage_s3_settings.emplace().loadFromConfig("s3", config, getSettingsRef());
    }

    return *shared->storage_s3_settings;
}

void Context::checkCanBeDropped(const String & database, const String & table, const size_t & size, const size_t & max_size_to_drop) const
{
    /// proton: starts. add setting `force_drop_big_stream`
    if (!max_size_to_drop || size <= max_size_to_drop || getSettingsRef().force_drop_big_stream)
        return;
    /// proton: ends.

    fs::path force_file(getFlagsPath() + "force_drop_table");
    bool force_file_exists = fs::exists(force_file);

    if (force_file_exists)
    {
        try
        {
            fs::remove(force_file);
            return;
        }
        catch (...)
        {
            /// User should recreate force file on each drop, it shouldn't be protected
            tryLogCurrentException("Drop stream check", "Can't remove force file to enable stream or partition drop");
        }
    }

    String size_str = formatReadableSizeWithDecimalSuffix(size);
    String max_size_to_drop_str = formatReadableSizeWithDecimalSuffix(max_size_to_drop);
    throw Exception(ErrorCodes::STREAM_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT,
                    "Stream or Partition in {}.{} was not dropped.\nReason:\n"
                    "1. Size ({}) is greater than max_[stream/partition]_size_to_drop ({})\n"
                    "2. File '{}' intended to force DROP {}\n"
                    "3. Setting 'force_drop_big_stream' is not set\n"
                    "How to fix this:\n"
                    "1. Either increase (or set to zero) max_[stream/partition]_size_to_drop in server config\n"
                    "2. Either create forcing file {} and make sure that timeplus DBMS has write permission for it.\n"
                    "3. Either add setting 'force_drop_big_stream=true'\n"
                    "Example:\nsudo touch '{}' && sudo chmod 666 '{}'",
                    backQuoteIfNeed(database), backQuoteIfNeed(table),
                    size_str, max_size_to_drop_str,
                    force_file.string(), force_file_exists ? "exists but not writeable (could not be removed)" : "doesn't exist",
                    force_file.string(),
                    force_file.string(), force_file.string());
}


void Context::setMaxTableSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_stream_size_to_drop.store(max_size, std::memory_order_relaxed);
}


void Context::checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const
{
    size_t max_stream_size_to_drop = shared->max_stream_size_to_drop.load(std::memory_order_relaxed);

    checkCanBeDropped(database, table, table_size, max_stream_size_to_drop);
}


void Context::setMaxPartitionSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_partition_size_to_drop.store(max_size, std::memory_order_relaxed);
}


void Context::checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const
{
    size_t max_partition_size_to_drop = shared->max_partition_size_to_drop.load(std::memory_order_relaxed);

    checkCanBeDropped(database, table, partition_size, max_partition_size_to_drop);
}


InputFormatPtr Context::getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size, const std::optional<FormatSettings> & format_settings) const
{
    return FormatFactory::instance().getInput(name, buf, sample, shared_from_this(), max_block_size, format_settings);
}

OutputFormatPtr Context::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputFormat(name, buf, sample, shared_from_this());
}

OutputFormatPtr Context::getOutputFormatParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputFormatParallelIfPossible(name, buf, sample, shared_from_this());
}


double Context::getUptimeSeconds() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->uptime_watch.elapsedSeconds();
}


void Context::setConfigReloadCallback(ConfigReloadCallback && callback)
{
    /// Is initialized at server startup, so lock isn't required. Otherwise use mutex.
    shared->config_reload_callback = std::move(callback);
}

void Context::reloadConfig() const
{
    /// Use mutex if callback may be changed after startup.
    if (!shared->config_reload_callback)
        throw Exception("Can't reload config because config_reload_callback is not set.", ErrorCodes::LOGICAL_ERROR);

    shared->config_reload_callback();
}


void Context::shutdown() TSA_NO_THREAD_SAFETY_ANALYSIS
{
    // Disk selector might not be initialized if there was some error during
    // its initialization. Don't try to initialize it again on shutdown.
    if (shared->merge_tree_disk_selector)
    {
        for (auto & [disk_name, disk] : getDisksMap())
        {
            LOG_INFO(shared->log, "Shutdown disk {}", disk_name);
            disk->shutdown();
        }
    }

    // Special volumes might also use disks that require shutdown.
    for (const auto & volume : {shared->tmp_volume, shared->backups_volume})
    {
        if (volume)
        {
            auto & disks = volume->getDisks();
            for (auto & disk : disks)
            {
                disk->shutdown();
            }
        }
    }

    shared->shutdown();
}


Context::ApplicationType Context::getApplicationType() const
{
    return shared->application_type;
}

void Context::setApplicationType(ApplicationType type)
{
    /// Lock isn't required, you should set it at start
    shared->application_type = type;
}

void Context::setDefaultProfiles(const Poco::Util::AbstractConfiguration & config)
{
    shared->default_profile_name = config.getString("default_profile", "default");
    getAccessControl().setDefaultProfileName(shared->default_profile_name);

    shared->system_profile_name = config.getString("system_profile", shared->default_profile_name);
    setCurrentProfile(shared->system_profile_name);

    applySettingsQuirks(settings, &Poco::Logger::get("SettingsQuirks"));

    shared->buffer_profile_name = config.getString("buffer_profile", shared->system_profile_name);
    buffer_context = Context::createCopy(shared_from_this());
    buffer_context->setCurrentProfile(shared->buffer_profile_name);
}

String Context::getDefaultProfileName() const
{
    return shared->default_profile_name;
}

String Context::getSystemProfileName() const
{
    return shared->system_profile_name;
}

String Context::getFormatSchemaPath() const
{
    return shared->format_schema_path;
}

void Context::setFormatSchemaPath(const String & path)
{
    shared->format_schema_path = path;
}

Context::SampleBlockCache & Context::getSampleBlockCache() const
{
    assert(hasQueryContext());
    return getQueryContext()->sample_block_cache;
}


bool Context::hasQueryParameters() const
{
    return !query_parameters.empty();
}


const NameToNameMap & Context::getQueryParameters() const
{
    return query_parameters;
}


void Context::setQueryParameter(const String & name, const String & value)
{
    if (!query_parameters.emplace(name, value).second)
        throw Exception("Duplicate name " + backQuote(name) + " of query parameter", ErrorCodes::BAD_ARGUMENTS);
}

void Context::addQueryParameters(const NameToNameMap & parameters)
{
    for (const auto & [name, value] : parameters)
        query_parameters.insert_or_assign(name, value);
}

IHostContextPtr & Context::getHostContext()
{
    return host_context;
}


const IHostContextPtr & Context::getHostContext() const
{
    return host_context;
}


std::shared_ptr<ActionLocksManager> Context::getActionLocksManager()
{
    callOnce(shared->action_locks_manager_initialized, [&] {
        shared->action_locks_manager = std::make_shared<ActionLocksManager>(shared_from_this());
    });

    return shared->action_locks_manager;
}


void Context::setExternalTablesInitializer(ExternalTablesInitializer && initializer)
{
    if (external_tables_initializer_callback)
        throw Exception("External tables initializer is already set", ErrorCodes::LOGICAL_ERROR);

    external_tables_initializer_callback = std::move(initializer);
}

void Context::initializeExternalTablesIfSet()
{
    if (external_tables_initializer_callback)
    {
        external_tables_initializer_callback(shared_from_this());
        /// Reset callback
        external_tables_initializer_callback = {};
    }
}


void Context::setInputInitializer(InputInitializer && initializer)
{
    if (input_initializer_callback)
        throw Exception("Input initializer is already set", ErrorCodes::LOGICAL_ERROR);

    input_initializer_callback = std::move(initializer);
}


void Context::initializeInput(const StoragePtr & input_storage)
{
    if (!input_initializer_callback)
        throw Exception("Input initializer is not set", ErrorCodes::LOGICAL_ERROR);

    input_initializer_callback(shared_from_this(), input_storage);
    /// Reset callback
    input_initializer_callback = {};
}


void Context::setInputBlocksReaderCallback(InputBlocksReader && reader)
{
    if (input_blocks_reader)
        throw Exception("Input blocks reader is already set", ErrorCodes::LOGICAL_ERROR);

    input_blocks_reader = std::move(reader);
}


InputBlocksReader Context::getInputBlocksReaderCallback() const
{
    return input_blocks_reader;
}


void Context::resetInputCallbacks()
{
    if (input_initializer_callback)
        input_initializer_callback = {};

    if (input_blocks_reader)
        input_blocks_reader = {};
}


StorageID Context::resolveStorageID(StorageID storage_id, StorageNamespace where) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    StorageID resolved = StorageID::createEmpty();
    std::optional<Exception> exc;
    {
        SharedLockGuard lock(mutex);
        resolved = resolveStorageIDImpl(std::move(storage_id), where, &exc);
    }
    if (exc)
        throw Exception(*exc);
    if (!resolved.hasUUID() && resolved.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
        resolved.uuid = DatabaseCatalog::instance().getDatabase(resolved.database_name)->tryGetTableUUID(resolved.table_name);
    return resolved;
}

StorageID Context::tryResolveStorageID(StorageID storage_id, StorageNamespace where) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    StorageID resolved = StorageID::createEmpty();
    {
        SharedLockGuard lock(mutex);
        resolved = resolveStorageIDImpl(std::move(storage_id), where, nullptr);
    }
    if (resolved && !resolved.hasUUID() && resolved.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
    {
        auto db = DatabaseCatalog::instance().tryGetDatabase(resolved.database_name);
        if (db)
            resolved.uuid = db->tryGetTableUUID(resolved.table_name);
    }
    return resolved;
}

StorageID Context::resolveStorageIDImpl(StorageID storage_id, StorageNamespace where, std::optional<Exception> * exception) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    if (!storage_id)
    {
        if (exception)
            exception->emplace("Both stream name and UUID are empty", ErrorCodes::UNKNOWN_STREAM);
        return storage_id;
    }

    bool look_for_external_table = where & StorageNamespace::ResolveExternal;
    /// Global context should not contain temporary tables
    if (isGlobalContext())
        look_for_external_table = false;

    bool in_current_database = where & StorageNamespace::ResolveCurrentDatabase;
    bool in_specified_database = where & StorageNamespace::ResolveGlobal;

    if (!storage_id.database_name.empty())
    {
        if (in_specified_database)
            return storage_id;     /// NOTE There is no guarantees that table actually exists in database.
        if (exception)
            exception->emplace("External and temporary tables have no database, but " +
                        storage_id.database_name + " is specified", ErrorCodes::UNKNOWN_STREAM);
        return StorageID::createEmpty();
    }

    /// Database name is not specified. It's temporary table or table in current database.

    if (look_for_external_table)
    {
        auto resolved_id = StorageID::createEmpty();
        auto try_resolve = [&](ContextPtr context) -> bool
        {
            const auto & tables = context->external_tables_mapping;
            auto it = tables.find(storage_id.getTableName());
            if (it == tables.end())
                return false;
            resolved_id = it->second->getGlobalTableID();
            return true;
        };

        /// Firstly look for temporary table in current context
        if (try_resolve(shared_from_this()))
            return resolved_id;

        /// If not found and current context was created from some query context, look for temporary table in query context
        auto query_context_ptr = query_context.lock();
        bool is_local_context = query_context_ptr && query_context_ptr.get() != this;
        if (is_local_context && try_resolve(query_context_ptr))
            return resolved_id;

        /// If not found and current context was created from some session context, look for temporary table in session context
        auto session_context_ptr = session_context.lock();
        bool is_local_or_query_context = session_context_ptr && session_context_ptr.get() != this;
        if (is_local_or_query_context && try_resolve(session_context_ptr))
            return resolved_id;
    }

    /// Temporary table not found. It's table in current database.

    if (in_current_database)
    {
        if (current_database.empty())
        {
            if (exception)
                exception->emplace("Default database is not selected", ErrorCodes::UNKNOWN_DATABASE);
            return StorageID::createEmpty();
        }
        storage_id.database_name = current_database;
        /// NOTE There is no guarantees that table actually exists in database.
        return storage_id;
    }

    if (exception)
        exception->emplace("Cannot resolve database name for stream " + storage_id.getNameForLogs(), ErrorCodes::UNKNOWN_STREAM);
    return StorageID::createEmpty();
}

void Context::checkTransactionsAreAllowed(bool explicit_tcl_query /* = false */) const
{
    if (getConfigRef().getInt("allow_experimental_transactions", 0))
        return;

    if (explicit_tcl_query)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transactions are not supported");

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Experimental support for transactions is disabled, "
                    "however, some query or background task tried to access TransactionLog. "
                    "If you have not enabled this feature explicitly, then it's a bug.");
}

void Context::initCurrentTransaction(MergeTreeTransactionPtr txn)
{
    merge_tree_transaction_holder = MergeTreeTransactionHolder(txn, false, this);
    setCurrentTransaction(std::move(txn));
}

void Context::setCurrentTransaction(MergeTreeTransactionPtr txn)
{
    assert(!merge_tree_transaction || !txn);
    assert(this == session_context.lock().get() || this == query_context.lock().get());
    merge_tree_transaction = std::move(txn);
    if (!merge_tree_transaction)
        merge_tree_transaction_holder = {};
}

MergeTreeTransactionPtr Context::getCurrentTransaction() const
{
    return merge_tree_transaction;
}

bool Context::isServerCompletelyStarted() const
{
    SharedLockGuard lock(shared->mutex);
    assert(getApplicationType() == ApplicationType::SERVER);
    return shared->is_server_completely_started;
}

void Context::setServerCompletelyStarted()
{
    std::lock_guard lock(shared->mutex);
    assert(global_context.lock().get() == this);
    assert(!shared->is_server_completely_started);
    assert(getApplicationType() == ApplicationType::SERVER);
    shared->is_server_completely_started = true;
}

PartUUIDsPtr Context::getPartUUIDs() const
{
    std::lock_guard lock(mutex);

    if (!part_uuids)
        /// For context itself, only this initialization is not const.
        /// We could have done in constructor.
        /// TODO: probably, remove this from Context.
        const_cast<PartUUIDsPtr &>(part_uuids) = std::make_shared<PartUUIDs>();

    return part_uuids;
}


ReadTaskCallback Context::getReadTaskCallback() const
{
    if (!next_task_callback.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Next task callback is not set for query {}", getInitialQueryId());
    return next_task_callback.value();
}


void Context::setReadTaskCallback(ReadTaskCallback && callback)
{
    next_task_callback = callback;
}


MergeTreeReadTaskCallback Context::getMergeTreeReadTaskCallback() const
{
    if (!merge_tree_read_task_callback.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Next task callback for is not set for query {}", getInitialQueryId());

    return merge_tree_read_task_callback.value();
}

void Context::setMergeTreeReadTaskCallback(MergeTreeReadTaskCallback && callback)
{
    merge_tree_read_task_callback = callback;
}

PartUUIDsPtr Context::getIgnoredPartUUIDs() const
{
    std::lock_guard lock(mutex);
    if (!ignored_part_uuids)
        const_cast<PartUUIDsPtr &>(ignored_part_uuids) = std::make_shared<PartUUIDs>();

    return ignored_part_uuids;
}

AsynchronousInsertQueue * Context::getAsynchronousInsertQueue() const
{
    return shared->async_insert_queue.get();
}

void Context::setAsynchronousInsertQueue(const std::shared_ptr<AsynchronousInsertQueue> & ptr)
{
    using namespace std::chrono;

    if (std::chrono::milliseconds(settings.async_insert_busy_timeout_ms) == 0ms)
        throw Exception("Setting async_insert_busy_timeout_ms can't be zero", ErrorCodes::INVALID_SETTING_VALUE);

    shared->async_insert_queue = ptr;
}

void Context::initializeBackgroundExecutorsIfNeeded()
{
    std::lock_guard lock(shared->background_executors_mutex);

    if (shared->is_background_executors_initialized)
        return;

    const size_t max_merges_and_mutations = static_cast<size_t>(getSettingsRef().background_pool_size * getSettingsRef().background_merges_mutations_concurrency_ratio.value);

    /// With this executor we can execute more tasks than threads we have
    shared->merge_mutate_executor = MergeMutateBackgroundExecutor::create
    (
        "MergeMutate",
        /*max_threads_count*/getSettingsRef().background_pool_size,
        /*max_tasks_count*/max_merges_and_mutations,
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask
    );

    LOG_INFO(shared->log, "Initialized background executor for merges and mutations with num_threads={}, num_tasks={}",
        getSettingsRef().background_pool_size, max_merges_and_mutations);

    shared->moves_executor = OrdinaryBackgroundExecutor::create
    (
        "Move",
        getSettingsRef().background_move_pool_size,
        getSettingsRef().background_move_pool_size,
        CurrentMetrics::BackgroundMovePoolTask
    );

    LOG_INFO(shared->log, "Initialized background executor for move operations with num_threads={}, num_tasks={}",
        getSettingsRef().background_move_pool_size, getSettingsRef().background_move_pool_size);

    shared->fetch_executor = OrdinaryBackgroundExecutor::create
    (
        "Fetch",
        getSettingsRef().background_fetches_pool_size,
        getSettingsRef().background_fetches_pool_size,
        CurrentMetrics::BackgroundFetchesPoolTask
    );

    LOG_INFO(shared->log, "Initialized background executor for fetches with num_threads={}, num_tasks={}",
        getSettingsRef().background_fetches_pool_size, getSettingsRef().background_fetches_pool_size);

    shared->common_executor = OrdinaryBackgroundExecutor::create
    (
        "Common",
        getSettingsRef().background_common_pool_size,
        getSettingsRef().background_common_pool_size,
        CurrentMetrics::BackgroundCommonPoolTask
    );

    LOG_INFO(shared->log, "Initialized background executor for common operations (e.g. clearing old parts) with num_threads={}, num_tasks={}",
        getSettingsRef().background_common_pool_size, getSettingsRef().background_common_pool_size);

    shared->is_background_executors_initialized = true;
}


MergeMutateBackgroundExecutorPtr Context::getMergeMutateExecutor() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->merge_mutate_executor;
}

OrdinaryBackgroundExecutorPtr Context::getMovesExecutor() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->moves_executor;
}

OrdinaryBackgroundExecutorPtr Context::getFetchesExecutor() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->fetch_executor;
}

OrdinaryBackgroundExecutorPtr Context::getCommonExecutor() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->common_executor;
}

static size_t getThreadPoolReaderSizeFromConfig(Context::FilesystemReaderType type, const Poco::Util::AbstractConfiguration & config)
{
    switch (type)
    {
        case Context::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER:
        {
            return config.getUInt(".threadpool_remote_fs_reader_pool_size", 250);
        }
        case Context::FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER:
        {
            return config.getUInt(".threadpool_local_fs_reader_pool_size", 100);
        }
        case Context::FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER:
        {
            return std::numeric_limits<std::size_t>::max();
        }
    }
}

size_t Context::getThreadPoolReaderSize(FilesystemReaderType type) const
{
    const auto & config = getConfigRef();
    return getThreadPoolReaderSizeFromConfig(type, config);
}

IAsynchronousReader & Context::getThreadPoolReader(FilesystemReaderType type) const
{
    callOnce(shared->readers_initialized, [&] {
        const auto & config = getConfigRef();
        auto pool_size = getThreadPoolReaderSizeFromConfig(type, config);
        auto queue_size = config.getUInt(".threadpool_remote_fs_reader_queue_size", 1000000);
        shared->asynchronous_remote_fs_reader = std::make_unique<ThreadPoolRemoteFSReader>(pool_size, queue_size);

        queue_size = config.getUInt(".threadpool_local_fs_reader_queue_size", 1000000);
        shared->asynchronous_local_fs_reader = std::make_unique<ThreadPoolReader>(pool_size, queue_size);

        shared->synchronous_local_fs_reader = std::make_unique<SynchronousReader>();
    });

    switch (type)
    {
        case FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER:
            return *shared->asynchronous_remote_fs_reader;
        case FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER:
            return *shared->asynchronous_local_fs_reader;
        case FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER:
            return *shared->synchronous_local_fs_reader;
    }
}

ThreadPool & Context::getThreadPoolWriter() const
{
    callOnce(shared->threadpool_writer_initialized, [&] {
        const auto & config = getConfigRef();
        auto pool_size = config.getUInt(".threadpool_writer_pool_size", 100);
        auto queue_size = config.getUInt(".threadpool_writer_queue_size", 1000000);

        shared->threadpool_writer = std::make_unique<ThreadPool>(pool_size, pool_size, queue_size);
    });

    return *shared->threadpool_writer;
}

ReadSettings Context::getReadSettings() const
{
    ReadSettings res;

    std::string_view read_method_str = settings.local_filesystem_read_method.value;

    if (auto opt_method = magic_enum::enum_cast<LocalFSReadMethod>(read_method_str))
        res.local_fs_method = *opt_method;
    else
        throw Exception(ErrorCodes::UNKNOWN_READ_METHOD, "Unknown read method '{}' for local filesystem", read_method_str);

    read_method_str = settings.remote_filesystem_read_method.value;

    if (auto opt_method = magic_enum::enum_cast<RemoteFSReadMethod>(read_method_str))
        res.remote_fs_method = *opt_method;
    else
        throw Exception(ErrorCodes::UNKNOWN_READ_METHOD, "Unknown read method '{}' for remote filesystem", read_method_str);

    res.local_fs_prefetch = settings.local_filesystem_read_prefetch;
    res.remote_fs_prefetch = settings.remote_filesystem_read_prefetch;

    res.load_marks_asynchronously = settings.load_marks_asynchronously;

    res.remote_fs_read_max_backoff_ms = settings.remote_fs_read_max_backoff_ms;
    res.remote_fs_read_backoff_max_tries = settings.remote_fs_read_backoff_max_tries;
    res.enable_filesystem_cache = settings.enable_filesystem_cache;
    res.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache;
    res.enable_filesystem_cache_log = settings.enable_filesystem_cache_log;
    res.enable_filesystem_cache_on_lower_level = settings.enable_filesystem_cache_on_lower_level;

    res.max_query_cache_size = settings.max_query_cache_size;
    res.skip_download_if_exceeds_query_cache = settings.skip_download_if_exceeds_query_cache;

    res.remote_read_min_bytes_for_seek = settings.remote_read_min_bytes_for_seek;

    /// Zero read buffer will not make progress.
    if (!settings.max_read_buffer_size)
    {
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid value '{}' for max_read_buffer_size", settings.max_read_buffer_size);
    }

    res.local_fs_buffer_size = settings.max_read_buffer_size;
    res.remote_fs_buffer_size = settings.max_read_buffer_size;
    res.direct_io_threshold = settings.min_bytes_to_use_direct_io;
    res.mmap_threshold = settings.min_bytes_to_use_mmap_io;
    res.priority = settings.read_priority;

    res.remote_throttler = getRemoteReadThrottler();

    res.http_max_tries = settings.http_max_tries;
    res.http_retry_initial_backoff_ms = settings.http_retry_initial_backoff_ms;
    res.http_retry_max_backoff_ms = settings.http_retry_max_backoff_ms;
    res.http_skip_not_found_url_for_globs = settings.http_skip_not_found_url_for_globs;

    res.mmap_cache = getMMappedFileCache().get();

    return res;
}

WriteSettings Context::getWriteSettings() const
{
    WriteSettings res;

    res.enable_filesystem_cache_on_write_operations = settings.enable_filesystem_cache_on_write_operations;
    res.enable_filesystem_cache_log = settings.enable_filesystem_cache_log;
    res.throw_on_error_from_cache = settings.throw_on_error_from_cache_on_write_operations;

    res.remote_throttler = getRemoteWriteThrottler();

    return res;
}

/// proton: starts.
bool Context::isDistributedEnv() const
{
    /// FIXME， change this logic in future
    /// if no kafka logstore is enabled，for now， enforce single instance env
    return klog::KafkaWALPool::instance(shared_from_this()).enabled();
}

ThreadPool & Context::getPartCommitPool() const
{
    callOnce(shared->part_commit_pool_initialized, [&] {
        /// FIXME, queue size may matter
        shared->part_commit_pool = std::make_unique<ThreadPool>(settings.part_commit_pool_size);
    });
    return *shared->part_commit_pool;
}

void Context::setupNodeIdentity()
{
    if (!node_identity.empty() && !channel_id.empty())
        return;

    this_host = getFQDNOrHostName();
    auto id = getConfigRef().getString("cluster_settings.node_identity", "");
    if (!id.empty())
        node_identity = id;
    else
        node_identity = this_host;

    channel_id = std::to_string(CityHash_v1_0_2::CityHash64WithSeed(node_identity.data(), node_identity.size(), 123));
}

void Context::setupQueryStatusPollId(UInt64 block_base_id_)
{
    if (!query_status_poll_id.empty())
        return;

    /// Poll ID is composed by : (query_id, database.table (fullName), user, host, block_base_id, timestamp)
    const String sep = "!`$";
    std::vector<String> components;
    components.reserve(6);

    components.push_back(getCurrentQueryId());
    components.push_back(getInsertionTable().getFullNameNotQuoted());
    components.push_back(getUserName());
    components.push_back(getNodeIdentity());
    components.push_back(std::to_string(block_base_id_));
    components.push_back(std::to_string(MonotonicMicroseconds::now()));

    /// FIXME, encrypt it
    std::ostringstream ostr; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::Base64Encoder encoder(ostr);
    encoder.rdbuf()->setLineLength(0);
    encoder << boost::algorithm::join(components, sep);
    encoder.close();

    query_status_poll_id = ostr.str();
    block_base_id = block_base_id_;
}

/// (query_id, database, table, user_name, node_identity, block_base_id, timestamp)
std::vector<String> Context::parseQueryStatusPollId(const String & poll_id) const
{
    if (poll_id.size() > 512)
        throw Exception("Invalid poll ID", ErrorCodes::BAD_ARGUMENTS);

    std::istringstream istr(poll_id); /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::Base64Decoder decoder(istr);
    char buf[512] = {'\0'};
    decoder.read(buf, sizeof(buf));

    String decoded{buf, static_cast<size_t>(decoder.gcount())};

    static boost::regex rx("!`\\$");
    std::vector<String> components;
    components.reserve(6);

    boost::algorithm::split_regex(components, decoded, rx);

    /// FIXME, more check for future extension
    if (components.size() != 6)
        throw Exception("Invalid poll ID", ErrorCodes::BAD_ARGUMENTS);

    if (getUserName() != components[2])
        throw Exception("User doesn't own this poll ID", ErrorCodes::ACCESS_DENIED);

    std::vector<String> names;
    names.reserve(2);
    boost::algorithm::split(names, components[1], boost::is_any_of("."));
    if (names.size() != 2)
        throw Exception("Invalid poll ID: " + poll_id, ErrorCodes::INVALID_POLL_ID);

    std::vector<String> result = {
        std::move(components[0]), std::move(names[0]), std::move(names[1]),
        std::move(components[2]), std::move(components[3]), std::move(components[4]),
        std::move(components[5])
    };

    /// FIXME, check timestamp etc
    return result;
}

Context::DataStreamSemanticCache & Context::getDataStreamSemanticCache() const
{
    assert(hasQueryContext());
    return getQueryContext()->data_stream_semantic_cache;
}
/// proton: ends.

}
