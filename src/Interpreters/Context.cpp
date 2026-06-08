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
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerSettings.h>
#include <Formats/FormatFactory.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/CompressionCodecSelector.h>
#include <Storages/StorageS3Settings.h>
#include <Disks/DiskLocal.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ThreadPoolReader.h>
#include <Disks/StoragePolicy.h>
#include <Disks/IO/IOUringReader.h>
#include <Disks/IO/getIOUringReader.h>
#include <IO/SynchronousReader.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/PreparedSets.h>
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
#include <IO/ResourceManagerFactory.h>
#include <Backups/BackupFactory.h>
#include <Dictionaries/Embedded/GeoDictionariesLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
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
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
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
#include <Parsers/ASTFunction.h>
#include <Storages/StorageView.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Parsers/FunctionParameterValuesVisitor.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

#if USE_ROCKSDB
#include <rocksdb/table.h>
#endif

/// proton: starts
#include <Access/Authentication.h>
#include <Bootstrap/Bootstrap.h>
#include <Bootstrap/Globals.h>
#include <CPython/AsyncPythonPackageManager.h>
#include <Cluster/Common/TimeWheel/TimerService.h>
#include <Cluster/KafkaLog/KafkaWALPool.h>
#include <Core/SettingsUtil.h>
#include <Task/TaskScheduler.h>
#include <base/getFQDNOrHostName.h>
#include <Common/BackgroundSchedulePool.h>
#include <Common/ProtonCommon.h>

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
    extern const Metric BackgroundSchedulePoolNativeLogTask;
    extern const Metric BackgroundCommonPoolSize;
    extern const Metric MarksLoaderThreads;
    extern const Metric MarksLoaderThreadsActive;
    extern const Metric IOPrefetchThreads;
    extern const Metric IOPrefetchThreadsActive;
    extern const Metric IOWriterThreads;
    extern const Metric IOWriterThreadsActive;
    extern const Metric StorageCommitThreads;
    extern const Metric StorageCommitThreadsActive;
    extern const Metric NLogAdhocThreads;
    extern const Metric NLogAdhocThreadsActive;
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
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
    extern const int UNKNOWN_FUNCTION;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int UNKNOWN_DISK;
    extern const int UNKNOWN_READ_METHOD;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_FUNCTION;
    /// proton: starts
    extern const int UNKNOWN_USER;
    extern const int DISK_USAGE_RATIO_THRESHOLD_EXCEEDED;
    /// proton: ends
}

/** Set of known objects (environment), that could be used in query.
  * Shared (global) part. Order of members (especially, order of destruction) is very important.
  */
struct ContextSharedPart : boost::noncopyable
{
    LoggerPtr log = getLogger("Context");

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

    std::shared_ptr<IDisk> db_disk TSA_GUARDED_BY(mutex);

    /// FIXME. in latest commuity version, the variable has been removed
    TemporaryDataOnDiskScopePtr temp_data_on_disk;          /// Temporary files that occur when processing the request accounted here.

    /// FIXME(yokofly): This value is currently unused and will be removed in the commit referenced below on May 3, 2022.
    /// See: https://github.com/ClickHouse/ClickHouse/commit/5257ce31f86cd3852a8a30343596ad029c56b083#diff-c7c4cea868f661341c1e9866836dc34c1c88723f9f33b4e09db530c2ea074036L2843-R2854
    /// Therefore, I did not add TSA_GUARDED_BY.
    /// mutable VolumePtr backups_volume;                       /// Volume for all the backups.

    mutable std::unique_ptr<EmbeddedDictionaries> embedded_dictionaries TSA_GUARDED_BY(embedded_dictionaries_mutex);    /// Metrica's dictionaries. Have lazy initialization.
    mutable std::unique_ptr<ExternalDictionariesLoader> external_dictionaries_loader TSA_GUARDED_BY(external_dictionaries_mutex);
    mutable std::unique_ptr<ExternalModelsLoader> external_models_loader TSA_GUARDED_BY(external_models_mutex);

    ExternalLoaderXMLConfigRepository * external_models_config_repository TSA_GUARDED_BY(external_models_mutex) = nullptr;
    scope_guard models_repository_guard TSA_GUARDED_BY(external_models_mutex);

    ExternalLoaderXMLConfigRepository * external_dictionaries_config_repository TSA_GUARDED_BY(external_dictionaries_mutex) = nullptr;
    scope_guard dictionaries_xmls TSA_GUARDED_BY(external_dictionaries_mutex);

    scope_guard user_defined_executable_functions_xmls TSA_GUARDED_BY(external_user_defined_executable_functions_mutex);

#if USE_NLP
    mutable OnceFlag synonyms_extensions_initialized;
    mutable std::optional<SynonymsExtensions> synonyms_extensions;

    mutable OnceFlag lemmatizers_initialized;
    mutable std::optional<Lemmatizers> lemmatizers;
#endif

    /// No lock required for default_profile_name, system_profile_name, buffer_profile_name modified only during initialization
    String default_profile_name;                                /// Default profile name used for default values.
    String system_profile_name;                                 /// Profile used by system processes
    String buffer_profile_name;                                 /// Profile used by Buffer engine for flushing to the underlying
    String merge_workload TSA_GUARDED_BY(mutex);                /// Workload setting value that is used by all merges
    String mutation_workload TSA_GUARDED_BY(mutex);             /// Workload setting value that is used by all mutations
    std::shared_ptr<AccessControl> access_control TSA_GUARDED_BY(mutex);
    mutable OnceFlag resource_manager_initialized;
    mutable UncompressedCachePtr uncompressed_cache TSA_GUARDED_BY(mutex);        /// The cache of decompressed blocks.
    mutable MarkCachePtr mark_cache TSA_GUARDED_BY(mutex);                        /// Cache of marks in compressed files.
    mutable OnceFlag load_marks_threadpool_initialized;
    mutable std::unique_ptr<ThreadPool> load_marks_threadpool; /// Threadpool for loading marks cache.
    mutable std::unique_ptr<ThreadPool> prefetch_threadpool; /// Threadpool for loading marks cache.
    mutable ResourceManagerPtr resource_manager;
    mutable UncompressedCachePtr index_uncompressed_cache TSA_GUARDED_BY(mutex);  /// The cache of decompressed blocks for MergeTree indices.
    mutable MarkCachePtr index_mark_cache TSA_GUARDED_BY(mutex);                  /// Cache of marks in compressed files of MergeTree indices.
    mutable MMappedFileCachePtr mmap_cache TSA_GUARDED_BY(mutex); /// Cache of mmapped files to avoid frequent open/map/unmap/close and to reuse from several threads.
    ProcessList process_list;                               /// Executing queries at the moment.
    GlobalOvercommitTracker global_overcommit_tracker;
    MergeList merge_list;                                   /// The list of executable merge (for (Replicated)?MergeTree)
    ConfigurationPtr users_config TSA_GUARDED_BY(mutex);                          /// Config with the users, profiles and quotas sections.
    InterserverIOHandler interserver_io_handler;            /// Handler for interserver communication.

    OnceFlag buffer_flush_schedule_pool_initialized;
    mutable std::unique_ptr<BackgroundSchedulePool> buffer_flush_schedule_pool; /// A thread pool that can do background flush for Buffer tables.
    OnceFlag schedule_pool_initialized;
    mutable std::unique_ptr<BackgroundSchedulePool> schedule_pool;    /// A thread pool that can run different jobs in background (used in replicated tables)
    OnceFlag distributed_schedule_pool_initialized;
    mutable std::unique_ptr<BackgroundSchedulePool> distributed_schedule_pool; /// A thread pool that can run different jobs in background (used for distributed sends)
    OnceFlag message_broker_schedule_pool_initialized;
    mutable std::unique_ptr<BackgroundSchedulePool> message_broker_schedule_pool; /// A thread pool that can run different jobs in background (used for message brokers, like RabbitMQ and Kafka)

    /// proton : starts
    OnceFlag storage_commit_pool_initialized;
    mutable std::unique_ptr<ThreadPool> storage_commit_pool; /// A thread pool that can build part and commit in background (used for Stream table engine)
    OnceFlag nlog_adhoc_schedule_pool_initialized;
    mutable std::unique_ptr<ThreadPool> nlog_adhoc_schedule_pool; /// A thread pool which is used to schedule nativelog ad-hoc task
    OnceFlag nlog_schedule_pool_initialized;
    mutable std::unique_ptr<NLOG::BackgroundSchedulePool> nlog_schedule_pool; /// A thread pool which is used to schedule nativelog background task
    OnceFlag global_system_timer_initialized;
    mutable std::unique_ptr<cluster::TimerService> global_system_timer; /// A system timer which is used to schedule timer task
    OnceFlag global_adhoc_schedule_pool_initialized;
    mutable std::unique_ptr<ThreadPool> global_adhoc_schedule_pool; /// A thread pool which is used to schedule ad-hoc task
    /// proton : ends

    mutable OnceFlag readers_initialized;
    mutable std::unique_ptr<IAsynchronousReader> asynchronous_remote_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> asynchronous_local_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> synchronous_local_fs_reader;

    mutable OnceFlag threadpool_writer_initialized;
    mutable std::unique_ptr<ThreadPool> threadpool_writer;

#if USE_LIBURING
    mutable OnceFlag io_uring_reader_initialized;
    mutable std::unique_ptr<IOUringReader> io_uring_reader;
#endif

    mutable ThrottlerPtr remote_read_throttler;             /// A server-wide throttler for remote IO reads
    mutable ThrottlerPtr remote_write_throttler;            /// A server-wide throttler for remote IO writes

    mutable ThrottlerPtr local_read_throttler;              /// A server-wide throttler for local IO reads
    mutable ThrottlerPtr local_write_throttler;             /// A server-wide throttler for local IO writes

    mutable ThrottlerPtr backups_server_throttler;          /// A server-wide throttler for BACKUPs

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

    ServerSettings server_settings;

    std::atomic_size_t max_stream_size_to_drop = 50000000000lu; /// Protects MergeTree tables from accidental DROP (50GB by default)
    std::atomic_size_t max_partition_size_to_drop = 50000000000lu; /// Protects MergeTree partitions from accidental DROP (50GB by default)
    /// No lock required for format_schema_path modified only during initialization
    String format_schema_path;                              /// Path to a directory that contains schema files used by input formats.
    String google_protos_path; /// Path to a directory that contains the proto files for the well-known Protobuf types.
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
    OrdinaryBackgroundExecutorPtr common_executor TSA_GUARDED_BY(background_executors_mutex);

    RemoteHostFilter remote_host_filter; /// Allowed URL from config.xml

    /// No lock required for trace_collector modified only during initialization
    std::optional<TraceCollector> trace_collector;        /// Thread collecting traces from threads executing queries

    /// No lock required for async_insert_queue modified only during initialization
    std::shared_ptr<AsynchronousInsertQueue> async_insert_queue;

#if USE_PYTHON_UDF
    /// No lock required for async_python_package_manager modified only during initialization
    std::shared_ptr<cpython::AsyncPythonPackageManager> async_python_package_manager;
#endif

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

    /// proton : starts
    String config_path;
    String spill_dir_path;

    /// When boot, we will set these data members up and moving forward, they are read only
    const String this_host;
    cluster::NodeID this_node_id = 1; /// Always 1
    double max_disk_util = 0.9;


    mutable ContextSharedMutex task_scheduler_mutex;
    Task::TaskSchedulerPtr task_scheduler TSA_GUARDED_BY(task_scheduler_mutex);
    /// proton : ends

    ContextSharedPart()
        : access_control(std::make_shared<AccessControl>())
        , global_overcommit_tracker(&process_list)
        , macros(std::make_unique<Macros>())
        , this_host(getFQDNOrHostName())
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

        if (prefetch_threadpool)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing prefetch threadpool");
                prefetch_threadpool->wait();
                prefetch_threadpool.reset();
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

        if (external_models_loader)
            external_models_loader->enablePeriodicUpdates(false);

        Session::shutdownNamedSessions();

        /**  After system_logs have been shut down it is guaranteed that no system table gets created or written to.
          *  Note that part changes at shutdown won't be logged to part log.
          */
        if (system_logs)
            system_logs->shutdown();

        DatabaseCatalog::shutdown();

        NamedCollectionFactory::instance().shutdown();

#if USE_PYTHON_UDF
        /// Shutdown AsyncPythonPackageManager before other thread pools to ensure
        /// Python package operations complete and their threads are cleaned up
        if (async_python_package_manager)
        {
            async_python_package_manager->shutdown();
            async_python_package_manager.reset();
        }
#endif

        // delete_async_insert_queue.reset();

        // SHUTDOWN(log, "merges executor", merge_mutate_executor, wait());
        // SHUTDOWN(log, "fetches executor", fetch_executor, wait());
        // SHUTDOWN(log, "moves executor", moves_executor, wait());
        // SHUTDOWN(log, "common executor", common_executor, wait());

        TransactionLog::shutdownIfAny();

        std::unique_ptr<SystemLogs> delete_system_logs;
        std::unique_ptr<EmbeddedDictionaries> delete_embedded_dictionaries;
        std::unique_ptr<ExternalDictionariesLoader> delete_external_dictionaries_loader;
        std::unique_ptr<ExternalModelsLoader> delete_external_models_loader;
        std::unique_ptr<BackgroundSchedulePool> delete_buffer_flush_schedule_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_schedule_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_distributed_schedule_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_message_broker_schedule_pool;
        std::shared_ptr<AccessControl> delete_access_control; /// proton updates

        /// proton: starts
        std::unique_ptr<ThreadPool> delete_storage_commit_pool;
        std::unique_ptr<ThreadPool> delete_nlog_adhoc_schedule_pool;
        std::unique_ptr<NLOG::BackgroundSchedulePool> delete_nlog_schedule_pool;
        std::unique_ptr<ThreadPool> delete_global_adhoc_schedule_pool;
        std::unique_ptr<cluster::TimerService> delete_global_system_timer;


        {
            std::lock_guard lock(task_scheduler_mutex);
            if (task_scheduler)
            {
                task_scheduler->shutdown();
                task_scheduler.reset();
            }
        }
        /// proton: ends

        /// Background operations in cache use background schedule pool.
        /// Deactivate them before destructing it.
        const auto & caches = FileCacheFactory::instance().getAll();
        for (const auto & [_, cache] : caches)
            cache->cache->deactivateBackgroundOperations();

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
            delete_embedded_dictionaries = std::move(embedded_dictionaries);
            delete_external_dictionaries_loader = std::move(external_dictionaries_loader);
            delete_external_models_loader = std::move(external_models_loader);
            delete_buffer_flush_schedule_pool = std::move(buffer_flush_schedule_pool);
            delete_schedule_pool = std::move(schedule_pool);
            delete_distributed_schedule_pool = std::move(distributed_schedule_pool);
            delete_message_broker_schedule_pool = std::move(message_broker_schedule_pool);
            std::swap(delete_access_control, access_control); /// proton updates

            /// proton : starts.
            delete_storage_commit_pool = std::move(storage_commit_pool);
            delete_nlog_adhoc_schedule_pool =  std::move(nlog_adhoc_schedule_pool);
            delete_nlog_schedule_pool =  std::move(nlog_schedule_pool);
            delete_global_adhoc_schedule_pool = std::move(global_adhoc_schedule_pool);
            delete_global_system_timer = std::move(global_system_timer);
            /// proton : ends

            /// Stop trace collector if any
            trace_collector.reset();

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
        /// proton: starts. Teardown other components before thread pools tearing down
        /// since the components may depend on these thread pools
        DB::teardown(log);
        /// proton: ends.

        delete_system_logs.reset();
        delete_embedded_dictionaries.reset();
        delete_external_dictionaries_loader.reset();
        delete_external_models_loader.reset();
        delete_buffer_flush_schedule_pool.reset();
        delete_schedule_pool.reset();
        delete_distributed_schedule_pool.reset();
        delete_message_broker_schedule_pool.reset();
        delete_access_control.reset();

        /// proton: starts
        delete_storage_commit_pool.reset();
        delete_nlog_adhoc_schedule_pool.reset();
        delete_nlog_schedule_pool.reset();
        delete_global_adhoc_schedule_pool.reset();
        delete_global_system_timer.reset();
        /// proton: ends

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

    /// proton : starts
    void checkDiskUtil() const
    {
        /// Read from ServerDescriptor instead of instance_metrics
        const auto & server = Globals::getServerDescriptor();
        for (const auto & [name, stats] : server.disk_utils)
        {
            if (stats.util >= max_disk_util)
            {
                LOG_ERROR(log, "The utilization '{}' of disk='{}' exceeds the max_disk_util {}", stats.util, name, max_disk_util);

                throw Exception(
                    ErrorCodes::DISK_USAGE_RATIO_THRESHOLD_EXCEEDED,
                    "The utilization {} of disk {} exceeds the max_disk_util {}",
                    stats.util,
                    name,
                    max_disk_util);
            }
        }
    }
    /// proton : ends
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
    if (!ptr) throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't copy an expired context");
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
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Default database is not selected");
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

std::shared_ptr<IDisk> Context::getDatabaseDisk() const
{
    {
        SharedLockGuard lock(shared->mutex);
        if (shared->db_disk)
            return shared->db_disk;
    }

    // This is called first time early during the initialization.
    // Even if multiple threads try to get target_db_disk, only the first one will initialize the disks as there is another mutex in `getDiskMap()`
    // It is not necessary to introduce a mutex here.
    auto target_db_disk = [&]() -> std::shared_ptr<IDisk>
    {
        const auto & config = shared->getConfigRef();
        const auto & disk_map = getDisksMap();
        auto disk_name = config.getString("database_disk.disk", DiskSelector::DEFAULT_DISK_NAME);

        LOG_INFO(shared->log, "Database disk name: {}", disk_name);

        auto it = disk_map.find(disk_name);
        if (it == disk_map.end())
            throw Exception(ErrorCodes::UNKNOWN_DISK, "No disk {}", backQuote(disk_name));

        chassert(it->second);

        LOG_INFO(shared->log, "Database disk name: {}, path: {}", disk_name, it->second->getPath());
        return it->second;
    }();

    std::lock_guard lock(shared->mutex);
    if (shared->db_disk)
        return shared->db_disk;

    return shared->db_disk = target_db_disk;
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

/// TODO: remove, use `getTempDataOnDisk`
VolumePtr Context::getTemporaryVolume() const
{
    std::lock_guard lock(shared->mutex);
    if (shared->temp_data_on_disk)
        return shared->temp_data_on_disk->getVolume();
    return nullptr;
}

TemporaryDataOnDiskScopePtr Context::getTempDataOnDisk() const
{
    std::lock_guard lock(shared->mutex);
    if (this->temp_data_on_disk)
        return this->temp_data_on_disk;
    return shared->temp_data_on_disk;
}

void Context::setTempDataOnDisk(TemporaryDataOnDiskScopePtr temp_data_on_disk_)
{
    std::lock_guard lock(shared->mutex);
    this->temp_data_on_disk = std::move(temp_data_on_disk_);
}

void Context::setPath(const String & path)
{
    std::lock_guard lock(shared->mutex);

    shared->path = path;

    if (shared->tmp_path.empty() && !shared->temp_data_on_disk)
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

static void setupTmpPath(LoggerPtr log, const std::string & path)
try
{
    LOG_DEBUG(log, "Setting up {} to store temporary data in it", path);

    fs::create_directories(path);

    /// Clearing old temporary files.
    fs::directory_iterator dir_end;
    for (fs::directory_iterator it(path); it != dir_end; ++it)
    {
        if (it->is_regular_file() && startsWith(it->path().filename(), "tmp"))
        {
            LOG_DEBUG(log, "Removing old temporary file {}", it->path().string());
            fs::remove(it->path());
        }
        else
            LOG_DEBUG(log, "Found unknown file in temporary path {}", it->path().string());
    }
}
catch (...)
{
    DB::tryLogCurrentException(log, fmt::format(
        "Caught exception while setup temporary path: {}. "
        "It is ok to skip this exception as cleaning old temporary files is not necessary", path));
}

static VolumePtr createLocalSingleDiskVolume(const std::string & path, const Poco::Util::AbstractConfiguration & config_)
{
    auto disk = std::make_shared<DiskLocal>("_tmp_default", path, 0, config_, "storage_configuration.disks._tmp_default");
    VolumePtr volume = std::make_shared<SingleDiskVolume>("_tmp_default", disk, 0);
    return volume;
}

void Context::setTemporaryStoragePath(const String & path, size_t max_size)
{
    std::lock_guard lock(shared->mutex);

    shared->tmp_path = path;
    if (!shared->tmp_path.ends_with('/'))
        shared->tmp_path += '/';

    VolumePtr volume = createLocalSingleDiskVolume(shared->tmp_path, shared->getConfigRefWithLock(lock));

    for (const auto & disk : volume->getDisks())
    {
        setupTmpPath(shared->log, disk->getPath());
    }

    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, max_size);
}

void Context::setTemporaryStoragePolicy(const String & policy_name, size_t max_size)
{
    std::lock_guard lock(shared->storage_policies_mutex);

    StoragePolicyPtr tmp_policy = getStoragePolicySelector(lock)->get(policy_name);
    if (tmp_policy->getVolumes().size() != 1)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
        "Policy '{}' is used temporary files, such policy should have exactly one volume", policy_name);
    VolumePtr volume = tmp_policy->getVolume(0);

    if (volume->getDisks().empty())
         throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No disks volume for temporary files");

    for (const auto & disk : volume->getDisks())
    {
        if (!disk)
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Temporary disk is null");

        /// Check that underlying disk is local (can be wrapped in decorator)
        DiskPtr disk_ptr = disk;

        if (dynamic_cast<const DiskLocal *>(disk_ptr.get()) == nullptr)
        {
            const auto * disk_raw_ptr = disk_ptr.get();
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                "Disk '{}' ({}) is not local and can't be used for temporary files",
                disk_ptr->getName(), typeid(*disk_raw_ptr).name());
        }

        setupTmpPath(shared->log, disk->getPath());
    }

    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, max_size);
}


void Context::setTemporaryStorageInCache(const String & cache_disk_name, size_t max_size)
{
    auto disk_ptr = getDisk(cache_disk_name);
    if (!disk_ptr)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Disk '{}' is not found", cache_disk_name);

    std::lock_guard lock(shared->mutex);
    auto file_cache = FileCacheFactory::instance().getByName(disk_ptr->getCacheName()).cache;
    if (!file_cache)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Cache '{}' is not found", file_cache->getBasePath());

    LOG_DEBUG(shared->log, "Using file cache ({}) for temporary files", file_cache->getBasePath());

    shared->tmp_path = file_cache->getBasePath();
    VolumePtr volume = createLocalSingleDiskVolume(shared->tmp_path, getConfigRef());
    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, file_cache.get(), max_size);
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

/// proton starts: return shared_ptr copy to fix use after dtor on shutdown
std::shared_ptr<AccessControl> Context::getAccessControl() const
{
    SharedLockGuard lock(shared->mutex);
    chassert(shared->access_control != nullptr);
    return shared->access_control;
}
/// proton ends

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

    access = getAccessControl()->getContextAccess(
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

void Context::calculateAccessRightsWithLock([[maybe_unused]] const std::lock_guard<ContextSharedMutex> & lock)
{
    if (user_id)
        access = getAccessControl()->getContextAccess(
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
    auto initial_user_id = getAccessControl()->find<User>(client_info.initial_user);
    if (!initial_user_id)
        return;
    row_policies_of_initial_user = getAccessControl()->tryGetDefaultRowPolicies(*initial_user_id);
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
        UUID profile_id = getAccessControl()->getID<SettingsProfile>(profile_name);
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
    auto profile_info = getAccessControl()->getSettingsProfileInfo(profile_id);
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


ResourceManagerPtr Context::getResourceManager() const
{
    callOnce(shared->resource_manager_initialized, [&] {
        shared->resource_manager = shared->resource_manager = ResourceManagerFactory::instance().get(getConfigRef().getString("resource_manager", "static"));
    });

    return shared->resource_manager;
}

ClassifierPtr Context::getWorkloadClassifier() const
{
    std::lock_guard lock(mutex);
    // NOTE: Workload cannot be changed after query start, and getWorkloadClassifier() should not be called before proper `workload` is set
    if (!classifier)
        classifier = getResourceManager()->acquire(getSettingsRef().workload);
    return classifier;
}


Scalars Context::getScalars() const
{
    std::lock_guard lock(mutex);
    return scalars;
}


const Block & Context::getScalar(const String & name) const
{
    auto it = scalars.find(name);
    if (scalars.end() == it)
    {
        // This should be a logical error, but it fails the sql_fuzz test too
        // often, so 'bad arguments' for now.
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Scalar {} doesn't exist (internal bug)", backQuoteIfNeed(name));
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
    if (external_tables_mapping.contains(table_name))
        throw Exception(ErrorCodes::STREAM_ALREADY_EXISTS, "Temporary stream {} already exists.", backQuoteIfNeed(table_name));
    external_tables_mapping.emplace(table_name, std::make_shared<TemporaryTableHolder>(std::move(temporary_table)));
}

std::shared_ptr<TemporaryTableHolder> Context::findExternalTable(const String & table_name) const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    std::shared_ptr<TemporaryTableHolder> holder;
    {
        std::lock_guard lock(shared->mutex);
        auto iter = external_tables_mapping.find(table_name);
        if (iter == external_tables_mapping.end())
            return {};
        holder = iter->second;
    }
    return holder;
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


static bool findIdentifier(const ASTFunction * function)
{
    if (!function || !function->arguments)
        return false;
    if (const auto * arguments = function->arguments->as<ASTExpressionList>())
    {
        for (const auto & argument : arguments->children)
        {
            if (argument->as<ASTIdentifier>())
                return true;
            if (const auto * f = argument->as<ASTFunction>(); f && findIdentifier(f))
                return true;
        }
    }
    return false;
}

/// proton: starts.
StoragePtr Context::getTableFunctionResults(const String & key) const
{
    SharedLockGuard lock(mutex);
    auto it = table_function_results.find(key);
    if (it != table_function_results.end())
        return it->second;
    return nullptr;
}

void Context::setTableFunctionResults(const String & key, const StoragePtr & table_function_result)
{
    std::lock_guard lock(mutex);
    table_function_results.emplace(key, table_function_result);
}
/// proton: ends.

StoragePtr Context::executeTableFunction(const ASTPtr & table_expression, const ASTSelectQuery * select_query_hint)
{
    ASTFunction * function = assert_cast<ASTFunction *>(table_expression.get());
    String database_name = getCurrentDatabase();
    String table_name = function->name;

    if (function->is_compound_name)
    {
        std::vector<std::string> parts;
        splitInto<'.'>(parts, function->name);

        if (parts.size() == 2)
        {
            database_name = parts[0];
            table_name = parts[1];
        }
    }

    StoragePtr table = DatabaseCatalog::instance().tryGetTable({database_name, table_name}, getQueryContext());
    if (table)
    {
        if (table.get()->isView() && table->as<StorageView>() && table->as<StorageView>()->isParameterizedView())
        {
            auto query = table->getInMemoryMetadataPtr()->getSelectQuery().inner_query->clone();
            NameToNameMap parameterized_view_values = analyzeFunctionParamValues(table_expression, getQueryContext());
            StorageView::replaceQueryParametersIfParametrizedView(query, parameterized_view_values);

            ASTCreateQuery create;
            create.select = query->as<ASTSelectWithUnionQuery>();
            auto sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(query, getQueryContext(), false);
            auto res = StorageView::create(StorageID(database_name, table_name),
                                                     create,
                                                     ColumnsDescription(sample_block.getNamesAndTypesList()),
                                                     /* comment */ "",
                                                     getQueryContext(),
                                                     /* is_parameterized_view */ true);
            res->startup();
            function->prefer_subquery_to_function_formatting = true;
            return res;
        }
    }
    auto hash = table_expression->getTreeHash();
    String key = toString(hash.first) + '_' + toString(hash.second);
    /// proton: starts.
    auto res = getTableFunctionResults(key);
    /// proton: ends.
    if (!res)
    {
#if USE_PYTHON_UDF
        if (table)
        {
            const auto * external_stream = table->as<StorageExternalStream>();
            if (external_stream && external_stream->getType() == ExternalStreamTypes::PYTHON)
            {
                /// Prefer built-in table functions/aliases over external stream rewrites to avoid
                /// surprising name collisions (e.g. external stream named "remote"/"python").
                if (TableFunctionFactory::instance().tryGetProperties(function->name))
                {
                    /// Fall through to the normal table function resolution path below.
                }
                else
                {
                    const auto * args = function->arguments ? function->arguments->as<ASTExpressionList>() : nullptr;
                    if (!args || args->children.empty())
                        return table;

                    ASTs python_args;
                    python_args.reserve(args->children.size() + 1);
                    if (!database_name.empty())
                        python_args.emplace_back(std::make_shared<ASTIdentifier>(std::vector<String>{database_name, table_name}));
                    else
                        python_args.emplace_back(std::make_shared<ASTIdentifier>(table_name));
                    for (const auto & arg : args->children)
                        python_args.emplace_back(arg->clone());

                    auto python_table_ast = makeASTFunction("python_table", std::move(python_args));

                    try
                    {
                        auto python_table_func = TableFunctionFactory::instance().get(python_table_ast, shared_from_this());
                        res = python_table_func->execute(python_table_ast, shared_from_this(), python_table_func->getName());
                        setTableFunctionResults(key, res);
                        return res;
                    }
                    catch (Exception & e)
                    {
                        e.addMessage(" while resolving Python external stream '{}'", table->getStorageID().getNameForLogs());
                        throw;
                    }
                }
            }
        }
#endif

        TableFunctionPtr table_function_ptr;
        try
        {
            table_function_ptr = TableFunctionFactory::instance().get(table_expression, shared_from_this());
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_FUNCTION)
            {
                e.addMessage(" or incorrect parameterized view");
            }
            throw;
        }

        uint64_t use_structure_from_insertion_table_in_table_functions = getSettingsRef().use_structure_from_insertion_table_in_table_functions;
        if (use_structure_from_insertion_table_in_table_functions && table_function_ptr->needStructureHint() && hasInsertionTable())
        {
            const auto & insert_columns = DatabaseCatalog::instance()
                                              .getTable(getInsertionTable(), shared_from_this())
                                              ->getInMemoryMetadataPtr()
                                              ->getColumns();

            const auto & insert_column_names = hasInsertionTableColumnNames() ? *getInsertionTableColumnNames() : insert_columns.getOrdinary().getNames();
            DB::ColumnsDescription structure_hint;

            bool use_columns_from_insert_query = true;

            /// Insert table matches columns against SELECT expression by position, so we want to map
            /// insert table columns to table function columns through names from SELECT expression.

            auto insert_column_name_it = insert_column_names.begin();
            auto insert_column_names_end = insert_column_names.end();  /// end iterator of the range covered by possible asterisk
            auto virtual_column_names = table_function_ptr->getVirtualsToCheckBeforeUsingStructureHint();
            bool asterisk = false;
            const auto & expression_list = select_query_hint->select()->as<ASTExpressionList>()->children;
            auto expression = expression_list.begin();

            /// We want to go through SELECT expression list and correspond each expression to column in insert table
            /// which type will be used as a hint for the file structure inference.
            for (; expression != expression_list.end() && insert_column_name_it != insert_column_names_end; ++expression)
            {
                if (auto * identifier = (*expression)->as<ASTIdentifier>())
                {
                    if (!virtual_column_names.contains(identifier->name()))
                    {
                        if (asterisk)
                        {
                            if (use_structure_from_insertion_table_in_table_functions == 1)
                                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Asterisk cannot be mixed with column list in INSERT SELECT query.");

                            use_columns_from_insert_query = false;
                            break;
                        }

                        ColumnDescription column = insert_columns.get(*insert_column_name_it);
                        column.name = identifier->name();
                        /// Change ephemeral columns to default columns.
                        column.default_desc.kind = ColumnDefaultKind::Default;
                        structure_hint.add(std::move(column));
                    }

                    /// Once we hit asterisk we want to find end of the range covered by asterisk
                    /// contributing every further SELECT expression to the tail of insert structure
                    if (asterisk)
                        --insert_column_names_end;
                    else
                        ++insert_column_name_it;
                }
                else if ((*expression)->as<ASTAsterisk>())
                {
                    if (asterisk)
                    {
                        if (use_structure_from_insertion_table_in_table_functions == 1)
                            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Only one asterisk can be used in INSERT SELECT query.");

                        use_columns_from_insert_query = false;
                        break;
                    }
                    if (!structure_hint.empty())
                    {
                        if (use_structure_from_insertion_table_in_table_functions == 1)
                            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Asterisk cannot be mixed with column list in INSERT SELECT query.");

                        use_columns_from_insert_query = false;
                        break;
                    }

                    asterisk = true;
                }
                else if (auto * func = (*expression)->as<ASTFunction>())
                {
                    if (use_structure_from_insertion_table_in_table_functions == 2 && findIdentifier(func))
                    {
                        use_columns_from_insert_query = false;
                        break;
                    }

                    /// Once we hit asterisk we want to find end of the range covered by asterisk
                    /// contributing every further SELECT expression to the tail of insert structure
                    if (asterisk)
                        --insert_column_names_end;
                    else
                        ++insert_column_name_it;
                }
                else
                {
                    /// Once we hit asterisk we want to find end of the range covered by asterisk
                    /// contributing every further SELECT expression to the tail of insert structure
                    if (asterisk)
                        --insert_column_names_end;
                    else
                        ++insert_column_name_it;
                }
            }

            if (use_structure_from_insertion_table_in_table_functions == 2 && !asterisk)
            {
                /// For input function we should check if input format supports reading subset of columns.
                if (table_function_ptr->getName() == "input")
                    use_columns_from_insert_query = FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(getInsertFormat(), shared_from_this());
                else
                    use_columns_from_insert_query = table_function_ptr->supportsReadingSubsetOfColumns(shared_from_this());
            }

            if (use_columns_from_insert_query)
            {
                if (expression == expression_list.end())
                {
                    /// Append tail of insert structure to the hint
                    if (asterisk)
                    {
                        for (; insert_column_name_it != insert_column_names_end; ++insert_column_name_it)
                        {
                            ColumnDescription column = insert_columns.get(*insert_column_name_it);
                            /// Change ephemeral columns to default columns.
                            column.default_desc.kind = ColumnDefaultKind::Default;

                            structure_hint.add(std::move(column));
                        }
                    }

                    if (!structure_hint.empty())
                        table_function_ptr->setStructureHint(structure_hint);

                } else if (use_structure_from_insertion_table_in_table_functions == 1)
                    throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH, "Number of columns in insert table less than required by SELECT expression.");
            }

            if (use_columns_from_insert_query)
                table_function_ptr->setStructureHint(structure_hint);
        }

        res = table_function_ptr->execute(table_expression, shared_from_this(), table_function_ptr->getName());
        setTableFunctionResults(key, res);

        /// Since ITableFunction::parseArguments() may change table_expression, i.e.:
        ///
        ///     remote('127.1', system.one) -> remote('127.1', 'system.one'),
        ///
        auto new_hash = table_expression->getTreeHash();
        if (hash != new_hash)
        {
            key = toString(new_hash.first) + '_' + toString(new_hash.second);
            /// proton: starts.
            setTableFunctionResults(key, res);
            /// proton: ends.
        }
    }
    return res;
}


void Context::addViewSource(const StoragePtr & storage)
{
    if (view_source)
        throw Exception(ErrorCodes::STREAM_ALREADY_EXISTS, "Temporary view source storage {} already exists.",
            backQuoteIfNeed(view_source->getName()));
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

    /// proton: starts
    if (name == "idempotent_id")
        setIdempotentKey(value);
    /// proton: ends

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

    /// proton: starts
    if (name == "idempotent_id")
        setIdempotentKey(value.safeGet<String>());
    /// proton: ends

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
        e.addMessage(fmt::format(
                         "in attempt to set the value of setting '{}' to {}",
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


void Context::checkSettingsConstraints(const SettingsProfileElements & profile_elements) const
{
    getSettingsConstraintsAndCurrentProfiles()->constraints.check(settings, profile_elements);
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

void Context::checkMergeTreeSettingsConstraints(const MergeTreeSettings & merge_tree_settings, const SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfiles()->constraints.check(merge_tree_settings, changes);
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
    static auto no_constraints_or_profiles = std::make_shared<SettingsConstraintsAndProfileIDs>(*getAccessControl());
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
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot set current database for non global context, this method should "
                        "be used during server initialization");
    std::lock_guard lock(mutex);

    if (!current_database.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Default database name cannot be changed in global context without server restart");

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

    /// proton: starts.
    spill_id_for_current_query = std::make_shared<std::atomic<UInt32>>(0);
    /// proton: ends.
}

void Context::killCurrentQuery() const
{
    if (auto elem = getProcessListElement())
        elem->cancelQuery(true);
}

String Context::getDefaultFormat() const
{
    return default_format.empty() ? "TabSeparated" : default_format;
}

void Context::setDefaultFormat(const String & name)
{
    default_format = name;
}

String Context::getInsertFormat() const
{
    return insert_format;
}

void Context::setInsertFormat(const String & name)
{
    insert_format = name;
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
    if (!ptr) throw Exception(ErrorCodes::THERE_IS_NO_QUERY, "There is no query or query context has expired");
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
    if (!ptr) throw Exception(ErrorCodes::THERE_IS_NO_SESSION, "There is no session or session context has expired");
    return ptr;
}

ContextMutablePtr Context::getGlobalContext() const
{
    auto ptr = global_context.lock();
    if (!ptr) throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no global context or global context has expired");
    return ptr;
}

ContextMutablePtr Context::getBufferContext() const
{
    if (!buffer_context) throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no buffer context");
    return buffer_context;
}

void Context::makeQueryContext()
{
    query_context = shared_from_this();

    /// Throttling should not be inherited, otherwise if you will set
    /// throttling for default profile you will not able to overwrite it
    /// per-user/query.
    ///
    /// Note, that if you need to set it server-wide, you should use
    /// per-server settings, i.e.:
    /// - max_backup_bandwidth_for_server
    /// - max_remote_read_network_bandwidth_for_server
    /// - max_remote_write_network_bandwidth_for_server
    /// - max_local_read_bandwidth_for_server
    /// - max_local_write_bandwidth_for_server
    remote_read_query_throttler.reset();
    remote_write_query_throttler.reset();
    local_read_query_throttler.reset();
    local_write_query_throttler.reset();
    backups_query_throttler.reset();
}

void Context::makeSessionContext()
{
    session_context = shared_from_this();
}

void Context::makeGlobalContext()
{
    initGlobal();
    global_context = shared_from_this();
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

/// proton: starts
void Context::setWriteProgressCallback(ProgressCallback callback)
{
    write_progress_callback = callback;
}

ProgressCallback Context::getWriteProgressCallback() const
{
    return write_progress_callback;
}
/// proton: ends

void Context::setProcessListElement(QueryStatusPtr elem)
{
    /// Set to a session or query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    process_list_elem = elem;
    has_process_list_elem = elem.get();
}

QueryStatusPtr Context::getProcessListElement() const
{
    if (!has_process_list_elem)
        return {};
    if (auto res = process_list_elem.lock())
        return res;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Weak pointer to process_list_elem expired during query execution, it's a bug");
}


void Context::setUncompressedCache(size_t max_size_in_bytes)
{
    std::lock_guard lock(shared->mutex);

    if (shared->uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Uncompressed cache has been already created.");

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark cache has been already created.");

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
        shared->load_marks_threadpool = std::make_unique<ThreadPool>(
            CurrentMetrics::MarksLoaderThreads, CurrentMetrics::MarksLoaderThreadsActive, pool_size, pool_size, queue_size);
    });

    return *shared->load_marks_threadpool;
}

static size_t getPrefetchThreadpoolSizeFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    return config.getUInt(".prefetch_threadpool_pool_size", 100);
}

size_t Context::getPrefetchThreadpoolSize() const
{
    const auto & config = getConfigRef();
    return getPrefetchThreadpoolSizeFromConfig(config);
}

ThreadPool & Context::getPrefetchThreadpool() const
{
    const auto & config = getConfigRef();

    std::lock_guard lock(shared->mutex);
    if (!shared->prefetch_threadpool)
    {
        auto pool_size = getPrefetchThreadpoolSize();
        auto queue_size = config.getUInt(".prefetch_threadpool_queue_size", 1000000);
        shared->prefetch_threadpool = std::make_unique<ThreadPool>(
            CurrentMetrics::IOPrefetchThreads, CurrentMetrics::IOPrefetchThreadsActive, pool_size, pool_size, queue_size);
    }
    return *shared->prefetch_threadpool;
}

void Context::setIndexUncompressedCache(size_t max_size_in_bytes)
{
    std::lock_guard lock(shared->mutex);

    if (shared->index_uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index uncompressed cache has been already created.");

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index mark cache has been already created.");

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped file cache has been already created.");

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
            shared->server_settings.background_buffer_flush_schedule_pool_size,
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
            shared->server_settings.background_schedule_pool_size,
            CurrentMetrics::BackgroundSchedulePoolTask,
            "BgSchPool");
    });
    return *shared->schedule_pool;
}

BackgroundSchedulePool & Context::getDistributedSchedulePool() const
{
    callOnce(shared->distributed_schedule_pool_initialized, [&] {
        shared->distributed_schedule_pool = std::make_unique<BackgroundSchedulePool>(
            shared->server_settings.background_distributed_schedule_pool_size,
            CurrentMetrics::BackgroundDistributedSchedulePoolTask,
            "BgDistSchPool");
    });
    return *shared->distributed_schedule_pool;
}

BackgroundSchedulePool & Context::getMessageBrokerSchedulePool() const
{
    callOnce(shared->message_broker_schedule_pool_initialized, [&] {
        shared->message_broker_schedule_pool = std::make_unique<BackgroundSchedulePool>(
            shared->server_settings.background_message_broker_schedule_pool_size,
            CurrentMetrics::BackgroundMessageBrokerSchedulePoolTask,
            "BgMBSchPool");
    });
    return *shared->message_broker_schedule_pool;
}

ThrottlerPtr Context::getRemoteReadThrottler() const
{
    ThrottlerPtr throttler;

    const auto & query_settings = getSettingsRef();
    UInt64 bandwidth_for_server = shared->server_settings.max_remote_read_network_bandwidth_for_server;
    if (bandwidth_for_server)
    {
        std::lock_guard lock(mutex);
        if (!shared->remote_read_throttler)
            shared->remote_read_throttler = std::make_shared<Throttler>(bandwidth_for_server);
        throttler = shared->remote_read_throttler;
    }

    if (query_settings.max_remote_read_network_bandwidth)
    {
        std::lock_guard lock(mutex);
        if (!remote_read_query_throttler)
            remote_read_query_throttler = std::make_shared<Throttler>(query_settings.max_remote_read_network_bandwidth, throttler);
        throttler = remote_read_query_throttler;
    }

    return throttler;
}

ThrottlerPtr Context::getRemoteWriteThrottler() const
{
    ThrottlerPtr throttler;

    const auto & query_settings = getSettingsRef();
    UInt64 bandwidth_for_server = shared->server_settings.max_remote_write_network_bandwidth_for_server;
    if (bandwidth_for_server)
    {
        std::lock_guard lock(mutex);
        if (!shared->remote_write_throttler)
            shared->remote_write_throttler = std::make_shared<Throttler>(bandwidth_for_server);
        throttler = shared->remote_write_throttler;
    }

    if (query_settings.max_remote_write_network_bandwidth)
    {
        std::lock_guard lock(mutex);
        if (!remote_write_query_throttler)
            remote_write_query_throttler = std::make_shared<Throttler>(query_settings.max_remote_write_network_bandwidth, throttler);
        throttler = remote_write_query_throttler;
    }

    return throttler;
}

ThrottlerPtr Context::getLocalReadThrottler() const
{
    ThrottlerPtr throttler;

    if (shared->server_settings.max_local_read_bandwidth_for_server)
    {
        std::lock_guard lock(mutex);
        if (!shared->local_read_throttler)
            shared->local_read_throttler = std::make_shared<Throttler>(shared->server_settings.max_local_read_bandwidth_for_server);
        throttler = shared->local_read_throttler;
    }

    const auto & query_settings = getSettingsRef();
    if (query_settings.max_local_read_bandwidth)
    {
        std::lock_guard lock(mutex);
        if (!local_read_query_throttler)
            local_read_query_throttler = std::make_shared<Throttler>(query_settings.max_local_read_bandwidth, throttler);
        throttler = local_read_query_throttler;
    }

    return throttler;
}

ThrottlerPtr Context::getLocalWriteThrottler() const
{
    ThrottlerPtr throttler;

    if (shared->server_settings.max_local_write_bandwidth_for_server)
    {
        std::lock_guard lock(mutex);
        if (!shared->local_write_throttler)
            shared->local_write_throttler = std::make_shared<Throttler>(shared->server_settings.max_local_write_bandwidth_for_server);
        throttler = shared->local_write_throttler;
    }

    const auto & query_settings = getSettingsRef();
    if (query_settings.max_local_write_bandwidth)
    {
        std::lock_guard lock(mutex);
        if (!local_write_query_throttler)
            local_write_query_throttler = std::make_shared<Throttler>(query_settings.max_local_write_bandwidth, throttler);
        throttler = local_write_query_throttler;
    }

    return throttler;
}

ThrottlerPtr Context::getBackupsThrottler() const
{
    ThrottlerPtr throttler;

    if (shared->server_settings.max_backup_bandwidth_for_server)
    {
        std::lock_guard lock(mutex);
        if (!shared->backups_server_throttler)
            shared->backups_server_throttler = std::make_shared<Throttler>(shared->server_settings.max_backup_bandwidth_for_server);
        throttler = shared->backups_server_throttler;
    }

    const auto & query_settings = getSettingsRef();
    if (query_settings.max_backup_bandwidth)
    {
        std::lock_guard lock(mutex);
        if (!backups_query_throttler)
            backups_query_throttler = std::make_shared<Throttler>(query_settings.max_backup_bandwidth, throttler);
        throttler = backups_query_throttler;
    }

    return throttler;
}

/// proton: starts.
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

void Context::setTaskScheduler(std::shared_ptr<Task::TaskScheduler> scheduler)
{
    std::lock_guard lock(shared->task_scheduler_mutex);
    shared->task_scheduler = std::move(scheduler);
}

std::shared_ptr<Task::TaskScheduler> Context::getTaskScheduler() const
{
    std::lock_guard lock(shared->task_scheduler_mutex);
    return shared->task_scheduler;
}
/// proton: ends.

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
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                        "Parameter 'interserver_http(s)_port' required for replication is not specified "
                        "in configuration file.");

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
    shared->remote_host_filter.setValuesFromConfig(config);
}

const RemoteHostFilter & Context::getRemoteHostFilter() const
{
    return shared->remote_host_filter;
}

cluster::TCPPort Context::getTCPPort() const
{
    const auto & server = Globals::getServerDescriptor();
    return server.tcp_port;
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

std::shared_ptr<MaterializedViewDeadLetterQueue> Context::getMaterializedViewDLQ() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->mv_dlq;
}

std::shared_ptr<StreamMetricLog> Context::getStreamMetricLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->stream_metric_log;
}

std::shared_ptr<IntrospectionStateLog> Context::getIntrospectionStateLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->introspection_state_log;
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

    return shared->system_logs->filesystem_cache_log;
}

std::shared_ptr<FilesystemReadPrefetchesLog> Context::getFilesystemReadPrefetchesLog() const
{
    SharedLockGuard lock(shared->mutex);
    if (!shared->system_logs)
        return {};

    return shared->system_logs->filesystem_read_prefetches_log;
}

std::shared_ptr<AsynchronousInsertLog> Context::getAsynchronousInsertLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->asynchronous_insert_log;
}

std::shared_ptr<BlobStorageLog> Context::getBlobStorageLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};
    return shared->system_logs->blob_storage_log;
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

DiskPtr Context::getOrCreateDisk(const String & name, DiskCreator disk_creator) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto disk_selector = getDiskSelector(lock);

    auto disk = disk_selector->tryGet(name);
    if (!disk)
    {
        disk = disk_creator(getDisksMap(lock));
        const_cast<DiskSelector *>(disk_selector.get())->addToDiskMap(name, disk);
    }

    return disk;
}

StoragePolicyPtr Context::getStoragePolicy(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto policy_selector = getStoragePolicySelector(lock);

    return policy_selector->get(name);
}

StoragePolicyPtr Context::getStoragePolicyFromDisk(const String & disk_name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    const std::string storage_policy_name = StoragePolicySelector::TMP_STORAGE_POLICY_PREFIX + disk_name;
    auto storage_policy_selector = getStoragePolicySelector(lock);
    StoragePolicyPtr storage_policy = storage_policy_selector->tryGet(storage_policy_name);

    if (!storage_policy)
    {
        auto disk_selector = getDiskSelector(lock);
        auto disk = disk_selector->get(disk_name);
        auto volume = std::make_shared<SingleDiskVolume>("_volume_" + disk_name, disk);

        static const auto move_factor_for_single_disk_volume = 0.0;
        storage_policy = std::make_shared<StoragePolicy>(storage_policy_name, Volumes{volume}, move_factor_for_single_disk_volume);
        const_cast<StoragePolicySelector *>(storage_policy_selector.get())->add(storage_policy);
    }
    /// Note: it is important to put storage policy into disk selector (and not recreate it on each call)
    /// because in some places there are checks that storage policy pointers are the same from different tables.
    /// (We can assume that tables with the same `disk` setting are on the same storage policy).

    return storage_policy;
}

StoragePolicyPtr Context::getStoragePolicyFromConfiguration(const String & name, const Poco::AutoPtr<Poco::Util::AbstractConfiguration> & config) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto storage_policy_selector = getStoragePolicySelector(lock);
    StoragePolicyPtr storage_policy = storage_policy_selector->tryGet(name);

    if (!storage_policy)
    {
        auto disk_selector = getDiskSelector(lock);
        storage_policy = std::make_shared<StoragePolicy>(name, *config, name, disk_selector);
        const_cast<StoragePolicySelector *>(storage_policy_selector.get())->add(storage_policy);
    }
    /// Note: it is important to put storage policy into disk selector (and not recreate it on each call)
    /// because in some places there are checks that storage policy pointers are the same from different tables.
    /// (We can assume that tables with the same `disk` setting are on the same storage policy).

    return storage_policy;
}

void Context::deleteStoragePolicy(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto policy_selector = getStoragePolicySelector(lock);
    auto policy = policy_selector->tryGet(name);
    if (!policy)
        return;

    const_cast<StoragePolicySelector *>(policy_selector.get())->remove(name);
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

void Context::updateDisk(const String & /*name*/, const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->storage_policies_mutex);

    if (shared->merge_tree_disk_selector)
        shared->merge_tree_disk_selector
            = shared->merge_tree_disk_selector->updateFromConfig(config, "disks", shared_from_this());
}

void Context::deleteDisk(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto disk_selector = getDiskSelector(lock);

    auto disk = disk_selector->tryGet(name);
    if (!disk)
        return;

    const_cast<DiskSelector *>(disk_selector.get())->removeFromDiskMap(name);
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
    /// proton: starts.
    throw Exception(ErrorCodes::STREAM_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT,
                    "Stream or Partition in {}.{} was not dropped.\nReason:\n"
                    "1. Size ({}) is greater than max_[stream/partition]_size_to_drop ({})\n"
                    "2. File '{}' intended to force DROP {}\n"
                    "3. Setting 'force_drop_big_stream' is not set\n"
                    "How to fix this, choose one of the following options:\n"
                    "1. Increase (or set to zero) max_[stream/partition]_size_to_drop in server config\n"
                    "2. Create force-drop file {} and make sure that proton has write permission for it.\n"
                    "3. Add the per-statement setting to the query, for example:\n"
                    "   DROP STREAM IF EXISTS {}.{} SETTINGS force_drop_big_stream = true;\n"
                    "Force-drop file example:\nsudo touch '{}' && sudo chmod 666 '{}'",
                    backQuoteIfNeed(database), backQuoteIfNeed(table),
                    size_str, max_size_to_drop_str,
                    force_file.string(), force_file_exists ? "exists but not writeable (could not be removed)" : "doesn't exist",
                    force_file.string(),
                    backQuoteIfNeed(database), backQuoteIfNeed(table),
                    force_file.string(), force_file.string());
    /// proton: ends.
}


void Context::setMaxTableSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_stream_size_to_drop.store(max_size, std::memory_order_relaxed);
}

void Context::checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const
{
    size_t max_stream_size_to_drop = shared->max_stream_size_to_drop.load();

    checkCanBeDropped(database, table, table_size, max_stream_size_to_drop);
}


void Context::setMaxPartitionSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_partition_size_to_drop.store(max_size, std::memory_order_relaxed);
}

size_t Context::getMaxPartitionSizeToDrop() const
{
    return shared->max_partition_size_to_drop.load();
}

void Context::checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const
{
    size_t max_partition_size_to_drop = shared->max_partition_size_to_drop.load();

    checkCanBeDropped(database, table, partition_size, max_partition_size_to_drop);
}


InputFormatPtr Context::getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size, const std::optional<FormatSettings> & format_settings, const std::optional<size_t> max_parsing_threads) const
{
    return FormatFactory::instance().getInput(name, buf, sample, shared_from_this(), max_block_size, format_settings, max_parsing_threads);
}

OutputFormatPtr Context::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample, const std::optional<FormatSettings> & format_settings) const
{
    return FormatFactory::instance().getOutputFormat(name, buf, sample, shared_from_this(), format_settings);
}

OutputFormatPtr Context::getOutputFormatParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample, const std::optional<FormatSettings> & format_settings) const
{
    return FormatFactory::instance().getOutputFormatParallelIfPossible(name, buf, sample, shared_from_this(), format_settings);
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't reload config because config_reload_callback is not set.");

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

    /// Special volumes might also use disks that require shutdown.
    auto & tmp_data = shared->temp_data_on_disk;
    if (tmp_data && tmp_data->getVolume())
    {
        auto & disks = tmp_data->getVolume()->getDisks();
        for (auto & disk : disks)
            disk->shutdown();
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

    if (type == ApplicationType::SERVER)
        shared->server_settings.loadSettingsFromConfig(Poco::Util::Application::instance().config());
}

void Context::setDefaultProfiles(const Poco::Util::AbstractConfiguration & config)
{
    shared->default_profile_name = config.getString("default_profile", "default");
    getAccessControl()->setDefaultProfileName(shared->default_profile_name);

    shared->system_profile_name = config.getString("system_profile", shared->default_profile_name);
    setCurrentProfile(shared->system_profile_name);

    applySettingsQuirks(settings, getLogger("SettingsQuirks"));

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

String Context::getGoogleProtosPath() const
{
    return shared->google_protos_path;
}

void Context::setGoogleProtosPath(const String & path)
{
    shared->google_protos_path = path;
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate name {} of query parameter", backQuote(name));
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "External tables initializer is already set");

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input initializer is already set");

    input_initializer_callback = std::move(initializer);
}


void Context::initializeInput(const StoragePtr & input_storage)
{
    if (!input_initializer_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input initializer is not set");

    input_initializer_callback(shared_from_this(), input_storage);
    /// Reset callback
    input_initializer_callback = {};
}


void Context::setInputBlocksReaderCallback(InputBlocksReader && reader)
{
    if (input_blocks_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input blocks reader is already set");

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

void Context::setClientInfo(const ClientInfo & client_info_)
{
    client_info = client_info_;
    need_recalculate_access = true;
}

void Context::increaseDistributedDepth()
{
    ++client_info.distributed_depth;
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
            exception->emplace(ErrorCodes::UNKNOWN_STREAM, "Both stream name and UUID are empty");
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
            exception->emplace(Exception(ErrorCodes::UNKNOWN_STREAM, "External and temporary tables have no database, but {} is specified",
                               storage_id.database_name));
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
                exception->emplace(ErrorCodes::UNKNOWN_DATABASE, "Default database is not selected");
            return StorageID::createEmpty();
        }
        storage_id.database_name = current_database;
        /// NOTE There is no guarantees that table actually exists in database.
        return storage_id;
    }

    if (exception)
        exception->emplace(Exception(ErrorCodes::UNKNOWN_STREAM, "Cannot resolve database name for stream {}", storage_id.getNameForLogs()));
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


MergeTreeAllRangesCallback Context::getMergeTreeAllRangesCallback() const
{
    if (!merge_tree_all_ranges_callback.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Next task callback is not set for query with id: {}", getInitialQueryId());

    return merge_tree_all_ranges_callback.value();
}


void Context::setMergeTreeAllRangesCallback(MergeTreeAllRangesCallback && callback)
{
    merge_tree_all_ranges_callback = callback;
}


void Context::setParallelReplicasGroupUUID(UUID uuid)
{
    parallel_replicas_group_uuid = uuid;
}

UUID Context::getParallelReplicasGroupUUID() const
{
    return parallel_replicas_group_uuid;
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
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting async_insert_busy_timeout_ms can't be zero");

    shared->async_insert_queue = ptr;
}

#if USE_PYTHON_UDF
cpython::AsyncPythonPackageManager * Context::getAsyncPythonPackageManager() const
{
    return shared->async_python_package_manager.get();
}

void Context::setAsyncPythonPackageManager(const std::shared_ptr<cpython::AsyncPythonPackageManager> & ptr)
{
    shared->async_python_package_manager = ptr;
}
#endif

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

OrdinaryBackgroundExecutorPtr Context::getCommonExecutor() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->common_executor;
}

IAsynchronousReader & Context::getThreadPoolReader(FilesystemReaderType type) const
{
    callOnce(shared->readers_initialized, [&] {
        const auto & config = getConfigRef();
        shared->asynchronous_remote_fs_reader = createThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER, config);
        shared->asynchronous_local_fs_reader = createThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER, config);
        shared->synchronous_local_fs_reader = createThreadPoolReader(FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER, config);
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

#if USE_LIBURING
IOUringReader & Context::getIOUringReader() const
{
    callOnce(shared->io_uring_reader_initialized, [&] {
        shared->io_uring_reader = createIOUringReader();
    });

    return *shared->io_uring_reader;
}
#endif

ThreadPool & Context::getThreadPoolWriter() const
{
    callOnce(shared->threadpool_writer_initialized, [&] {
        const auto & config = getConfigRef();
        auto pool_size = config.getUInt(".threadpool_writer_pool_size", 100);
        auto queue_size = config.getUInt(".threadpool_writer_queue_size", 1000000);

        shared->threadpool_writer = std::make_unique<ThreadPool>(
            CurrentMetrics::IOWriterThreads, CurrentMetrics::IOWriterThreadsActive, pool_size, pool_size, queue_size);
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

    res.enable_filesystem_read_prefetches_log = settings.enable_filesystem_read_prefetches_log;

    res.remote_fs_read_max_backoff_ms = settings.remote_fs_read_max_backoff_ms;
    res.remote_fs_read_backoff_max_tries = settings.remote_fs_read_backoff_max_tries;
    res.enable_filesystem_cache = settings.enable_filesystem_cache;
    res.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache;
    res.enable_filesystem_cache_log = settings.enable_filesystem_cache_log;

    res.filesystem_cache_max_download_size = settings.filesystem_cache_max_download_size;
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
    res.prefetch_buffer_size = settings.prefetch_buffer_size;
    res.direct_io_threshold = settings.min_bytes_to_use_direct_io;
    res.mmap_threshold = settings.min_bytes_to_use_mmap_io;
    res.priority = Priority{settings.read_priority};

    res.remote_throttler = getRemoteReadThrottler();
    res.local_throttler = getLocalReadThrottler();

    res.http_max_tries = settings.http_max_tries;
    res.http_retry_initial_backoff_ms = settings.http_retry_initial_backoff_ms;
    res.http_retry_max_backoff_ms = settings.http_retry_max_backoff_ms;
    res.http_skip_not_found_url_for_globs = settings.http_skip_not_found_url_for_globs;

    res.mmap_cache = getMMappedFileCache().get();

    return res;
}

ReadSettings Context::getBackupReadSettings() const
{
    ReadSettings settings = getReadSettings();
    settings.remote_throttler = getBackupsThrottler();
    settings.local_throttler = getBackupsThrottler();
    return settings;
}

WriteSettings Context::getWriteSettings() const
{
    WriteSettings res;

    res.enable_filesystem_cache_on_write_operations = settings.enable_filesystem_cache_on_write_operations;
    res.enable_filesystem_cache_log = settings.enable_filesystem_cache_log;
    res.s3_allow_parallel_part_upload = settings.s3_allow_parallel_part_upload;
    res.throw_on_error_from_cache = settings.throw_on_error_from_cache_on_write_operations;

    res.remote_throttler = getRemoteWriteThrottler();
    res.local_throttler = getLocalWriteThrottler();

    return res;
}

Context::ParallelReplicasMode Context::getParallelReplicasMode() const
{
    const auto & settings = getSettingsRef();

    using enum Context::ParallelReplicasMode;
    if (!settings.parallel_replicas_custom_key.value.empty())
        return CUSTOM_KEY;

    if (settings.allow_experimental_parallel_reading_from_replicas
        && !settings.use_hedged_requests)
        return READ_TASKS;

    return SAMPLE_KEY;
}

std::shared_ptr<AsyncReadCounters> Context::getAsyncReadCounters() const
{
    std::lock_guard lock(mutex);
    if (!async_read_counters)
        async_read_counters = std::make_shared<AsyncReadCounters>();
    return async_read_counters;
}

bool Context::canUseTaskBasedParallelReplicas() const
{
    /// TODO...
    return false;
}

bool Context::canUseParallelReplicasOnInitiator() const
{
    const auto & settings = getSettingsRef();
    return getParallelReplicasMode() == ParallelReplicasMode::READ_TASKS
        && settings.max_parallel_replicas > 1
        && !getClientInfo().collaborate_with_initiator;
}

bool Context::canUseParallelReplicasOnFollower() const
{
    const auto & settings = getSettingsRef();
    return getParallelReplicasMode() == ParallelReplicasMode::READ_TASKS
        && settings.max_parallel_replicas > 1
        && getClientInfo().collaborate_with_initiator;
}

bool Context::canUseParallelReplicasCustomKey() const
{
    const auto & settings_ref = getSettingsRef();

    const bool has_enough_servers = settings_ref.max_parallel_replicas > 1;
    const bool parallel_replicas_enabled = settings_ref.allow_experimental_parallel_reading_from_replicas > 0;
    const bool is_parallel_replicas_with_custom_key = false;
#if 0
    const bool is_parallel_replicas_with_custom_key =
        settings_ref.parallel_replicas_mode == ParallelReplicasMode::CUSTOM_KEY_SAMPLING ||
        settings_ref.parallel_replicas_mode == ParallelReplicasMode::CUSTOM_KEY_RANGE;
#endif

    return has_enough_servers && parallel_replicas_enabled && is_parallel_replicas_with_custom_key;
}

bool Context::canUseParallelReplicasCustomKeyForCluster(const Cluster & cluster) const
{
    return canUseParallelReplicasCustomKey() && cluster.getShardCount() == 1 && cluster.getShardsInfo()[0].getAllNodeCount() > 1;
}

bool Context::canUseOffsetParallelReplicas() const
{
    const auto & settings_ref = getSettingsRef();

    /**
     * Offset parallel replicas algorithm is not only the one which relies on native SAMPLING KEY,
     * but also those which rely on customer-provided "custom" key.
     * We combine them together into one group for convenience.
     */
    const bool has_enough_servers = settings_ref.max_parallel_replicas > 1;
    const bool parallel_replicas_enabled = settings_ref.allow_experimental_parallel_reading_from_replicas > 0;
    const bool is_parallel_replicas_with_custom_key_or_native_sampling_key = false;
    const bool offset_parallel_replicas_enabled = false;

#if 0
    const bool is_parallel_replicas_with_custom_key_or_native_sampling_key =
        settings_ref[Setting::parallel_replicas_mode] == ParallelReplicasMode::SAMPLING_KEY ||
        settings_ref[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_SAMPLING ||
        settings_ref[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_RANGE;
#endif
    return offset_parallel_replicas_enabled &&
        has_enough_servers &&
        parallel_replicas_enabled &&
        is_parallel_replicas_with_custom_key_or_native_sampling_key;
}

void Context::disableOffsetParallelReplicas()
{
    offset_parallel_replicas_enabled = false;
}

void Context::setPreparedSetsCache(const PreparedSetsCachePtr & cache)
{
    prepared_sets_cache = cache;
}

PreparedSetsCachePtr Context::getPreparedSetsCache() const
{
    return prepared_sets_cache;
}

UInt64 Context::getClientProtocolVersion() const
{
    return client_protocol_version;
}

void Context::setClientProtocolVersion(UInt64 version)
{
    client_protocol_version = version;
}

const ServerSettings & Context::getServerSettings() const
{
    return shared->server_settings;
}

/// proton: starts
String Context::getPasswordForCurrentUser() const
{
    return client_info.current_user_password;
}

void Context::setUserByName(const String & user_name)
{
    if (auto id = getAccessControl()->find<User>(user_name))
        setUser(*id);
    else
        throw Exception(ErrorCodes::UNKNOWN_USER, "User {} doesn't exist", user_name);
}

ServerDescriptorPtr Context::getServerDescriptor() const
{
    /// Return a shared_ptr to the global ServerDescriptor
    /// Since it's a global that lives for the entire program lifetime,
    /// we can use a no-op deleter
    return ServerDescriptorPtr(&Globals::getServerDescriptor(), [](ServerDescriptor *) { });
}

const String & Context::getHostFQDN() const noexcept
{
    return Globals::getServerDescriptor().hostname;
}

const String & Context::getClusterID() const noexcept
{
    return Globals::getServerDescriptor().cluster_id;
}

const DB::UUID & Context::getNodeUUID() const noexcept
{
    return Globals::getServerDescriptor().node_uuid;
}

void Context::setTotalMaterializedViews(UInt32 total_mvs) noexcept
{
    Globals::getServerDescriptor().total_materialized_views.store(total_mvs, std::memory_order_relaxed);
}

void Context::setTotalShards(UInt32 total_shards) noexcept
{
    Globals::getServerDescriptor().total_shards.store(total_shards, std::memory_order_relaxed);
}

void Context::setOSMemoryFreeMB(UInt32 os_memory_free_mb) noexcept
{
    Globals::getServerDescriptor().os_memory_free_mb.store(os_memory_free_mb, std::memory_order_relaxed);
}

void Context::setMemoryUsedMB(UInt32 memory_used_mb) noexcept
{
    Globals::getServerDescriptor().memory_used_mb.store(memory_used_mb, std::memory_order_relaxed);
}

void Context::setOSCPUUsage(double os_cpu_usage) noexcept
{
    Globals::getServerDescriptor().os_cpu_usage.store(os_cpu_usage, std::memory_order_relaxed);
}

void Context::setCPUUsage(double cpu_usage) noexcept
{
    Globals::getServerDescriptor().cpu_usage.store(cpu_usage, std::memory_order_relaxed);
}

void Context::updateDiskUsage(const String & name, UInt64 total_bytes, UInt64 available_bytes, bool) noexcept
{
    if (total_bytes > 0)
    {
        /// No lock needed - intentional design to avoid slowing the system
        /// The disk volumes are populated at startup and keys don't change
        ServerDescriptor::DiskStats stats;
        stats.total_mb = total_bytes / (1024 * 1024);
        stats.free_mb = available_bytes / (1024 * 1024);
        stats.util = 1.0 - (static_cast<double>(available_bytes) / total_bytes);

        Globals::getServerDescriptor().disk_utils[name] = stats;
    }
}

void Context::setNodeID(cluster::NodeID node_id_) noexcept
{
    shared->this_node_id = node_id_;
}

cluster::NodeID Context::getNodeID() const noexcept
{
    return shared->this_node_id;
}

String Context::getSpillDirForCurrentQuery(const String & postfix) const
{
    /// To isolate multiple joins or aggregations in the same query
    chassert(spill_id_for_current_query);
    auto spill_id = spill_id_for_current_query->fetch_add(1);
    auto spill_dir = fs::path(getSpillDirPath()) / fmt::format("{}-{}_{}", getCurrentQueryId(), spill_id, postfix);

    /// Always start with empty \spill_dir to avoid data conflicts. This ensures:
    /// 1) For a new query: The \spill_dir is empty
    /// 2) For a recovered query: We clean up any existing data and restore from checkpoint
    if (fs::exists(spill_dir))
    {
        LOG_INFO(shared->log, "Cleaning up spill directory {}", spill_dir);
        fs::remove_all(spill_dir);
    }

    return spill_dir;
}

void Context::setConfigPath(const String & config_path_)
{
    shared->config_path = config_path_;
}

const String & Context::getConfigPath() const noexcept
{
    return shared->config_path;
}

void Context::setSpillDirPath(const String & spill_dir_path_)
{
    shared->spill_dir_path = spill_dir_path_;
}

const String & Context::getSpillDirPath() const noexcept
{
    return shared->spill_dir_path;
}

void Context::setMaxDiskUtil(double max_disk_util)
{
    shared->max_disk_util = max_disk_util;
}

void Context::checkDiskUtil() const
{
    shared->checkDiskUtil();
}

/// Check if an ingest / insert is allowed
/// Disk utilization check
void Context::checkIngest(const String & /*database_name*/) const
{
    checkDiskUtil();
}

ThreadPool & Context::getAdhocSchedulePool() const
{
    callOnce(shared->global_adhoc_schedule_pool_initialized, [&] {
        shared->global_adhoc_schedule_pool = std::make_unique<ThreadPool>(
            CurrentMetrics::LocalThread,
            CurrentMetrics::LocalThreadActive,
            settings.adhoc_pool_size,
            settings.adhoc_pool_size,
            std::max<size_t>(settings.adhoc_pool_size, 100),
            /*shutdown_on_exception=*/false);
    });
    return *shared->global_adhoc_schedule_pool;
}

ThreadPool & Context::getStorageCommitPool() const
{
    callOnce(shared->storage_commit_pool_initialized, [&] {
        /// FIXME, queue size may matter
        shared->storage_commit_pool = std::make_unique<ThreadPool>(
            CurrentMetrics::StorageCommitThreads,
            CurrentMetrics::StorageCommitThreadsActive,
            settings.storage_commit_pool_size,
            settings.storage_commit_pool_size,
            settings.storage_commit_pool_size,
            /*shutdown_on_exception=*/false);
    });
    return *shared->storage_commit_pool;
}

ThreadPool & Context::getNlogAdhocSchedulePool() const
{
    callOnce(shared->nlog_adhoc_schedule_pool_initialized, [&] {
        shared->nlog_adhoc_schedule_pool = std::make_unique<ThreadPool>(
            CurrentMetrics::NLogAdhocThreads,
            CurrentMetrics::NLogAdhocThreadsActive,
            settings.nlog_adhoc_pool_size,
            settings.nlog_adhoc_pool_size,
            settings.nlog_adhoc_pool_size,
            /*shutdown_on_exception=*/false);
    });
    return *shared->nlog_adhoc_schedule_pool;
}

NLOG::BackgroundSchedulePool & Context::getNlogBackgroundSchedulePool() const
{
    callOnce(shared->nlog_schedule_pool_initialized, [&] {
        shared->nlog_schedule_pool = std::make_unique<NLOG::BackgroundSchedulePool>(
            settings.nlog_background_pool_size, CurrentMetrics::BackgroundSchedulePoolNativeLogTask, "NLogSched");
    });
    return *shared->nlog_schedule_pool;
}

cluster::TimerService & Context::getTimerService() const
{
    callOnce(shared->global_system_timer_initialized, [&] {
        shared->global_system_timer = std::make_unique<cluster::TimerService>(settings.timer_service_pool_size);
    });
    return *shared->global_system_timer;
}

Context::QueryAnalysisCache & Context::getQueryAnalysisCache() const
{
    chassert(hasQueryContext());
    return getQueryContext()->query_analysis_cache;
}

/// proton: ends.

}
