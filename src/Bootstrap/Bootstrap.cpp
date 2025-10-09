#include <Bootstrap/Bootstrap.h>
#include <Bootstrap/ServerDescriptor.h>
#include <Bootstrap/bootstrapCheckpointCoordinator.h>
#include <Bootstrap/bootstrapNode.h>

#include <Checkpoint/CheckpointConfig.h>
#include <Checkpoint/CheckpointCoordinator.h>
/// proton: starts - NodeDescriptor no longer needed
#include <Cluster/Common/StreamIDShard.h>
#include <Cluster/Common/TimeWheel/SystemTimer.h>
#include <Cluster/MetaStore/LocalMetaQueue.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/MetaStore/ProposalRegistry.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Core/ServerMeta.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Apply/MetadataUpdater.h>
#include <Interpreters/Context.h>
#include <Common/Config/ExternalGrokPatterns.h>
#include <Common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <filesystem>

namespace DB
{
Context * g_global_ctx = nullptr;
ExternalGrokPatterns * g_grok_patterns = nullptr;
CheckpointCoordinator * g_state_ckpt_coordinator = nullptr;

/// Global timer for periodic tasks
std::unique_ptr<cluster::SystemTimer> g_cleanup_timer;

/// Global MetaStore and MetadataUpdater
std::unique_ptr<cluster::meta::MetaStore> g_meta_store;
std::unique_ptr<DB::MetadataUpdater> g_metadata_updater;
std::unique_ptr<cluster::NativeLog> g_native_log;

std::unique_ptr<ServerDescriptor> g_server_descriptor;

namespace ErrorCodes
{
extern const int SYSTEM_ERROR;
extern const int LOGICAL_ERROR;
}

/// Bootstrap Sequence (10 steps as per optionB.md)
void doBootstrap(ContextMutablePtr global_context, Poco::Logger * logger)
{
    LOG_INFO(logger, "Starting bootstrap");

    auto data_path = global_context->getPath();
    const auto & config_ref = global_context->getConfigRef();

    /// Step 1: Create required directories
    LOG_DEBUG(logger, "Creating required directories");

    /// Create all necessary directories with secure permissions (0750 = owner all, group read/exec)
    /// NOTE: std::filesystem::create_directories doesn't support permissions on all platforms,
    /// so we create then chmod
    auto create_secure_directory = [logger](const std::filesystem::path & dir) {
        std::filesystem::create_directories(dir);
        std::filesystem::permissions(
            dir,
            std::filesystem::perms::owner_all | std::filesystem::perms::group_read | std::filesystem::perms::group_exec,
            std::filesystem::perm_options::replace);
        LOG_DEBUG(logger, "Created directory with secure permissions: {}", dir.string());
    };

    create_secure_directory(data_path + "tables");
    create_secure_directory(data_path + "tmp");
    create_secure_directory(data_path + "checkpoint");
    create_secure_directory(data_path + "nativelog");
    create_secure_directory(data_path + "server_logs");

    /// Setup query states spill dir
    auto spill_dir = config_ref.getString("query_state_spill_path", data_path + "query_states_spilled");
    global_context->setSpillDirPath(spill_dir);
    create_secure_directory(spill_dir);

    LOG_DEBUG(logger, "Directories created at {}", data_path);

    /// Step 2: Parse metadata.metastore config FIRST to get the correct path
    LOG_DEBUG(logger, "Parsing metadata.metastore configuration");

    /// Parse metadata.metastore config to get the actual metastore path
    auto meta_store_config = parseStoreConfig(config_ref, "metadata.metastore", logger);
    std::filesystem::path metadata_dir;
    size_t metadata_keep_versions;

    if (!meta_store_config)
    {
        /// Create default config if not specified in config.yaml
        metadata_dir = data_path + "metastore";
        metadata_keep_versions = config_ref.getUInt64("metadata.metastore.metadata_keep_versions", 10);
        meta_store_config = std::make_shared<cluster::StoreConfig>(std::vector<std::filesystem::path>{metadata_dir});
        meta_store_config->metadata_keep_versions = metadata_keep_versions;
        LOG_DEBUG(logger, "Using default metadata.metastore config at {}", metadata_dir);
    }
    else
    {
        /// Use the configured path from metadata.metastore.data_dirs[0]
        if (meta_store_config->data_dirs.empty())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "metadata.metastore.data_dirs cannot be empty");
        }
        metadata_dir = meta_store_config->data_dirs.front();
        metadata_keep_versions = meta_store_config->metadata_keep_versions;
        LOG_DEBUG(logger, "Using configured metadata.metastore at {}", metadata_dir);
    }

    /// Ensure the metadata directory exists with secure permissions
    create_secure_directory(metadata_dir);

    /// Open MetaDB at the correct path
    cluster::StreamIDShard stream_shard(cluster::StreamID{0}, 0); /// Fixed stream/shard
    auto meta_db = std::make_shared<cluster::meta::MetaDB>(metadata_dir, stream_shard, metadata_keep_versions, getLogger("MetaDB"));

    LOG_DEBUG(logger, "MetaDB opened at {}", metadata_dir);

    /// Step 3: Get last applied sequence number
    LOG_DEBUG(logger, "Reading last applied sequence");

    auto last_applied_result = meta_db->loadAppliedSequence();
    uint64_t last_applied_sn = 0;
    if (!last_applied_result.hasError())
    {
        last_applied_sn = last_applied_result.result.sn;
        LOG_DEBUG(logger, "Last applied SN = {}", last_applied_sn);
    }
    else
    {
        LOG_DEBUG(logger, "No previous applied SN found, starting from 0");
    }

    /// Step 4: Create ProposalRegistry
    LOG_DEBUG(logger, "Creating ProposalRegistry");

    auto proposal_registry = std::make_shared<cluster::meta::ProposalRegistry>(getLogger("ProposalRegistry"));

    LOG_DEBUG(logger, "ProposalRegistry created");

    /// Step 5: Create LocalMetaQueue and recover pending requests
    LOG_DEBUG(logger, "Creating LocalMetaQueue");

    /// Read LocalMetaQueue configuration from config
    size_t max_queue_entries = config_ref.getUInt64("local_meta_queue.max_queue_entries", 10000);
    size_t max_queue_bytes = config_ref.getUInt64("local_meta_queue.max_queue_bytes", 100 * 1024 * 1024); /// Default 100MB

    LOG_DEBUG(logger, "LocalMetaQueue config: max_entries={}, max_bytes={}", max_queue_entries, max_queue_bytes);

    auto local_meta_queue
        = std::make_shared<cluster::meta::LocalMetaQueue>(meta_db, getLogger("LocalMetaQueue"), max_queue_entries, max_queue_bytes);

    /// Recover pending requests from persistent storage
    auto recover_err = local_meta_queue->recoverFromPersistent(last_applied_sn);
    if (recover_err.hasError())
    {
        throw Exception(recover_err.error_code, "Failed to recover LocalMetaQueue: {}", recover_err.error_message);
    }

    LOG_DEBUG(logger, "LocalMetaQueue recovered from SN {}", last_applied_sn);

    /// Step 6: Initialize MetaStore
    LOG_DEBUG(logger, "Initializing MetaStore");

    /// meta_store_config was already parsed in Step 2 to ensure consistency

    g_server_descriptor = std::make_unique<ServerDescriptor>();
    g_server_descriptor->loadFromConfig(config_ref);
    LOG_DEBUG(logger, "ServerDescriptor initialized with config");

    g_meta_store = std::make_unique<cluster::meta::MetaStore>(meta_store_config, meta_db);

    /// Set local components
    g_meta_store->setLocalMetaLog(local_meta_queue);
    g_meta_store->setProposalRegistry(proposal_registry);

    /// proton: starts - Set ServerDescriptor
    auto server_desc_ptr = std::shared_ptr<ServerDescriptor>(g_server_descriptor.get(), [](ServerDescriptor *) { });
    g_meta_store->setServerDescriptor(server_desc_ptr);

    /// Also set it in the Cluster
    auto & cluster = const_cast<cluster::Cluster &>(g_meta_store->getCluster());
    cluster.setServerDescriptor(server_desc_ptr);

    /// Start MetaStore
    g_meta_store->startup();

    LOG_DEBUG(logger, "MetaStore initialized");

    /// Step 7: Register and start CheckpointCoordinator (no lease)
    LOG_DEBUG(logger, "Starting CheckpointCoordinator");

    /// Use config-based initialization instead of hardcoding
    g_state_ckpt_coordinator = &bootstrapStateCheckpointCoordinator(global_context);
    g_state_ckpt_coordinator->startup();

    LOG_DEBUG(logger, "CheckpointCoordinator started with config-based settings");

    /// Step 8: Initialize NativeLog (MUST be before MetadataUpdater)
    LOG_DEBUG(logger, "Initializing NativeLog");

    /// Parse data.datastore config for NativeLog
    auto data_store_config = parseStoreConfig(config_ref, "data.datastore", logger);
    if (!data_store_config)
    {
        /// Create default config if not specified in config.yaml
        auto nativelog_dir = data_path + "nativelog";
        data_store_config = std::make_shared<cluster::StoreConfig>(std::vector<std::filesystem::path>{nativelog_dir});
        LOG_DEBUG(logger, "Using default data.datastore config");
    }
    else
    {
        LOG_DEBUG(logger, "Loaded data.datastore config from config.yaml");
    }

    /// Create NativeLog with the configuration from config.yaml
    /// but it will only use local log manager
    auto & scheduler = global_context->getNlogBackgroundSchedulePool();
    auto & adhoc_scheduler = global_context->getNlogAdhocSchedulePool();
    g_native_log = std::make_unique<cluster::NativeLog>(data_store_config, *g_meta_store, scheduler, adhoc_scheduler);

    /// Start NativeLog
    g_native_log->startup();

    LOG_DEBUG(logger, "NativeLog initialized and started");

    /// Step 9: Start MetadataUpdater (AFTER NativeLog is ready)
    LOG_DEBUG(logger, "Starting MetadataUpdater");

    g_metadata_updater = std::make_unique<DB::MetadataUpdater>(global_context);
    g_metadata_updater->startup();

    LOG_DEBUG(logger, "MetadataUpdater started");

    /// Step 10: Add periodic cleanup timer
    LOG_DEBUG(logger, "Setting up periodic cleanup timer");

    /// Read cleanup timer configuration from config
    size_t timer_thread_pool_size = config_ref.getUInt64("cleanup_timer.thread_pool_size", 1);
    uint64_t timer_tick_interval_ms = config_ref.getUInt64("cleanup_timer.tick_interval_ms", 60000);
    uint64_t cleanup_interval_ms = config_ref.getUInt64("cleanup_timer.cleanup_interval_ms", 60000);

    LOG_DEBUG(
        logger,
        "Cleanup timer config: thread_pool_size={}, tick_interval_ms={}, cleanup_interval_ms={}",
        timer_thread_pool_size,
        timer_tick_interval_ms,
        cleanup_interval_ms);

    g_cleanup_timer = std::make_unique<cluster::SystemTimer>(timer_thread_pool_size, timer_tick_interval_ms);

    /// Schedule periodic cleanup of old pending requests
    auto cleanup_task = g_cleanup_timer->add(
        cleanup_interval_ms,
        [meta_db, logger]() {
            try
            {
                auto current = meta_db->loadAppliedSequence();
                if (!current.hasError())
                {
                    /// Only cleanup requests that were truly applied
                    /// Delete all requests with SN <= applied_sn (they've been processed)
                    uint64_t cleanup_up_to = current.result.sn;

                    auto cleanup_err = meta_db->cleanupOldPendingRequests(cleanup_up_to);
                    if (cleanup_err.hasError())
                    {
                        LOG_WARNING(logger, "Failed to cleanup old requests: {}", cleanup_err.error_message);
                    }
                    else
                    {
                        LOG_DEBUG(logger, "Cleaned up applied pending requests up to SN {}", cleanup_up_to);
                    }
                }
            }
            catch (...)
            {
                tryLogCurrentException(logger, "Error during periodic cleanup");
            }
        },
        true /// repeat
    );

    if (!cleanup_task)
    {
        LOG_WARNING(logger, "Failed to schedule periodic cleanup timer");
    }

    LOG_DEBUG(logger, "Periodic cleanup scheduled");

    /// Set global context and other singletons
    g_global_ctx = global_context.get();
    global_context->setMaxDiskUtil(config_ref.getDouble("max_local_disk_usage_ratio", 0.9));

    /// Load server metadata
    std::filesystem::path path = global_context->getPath();
    ServerMeta::load(path / "server.meta", logger);

    /// Initialize remaining global singletons
    DataTypeFactory::instance();
    g_grok_patterns = &ExternalGrokPatterns::instance(global_context);

    /// Always use node ID 1
    global_context->setNodeID(1);

    LOG_INFO(logger, "Bootstrap completed successfully");
}

void teardown(LoggerPtr logger)
{
    LOG_DEBUG(logger, "Tearing down globals");

    /// 1. Stop cleanup timer first (no dependencies)
    if (g_cleanup_timer)
    {
        /// Timer shutdown happens automatically in destructor
        g_cleanup_timer.reset();
    }

    /// 2. Stop MetadataUpdater (depends on MetaStore)
    /// Must stop BEFORE NativeLog since it polls from MetaStore
    if (g_metadata_updater)
    {
        g_metadata_updater->shutdown();
        g_metadata_updater.reset();
    }

    /// 3. Stop NativeLog (depends on MetaStore)
    if (g_native_log)
    {
        g_native_log->shutdown();
        g_native_log.reset();
    }

    /// Stop CheckpointCoordinator
    if (g_state_ckpt_coordinator)
    {
        g_state_ckpt_coordinator->shutdown();
        g_state_ckpt_coordinator = nullptr;
    }

    /// Stop MetaStore
    if (g_meta_store)
    {
        g_meta_store->shutdown();
        g_meta_store.reset();
    }

    /// Cleanup grok patterns
    if (g_grok_patterns)
    {
        g_grok_patterns->shutdown();
        g_grok_patterns = nullptr;
    }

    g_global_ctx = nullptr;

    LOG_DEBUG(logger, "Teardown complete");
}

void bootstrap(ContextMutablePtr global_context, Poco::Logger * logger)
{
    try
    {
        doBootstrap(std::move(global_context), logger);
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to bootstrap");
        throw;
    }
}

}
