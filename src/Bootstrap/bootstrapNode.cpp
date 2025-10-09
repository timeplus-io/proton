#include <Bootstrap/ConfigSetting.h>

#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/NodeConfig.h>
#include <Cluster/StoreConfig.h>
#include <Interpreters/Context.h>
#include <base/getFileSystemSpace.h>
#include <Common/formatReadable.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <limits>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
extern const int UNKNOWN_COMPRESSION_METHOD;
}

namespace
{
DB::CompressionMethodByte codec(const std::string & method)
{
    if (method.empty() || method == "NONE" || method == "none")
        return DB::CompressionMethodByte::NONE;
    else if (method == "LZ4")
        return DB::CompressionMethodByte::LZ4;
    else if (method == "ZSTD")
        return DB::CompressionMethodByte::ZSTD;
    else if (method == "Multiple")
        return DB::CompressionMethodByte::Multiple;
    else if (method == "Delta")
        return DB::CompressionMethodByte::Delta;
    else if (method == "T64")
        return DB::CompressionMethodByte::T64;
    else if (method == "DoubleDelta")
        return DB::CompressionMethodByte::DoubleDelta;
    else if (method == "Gorilla")
        return DB::CompressionMethodByte::Gorilla;
    else if (method == "AES_128_GCM_SIV")
        return DB::CompressionMethodByte::AES_128_GCM_SIV;
    else if (method == "AES_256_GCM_SIV")
        return DB::CompressionMethodByte::AES_256_GCM_SIV;
    else
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_COMPRESSION_METHOD, "'{}' compression method is not supported", method);
}

} /// end anonymous namespace


cluster::StoreConfigPtr
parseStoreConfig(const Poco::Util::AbstractConfiguration & all_configs, const std::string & store_key_prefix, Poco::Logger * logger)
{
    if (!all_configs.has(store_key_prefix))
        return nullptr;

    /// Parse log data dirs settings
    std::vector<std::filesystem::path> log_data_dirs;
    Poco::Util::AbstractConfiguration::Keys keys;
    all_configs.keys(store_key_prefix, keys);

    /// 'keys' example: ["metadata_keep_versions", "data_dirs", "data_dirs[1]", "data_dirs[2]", "log"]
    for (const auto & key : keys)
    {
        if (!key.starts_with("data_dirs"))
            continue;

        const auto & path_str = all_configs.getString(fmt::format("{}.{}", store_key_prefix, key));
        std::filesystem::path filepath(path_str);
        std::filesystem::create_directories(filepath);
        log_data_dirs.push_back(std::filesystem::canonical(filepath));

        auto [total_space, free_space, available_space, used_space] = getFileSystemSpace(path_str);

        LOG_INFO(
            logger,
            "The path of {} is {}, The device where this directory is located has total space: {}, free space: {}, available space: {}, "
            "used space: {}.",
            store_key_prefix,
            path_str,
            formatReadableSizeWithBinarySuffix(total_space),
            formatReadableSizeWithBinarySuffix(free_space),
            formatReadableSizeWithBinarySuffix(available_space),
            formatReadableSizeWithBinarySuffix(used_space));
    }

    auto store_config = std::make_shared<cluster::StoreConfig>(std::move(log_data_dirs));

    /// MetaStore settings (metadata.metastore)
    {
        ConfigSettings config_settings = {
            {".metadata_keep_versions", store_config->metadata_keep_versions},
            {".metadata_max_retries", store_config->metadata_max_retries},
        };

        parseConfigSettings(config_settings, all_configs, store_key_prefix);

        if (store_config->metadata_max_retries == 0)
            store_config->metadata_max_retries = std::numeric_limits<uint64_t>::max();
    }

    /// MetaStore Log settings (metadata.metastore.log)
    {
        std::string compression_codec;

        ConfigSettings config_settings = {
            {".check_crcs", store_config->check_crcs},
            {".retention_check_ms", store_config->retention_check_ms},
            {".fetch_max_wait_ms", store_config->log_fetch_config->max_wait_ms},
            {".fetch_max_bytes", store_config->log_fetch_config->max_bytes},
            {".max_entry_size", store_config->log_config->max_entry_size},
            {".segment_size", store_config->log_config->segment_size},
            {".retention_size", store_config->log_config->retention_size},
            {".retention_ms", store_config->log_config->retention_ms},
            {".index_interval_bytes", store_config->log_config->index_interval_bytes},
            {".index_interval_entries", store_config->log_config->index_interval_entries},
            {".flush_interval_ms", store_config->log_config->flush_interval_ms},
            {".flush_interval_entries", store_config->log_config->flush_interval_entries},
            {".compression_codec", compression_codec},
            {".preallocate", store_config->log_config->preallocate},
            {".incremental_flush", store_config->log_config->incremental_flush},
            {".min_size_to_keep", store_config->log_config->min_size_to_keep},
            {".cache_max_cached_entries_per_shard", store_config->log_config->max_cached_entries_per_shard},
            {".cache_max_cached_bytes_per_shard", store_config->log_config->max_cached_bytes_per_shard},
            {".hard_state_ckpt_log_size", store_config->log_config->hard_state_ckpt_log_size},
            {".hard_state_ckpt_log_preallocate", store_config->log_config->hard_state_ckpt_log_preallocate},
            {".leader_epoch_index_log_size", store_config->log_config->leader_epoch_index_log_size},
            {".leader_epoch_index_log_preallocate", store_config->log_config->leader_epoch_index_log_preallocate},
            {".timestamp_index_log_size", store_config->log_config->timestamp_index_log_size},
            {".timestamp_index_log_preallocate", store_config->log_config->timestamp_index_log_preallocate},
            {".log_position_index_log_size", store_config->log_config->log_position_index_log_size},
            {".log_position_index_log_preallocate", store_config->log_config->log_position_index_log_preallocate},
        };

        parseConfigSettings(config_settings, all_configs, fmt::format("{}.log", store_key_prefix));

        store_config->log_config->codec = codec(compression_codec);
    }

    return store_config;
}

#if 0
void parseMetaDataConfigs(
    const Poco::Util::AbstractConfiguration & all_configs,
    const cluster::NodeDescriptorProviderPtr & node_provider,
    const cluster::NodeConfigPtr & node_config,
    Poco::Logger * logger)
{
    node_config->meta_store_config = parseStoreConfig(all_configs, METADATA_METASTORE_CONF_KEY_PREFIX, logger);
    if (!node_config->meta_store_config)
        /// Meta store is mandatory on all nodes
        node_config->meta_store_config = std::make_shared<cluster::StoreConfig>(std::vector{std::filesystem::path{DEFAULT_META_DIR}});
}

void parseDataConfigs(
    const Poco::Util::AbstractConfiguration & all_configs,
    const cluster::NodeDescriptorProviderPtr & node_provider,
    const cluster::NodeConfigPtr & node_config,
    Poco::Logger * logger)
{
    node_config->data_store_config = parseStoreConfig(all_configs, DATA_DATASTORE_CONF_KEY_PREFIX, logger);
    if (!node_config->data_store_config)
        node_config->data_store_config = std::make_shared<cluster::StoreConfig>(std::vector{std::filesystem::path{DEFAULT_DATA_DIR}});
}
#endif

#if 0
cluster::NodeConfigPtr
parseConfig(const ContextMutablePtr & global_context, cluster::NodeDescriptorProviderPtr node_provider, Poco::Logger * logger)
{
    assert(global_context);

    const auto & all_configs = global_context->getConfigRef();

    auto node_config = std::make_shared<cluster::NodeConfig>();
    parseMetaDataConfigs(all_configs, node_provider, node_config, logger);
    parseDataConfigs(all_configs, node_provider, node_config, logger);
    /// TestingConfig removed
    /// parseTestingConfigs(all_configs, node_config);

    return node_config;
}

void validateConfig(const cluster::NodeConfigPtr & node_config)
{
    /// Simplified validation for 
    assert(node_config->meta_store_config);
    node_config->meta_store_config->validate();

    if (node_config->data_store_config)
        node_config->data_store_config->validate();

    /// No server configs to validate in 
}
#endif

} /// namespace DB
