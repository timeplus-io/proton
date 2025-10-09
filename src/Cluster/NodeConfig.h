#pragma once

#include <Cluster/StoreConfig.h>

namespace cluster
{
/// Simplified NodeConfig
struct NodeConfig
{
    /// Meta store configuration
    StoreConfigPtr meta_store_config;

    /// Data store configuration
    StoreConfigPtr data_store_config;

    void validate()
    {
        if (meta_store_config)
            meta_store_config->validate();
        if (data_store_config)
            data_store_config->validate();
    }
};

using NodeConfigPtr = std::shared_ptr<NodeConfig>;
}
