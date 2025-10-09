#pragma once

#include <memory>
#include <Cluster/StoreConfig.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace Poco
{
class Logger;
}

namespace DB
{

/// proton: starts - Config parsing for performance settings
/// Parse store configuration from config.yaml for metadata.metastore or data.datastore
cluster::StoreConfigPtr
parseStoreConfig(const Poco::Util::AbstractConfiguration & all_configs, const std::string & store_key_prefix, Poco::Logger * logger);
}
