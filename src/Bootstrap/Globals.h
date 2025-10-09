#pragma once

#include <memory>
#include <cstdint>

namespace cluster
{
namespace meta
{
class MetaStore;
}
class NativeLog;
}

namespace DB
{
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class CheckpointCoordinator;
class MetadataUpdater;
class ExternalGrokPatterns;
struct ServerDescriptor;
}

namespace Globals
{
/// Get the global context
DB::Context & getGlobalContext();

cluster::meta::MetaStore & getMetaStore();


bool isMetaStoreInited();

DB::CheckpointCoordinator & getCheckpointCoordinator();

cluster::NativeLog & getNativeLog();


bool isNativeLogInited();


DB::MetadataUpdater & getMetadataUpdater();


bool isMetadataUpdaterInited();


DB::ExternalGrokPatterns & getGrokPatterns();

DB::ServerDescriptor & getServerDescriptor();

/// Get node ID (always returns 1)
uint32_t getNodeID();
}
