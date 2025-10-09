#include <Bootstrap/Globals.h>
#include <Bootstrap/ServerDescriptor.h>

#include <Checkpoint/CheckpointCoordinator.h>
#include <Cluster/Common/Stream.h>
#include <Cluster/Common/StreamIDShard.h>
#include <Cluster/Common/TimeWheel/SystemTimer.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Core/Types.h>
#include <Interpreters/Apply/MetadataUpdater.h>
#include <Interpreters/Context.h>
#include <Common/Config/ExternalGrokPatterns.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <fmt/format.h>


namespace DB
{
extern DB::Context * g_global_ctx;
extern DB::CheckpointCoordinator * g_state_ckpt_coordinator;
extern std::unique_ptr<cluster::meta::MetaStore> g_meta_store;
extern std::unique_ptr<cluster::NativeLog> g_native_log;
extern std::unique_ptr<DB::MetadataUpdater> g_metadata_updater;
extern DB::ExternalGrokPatterns * g_grok_patterns;
extern std::unique_ptr<cluster::SystemTimer> g_cleanup_timer;
extern std::unique_ptr<DB::ServerDescriptor> g_server_descriptor;

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
extern const int RESOURCE_NOT_FOUND;
}
}

namespace Globals
{
DB::Context & getGlobalContext()
{
    assert(DB::g_global_ctx);
    return *DB::g_global_ctx;
}

cluster::meta::MetaStore & getMetaStore()
{
    assert(DB::g_meta_store);
    return *DB::g_meta_store;
}

bool isMetaStoreInited()
{
    return DB::g_meta_store != nullptr;
}

DB::CheckpointCoordinator & getCheckpointCoordinator()
{
    assert(DB::g_state_ckpt_coordinator);
    return *DB::g_state_ckpt_coordinator;
}

cluster::NativeLog & getNativeLog()
{
    assert(DB::g_native_log);
    return *DB::g_native_log;
}

bool isNativeLogInited()
{
    return DB::g_native_log != nullptr;
}

DB::MetadataUpdater & getMetadataUpdater()
{
    assert(DB::g_metadata_updater);
    return *DB::g_metadata_updater;
}

bool isMetadataUpdaterInited()
{
    return DB::g_metadata_updater != nullptr;
}

DB::ExternalGrokPatterns & getGrokPatterns()
{
    assert(DB::g_grok_patterns);
    return *DB::g_grok_patterns;
}

DB::ServerDescriptor & getServerDescriptor()
{
    assert(DB::g_server_descriptor);
    return *DB::g_server_descriptor;
}

uint32_t getNodeID()
{
    /// Always returns node ID 1
    return 1;
}

}
