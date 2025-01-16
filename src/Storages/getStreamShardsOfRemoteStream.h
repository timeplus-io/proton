#pragma once

#include <Interpreters/Cluster.h>


namespace DB
{

struct StorageID;

/// Get the number of shards the remote stream has (i.e. the value of the `shards` setting of the CREATE query).
UInt32 getStreamShardsOfRemoteStream(
    const Cluster & cluster,
    const StorageID & table_id,
    ContextPtr context);

}
