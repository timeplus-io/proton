#pragma once

#include <Cluster/Common/EpochSequence.h>

#include <memory>

namespace rocksdb
{
class Snapshot;
}

namespace cluster
{

struct KeyValueSnapshot
{
    KeyValueSnapshot() = default;

    KeyValueSnapshot(std::shared_ptr<const rocksdb::Snapshot> snapshot_, EpochSequence applied_sn_)
        : snapshot(std::move(snapshot_)), applied_sn(applied_sn_)
    {
    }

    std::shared_ptr<const rocksdb::Snapshot> snapshot;
    EpochSequence applied_sn;
};

}
