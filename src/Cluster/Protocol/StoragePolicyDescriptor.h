#pragma once

#include <Cluster/Common/Common.h>

#include <limits>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{
struct VolumeDescriptor
{
    enum class VolumeType : uint8_t
    {
        Unknown = 0,
        JBOD = 1,
        SingleDisk = 2,
    };

    enum class VolumeLoadBalancing : uint8_t
    {
        Unknown = 0,
        RoundRobin = 1,
        LeastUsed = 2,
    };

    std::string name;
    VolumeType type = VolumeType::JBOD;

    uint64_t max_data_part_size = 0;
    double max_data_part_size_ratio = 0;

    /// Should a new data part be synchronously moved to a volume according to ttl on insert
    /// or move this part in background task asynchronously after insert.
    bool perform_ttl_move_on_insert = true;

    /// Load balancing, one of:
    /// - ROUND_ROBIN
    /// - LEAST_USED
    VolumeLoadBalancing load_balancing = VolumeLoadBalancing::Unknown;

    /// Ordering of this volume in the policy. Lower value = higher priority. Must be unique
    /// across the policy and (if any are set) cover 1..N. UINT64_MAX means "no explicit priority".
    /// Default 0 is also treated as "unset" for backwards compatibility with pre-v2 metastore
    /// data that was written before this field was wired through.
    uint64_t volume_priority = std::numeric_limits<uint64_t>::max();

    std::vector<std::string> disk_names;

    /// Added in schema v2: when true, the background merger will not select parts on this
    /// volume as merge candidates (VolumeJBOD::areMergesAvoided returns true). Typical use:
    /// cold/external tiers where merges cause expensive object-store I/O.
    bool prefer_not_to_merge = false;

    /// Added in schema v2: refresh interval in milliseconds for the LEAST_USED disk-size cache.
    /// 0 disables caching (every getDisk/reserve call recomputes ranks). Default 60000 (60s)
    /// matches upstream.
    uint64_t least_used_ttl_ms = 60'000;

    size_t approximateSerializedSize() const noexcept;

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    std::string string() const;
};

struct StoragePolicyDescriptor
{
    size_t approximateSerializedSize() const noexcept;

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    std::string string() const;

    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    /// v2: appended VolumeDescriptor.prefer_not_to_merge and least_used_ttl_ms to the volume
    ///     serde tail; widened volume_priority from uint16_t to uint64_t (wire-compatible
    ///     for all values the previous code could write).
    constexpr static uint32_t schema_version = 2;

    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    std::string name;

    /// move_factor from interval [0., 1.]
    /// We move something if disk from this policy
    /// filled more than total_size * move_factor
    double move_factor = 0.1;

    std::vector<VolumeDescriptor> volumes;

    int64_t create_timestamp_ms;
    int64_t last_modify_timestamp_ms;

    std::string created_by;
    std::string last_modified_by;
};

using StoragePolicyDescriptorPtr = std::shared_ptr<StoragePolicyDescriptor>;
using StoragePolicyDescriptorPtrs = std::vector<StoragePolicyDescriptorPtr>;
}
