#pragma once

#include <Cluster/Common/Common.h>

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

    /// reserved for future
    uint16_t volume_priority = 0;

    std::vector<std::string> disk_names;

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
    constexpr static uint32_t schema_version = 1;

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
