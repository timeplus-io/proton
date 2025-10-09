#pragma once

#include <Cluster/Common/Common.h>
#include <Cluster/Common/Stream.h>
#include <Cluster/Common/StreamShard.h>

#include <Cluster/Common/CallResult.h>
#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/ShardReplica.h>

#include <Compression/CompressionInfo.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{
struct StreamDescriptor
{
public:
    StreamDescriptor() = default;

    StreamDescriptor(const StreamDescriptor &) = default;
    StreamDescriptor & operator=(const StreamDescriptor &) = default;
    StreamDescriptor(StreamDescriptor && other) noexcept = default;
    StreamDescriptor & operator=(StreamDescriptor && other) noexcept = default;

    StreamDescriptor(
        std::string && ns_,
        std::string && stream_,
        const StreamID & stream_id_,
        uint32_t shards_,
        std::string && sql_def_,
        uint32_t data_version_)
        : data_version(data_version_)
        , stream(std::move(ns_), std::move(stream_), stream_id_)
        , shards(shards_)
        , sql_def(std::move(sql_def_))
    {
    }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    size_t approximateSerializedSize() const noexcept;

    std::vector<StreamShard> localStreamShards(NodeID /*this_node*/) const
    {
        /// return all shards (they're all local)
        std::vector<StreamShard> result;
        for (uint32_t i = 0; i < shards; ++i)
            result.push_back(StreamShard{stream, i});
        return result;
    }

    bool operator==(const StreamDescriptor & rhs) const;

    std::optional<ShardReplica> shardReplica(const StreamIDShard & stream_shard, NodeID /*node_id*/) const noexcept
    {
        /// return shard only (no replica field)
        return ShardReplica{stream_shard.shard};
    }

    bool hasLocalShardReplica(NodeID /*node_id*/) const noexcept { return true; } /// Always true

    std::string string() const;

    bool isValid() const noexcept { return data_version > 0 && stream.isValid() && shards > 0 && !sql_def.empty(); }

    std::optional<uint32_t> prevShard(uint32_t shard) { return shard > 0 ? std::optional<uint32_t>(shard - 1) : std::nullopt; }
    std::optional<uint32_t> nextShard(uint32_t shard) { return shard < shards - 1 ? std::optional<uint32_t>(shard + 1) : std::nullopt; }
    bool isFirstShard(uint32_t shard) const noexcept { return shard == 0; }
    uint32_t firstShard() const noexcept { return 0; }
    uint32_t lastShard() const noexcept { return shards > 0 ? shards - 1 : 0; }

    StreamDescriptor & withStartCommit() noexcept
    {
        flags |= COMMIT_PENDING;
        return *this;
    }

    StreamDescriptor & withCommit() noexcept
    {
        flags = 0;
        return *this;
    }


    bool isStartCommit() const noexcept { return flags & COMMIT_PENDING; }
    bool isCommitted() const noexcept { return flags == 0; }

public:
    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    /// flags for internal use to mimic transaction
    /// bits : 0-55, for application code encoding
    /// bits : 56-63 for type
    /// flags for internal use like mimic transaction
    uint64_t flags = 0x0;

    constexpr static uint64_t COMMIT_PENDING = 0x1;

    Stream stream;

    uint32_t shards = 0;

    bool inmemory = false;

    DB::CompressionMethodByte codec = DB::CompressionMethodByte::NONE;

    /// Flush settings: per interval or per messages whichever reaches first
    uint32_t flush_ms = 0;
    uint32_t flush_messages = 0;
    uint32_t flush_bytes = 0;

    /// Retention settings for `delete` policy, per interval or per data volume whichever reaches first
    uint64_t retention_ms = 0; /// Data old than this interval will be deleted
    uint64_t retention_bytes = 0; /// When data volume reaches this threshold, old data will be deleted

    int64_t create_timestamp_ms;
    int64_t last_modify_timestamp_ms;

    std::string created_by;
    std::string last_modified_by;

    /// SQL definition
    std::string sql_def;
};

using StreamDescriptorPtr = std::shared_ptr<StreamDescriptor>;
using StreamDescriptorPtrs = std::vector<StreamDescriptorPtr>;
using StreamDescriptors = std::vector<StreamDescriptor>;
}
