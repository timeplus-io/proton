#pragma once

#include <Core/Block.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
namespace Streaming
{
enum class DataStreamSemantic
{
    Append = 0,
    ChangelogKV = 1,
    VersionedKV = 2,
    Changelog = 3,
};

inline bool isKeyValueDataStreamSemantic(DataStreamSemantic data_stream_semantic)
{
    return data_stream_semantic == Streaming::DataStreamSemantic::VersionedKV || data_stream_semantic == Streaming::DataStreamSemantic::ChangelogKV;
}

DataStreamSemantic getDataStreamSemantic(StoragePtr storage);

struct JoinStreamDescription
{
    JoinStreamDescription(
        Block sample_block_,
        DataStreamSemantic data_stream_semantic_,
        UInt64 keep_versions_)
        : sample_block(std::move(sample_block_))
        , data_stream_semantic(data_stream_semantic_)
        , keep_versions(keep_versions_)
    {
    }

    JoinStreamDescription(
        Block sample_block_,
        DataStreamSemantic data_stream_semantic_,
        UInt64 keep_versions_,
        std::optional<std::vector<size_t>> && primary_key_columns_)
        : sample_block(std::move(sample_block_))
        , data_stream_semantic(data_stream_semantic_)
        , keep_versions(keep_versions_)
        , primary_key_columns(std::move(primary_key_columns_))
    {
    }

    JoinStreamDescription(JoinStreamDescription && other)
        : sample_block(std::move(other.sample_block)), data_stream_semantic(other.data_stream_semantic), keep_versions(other.keep_versions), primary_key_columns(std::move(other.primary_key_columns))
    {
    }

    Block sample_block;
    DataStreamSemantic data_stream_semantic;
    UInt64 keep_versions;
    std::optional<std::vector<size_t>> primary_key_columns;
};

using JoinStreamDescriptionPtr = std::shared_ptr<JoinStreamDescription>;
}
}
