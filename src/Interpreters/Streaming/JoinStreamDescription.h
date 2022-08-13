#pragma once

#include <Core/Block.h>

namespace DB
{
enum class HashSemantic
{
    Append = 0,
    ChangeLogKV = 1,
    VersionedKV = 2,
};

struct JoinStreamDescription
{
    JoinStreamDescription(Block sample_block_, HashSemantic hash_semantic_, UInt64 keep_versions_)
        : sample_block(std::move(sample_block_)), hash_semantic(hash_semantic_), keep_versions(keep_versions_)
    {
    }

    JoinStreamDescription(JoinStreamDescription && other)
        : sample_block(std::move(other.sample_block)), hash_semantic(other.hash_semantic), keep_versions(other.keep_versions)
    {
    }

    Block sample_block;
    HashSemantic hash_semantic;
    UInt64 keep_versions;
};
}
