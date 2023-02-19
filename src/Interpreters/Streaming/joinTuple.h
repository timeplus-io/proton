#pragma once

#include <boost/functional/hash.hpp>

#include <functional>
#include <unordered_set>

namespace DB
{
class Block;

namespace Streaming
{
struct JoinTuple
{
    uint64_t src_block_id;
    const Block * target_block;
    uint32_t row_num_in_src_block;
    uint32_t row_num_in_target_block;

    bool operator==(const JoinTuple & rhs) const
    {
        return src_block_id == rhs.src_block_id && row_num_in_src_block == rhs.row_num_in_src_block && target_block == rhs.target_block
            && row_num_in_target_block == rhs.row_num_in_target_block;
    }
};

using JoinTupleMap = std::unordered_set<DB::Streaming::JoinTuple>;
}
}

template <>
struct std::hash<DB::Streaming::JoinTuple>
{
    std::size_t operator()(const DB::Streaming::JoinTuple & join_tuple) const noexcept
    {
        size_t v = 0;
        boost::hash_combine(v, std::hash<uint64_t>{}(join_tuple.src_block_id));
        boost::hash_combine(v, std::hash<uint32_t>{}(join_tuple.row_num_in_src_block));
        boost::hash_combine(v, std::hash<const DB::Block *>{}(join_tuple.target_block));
        boost::hash_combine(v, std::hash<uint32_t>{}(join_tuple.row_num_in_target_block));
        return v;
    }
};
