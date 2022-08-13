#pragma once

#include <boost/functional/hash.hpp>

#include <functional>
#include <unordered_set>

namespace DB
{
class Block;

struct JoinTuple
{
    uint64_t left_block_id;
    const Block * right_block;
    uint32_t row_num_in_left_block;
    uint32_t row_num_in_right_block;

    bool operator==(const JoinTuple & rhs) const
    {
        return left_block_id == rhs.left_block_id && row_num_in_left_block == rhs.row_num_in_left_block && right_block == rhs.right_block
            && row_num_in_right_block == rhs.row_num_in_right_block;
    }
};

using JoinTupleMap = std::unordered_set<DB::JoinTuple>;

}

template <>
struct std::hash<DB::JoinTuple>
{
    std::size_t operator()(const DB::JoinTuple & join_tuple) const noexcept
    {
        size_t v = 0;
        boost::hash_combine(v, std::hash<uint64_t>{}(join_tuple.left_block_id));
        boost::hash_combine(v, std::hash<uint32_t>{}(join_tuple.row_num_in_left_block));
        boost::hash_combine(v, std::hash<const DB::Block *>{}(join_tuple.right_block));
        boost::hash_combine(v, std::hash<uint32_t>{}(join_tuple.row_num_in_right_block));
        return v;
    }
};
