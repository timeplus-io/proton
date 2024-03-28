#pragma once

#include <Interpreters/Streaming/HashJoin.h>

namespace DB
{
namespace Streaming
{
class BidirectionalRangeHashJoin final : public HashJoin
{
public:
    using HashJoin::HashJoin;
    HashJoinType type() const override { return HashJoinType::BidirectionalRange; }
};

}
}
