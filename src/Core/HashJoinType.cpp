#include <Core/HashJoinType.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTING_ENUM(
    HashJoinType,
    ErrorCodes::BAD_ARGUMENTS,
    {{"memory", HashJoinType::Memory},
     {"hybrid", HashJoinType::Hybrid}})
}
