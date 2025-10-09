#include <Core/HashTableType.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTING_ENUM(
    HashTableType,
    ErrorCodes::BAD_ARGUMENTS,
    {{"memory", HashTableType::Memory},
     {"hybrid", HashTableType::Hybrid}})
}
