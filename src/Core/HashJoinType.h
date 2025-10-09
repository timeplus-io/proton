#pragma once

#include <Core/SettingsFields.h>

namespace DB
{
enum class HashJoinType : uint8_t
{
    Memory = 0,
    Hybrid = 1,
};

DECLARE_SETTING_ENUM(HashJoinType)
}
