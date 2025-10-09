#pragma once

#include <Core/SettingsFields.h>

namespace DB
{
enum class HashTableType : uint8_t
{
    Memory = 0,
    Hybrid = 1,
};

DECLARE_SETTING_ENUM(HashTableType)
}
