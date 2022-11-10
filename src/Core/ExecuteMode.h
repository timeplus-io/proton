#pragma once

#include <Core/SettingsFields.h>

namespace DB
{
enum class ExecuteMode : uint8_t
{
    NORMAL = 0,
    SUBSCRIBE = 1, /// Enable checkpoint
    RECOVER = 2,   /// Recover from checkpoint
    UNSUBSCRIBE = 3, /// Remove checkpoint
};

DECLARE_SETTING_ENUM(ExecuteMode)
}
