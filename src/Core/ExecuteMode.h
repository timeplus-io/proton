#pragma once

#include <Core/SettingsFields.h>

namespace DB
{
enum class ExecuteMode : uint8_t
{
    Normal = 0,
    Subscribe = 1, /// Enable checkpoint
    Recover = 2,   /// Recover from checkpoint
};

DECLARE_SETTING_ENUM(ExecuteMode)
}
