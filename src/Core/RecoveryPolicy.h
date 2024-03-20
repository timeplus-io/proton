#pragma once

#include <Core/SettingsFields.h>

namespace DB
{
enum class RecoveryPolicy : uint8_t
{
    Strict = 0,      /// Always recover from checkpointed
    BestEffort = 1,     /// Attempt to recover from checkpointed and allow skipping of some data with permanent errors
};

DECLARE_SETTING_ENUM(RecoveryPolicy)
}
