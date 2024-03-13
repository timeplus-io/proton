#pragma once

#include <Core/SettingsFields.h>

namespace DB
{
enum class RecoveryPolicy : uint8_t
{
    Strict = 0,      /// Always recover from checkpointed
    // (TODO) Cautious = 1,   /// Attempt to recover from checkpointed and allow skipping of specific data that would cause a permanent error
    Loose = 2,     /// Attempt to recover from checkpointed and allow skipping of some data with permanent errors
};

DECLARE_SETTING_ENUM(RecoveryPolicy)
}
