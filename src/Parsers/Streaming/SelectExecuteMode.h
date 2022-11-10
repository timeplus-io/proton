#pragma once

namespace DB::Streaming
{
enum class SelectExecuteMode : uint8_t
{
    NORMAL = 0,
    SUBSCRIBE = 1, /// Enable checkpoint
    RECOVER = 2,   /// Recover from checkpoint
    UNSUBSCRIBE = 3, /// Remove checkpoint
};
}
