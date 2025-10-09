#pragma once

namespace DB::Streaming
{
enum class SelectExecuteMode : uint8_t
{
    Normal = 0,
    Subscribe = 1, /// Enable checkpoint
    Recover = 2,   /// Recover from checkpoint
};
}
