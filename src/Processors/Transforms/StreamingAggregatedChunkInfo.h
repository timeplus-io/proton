#pragma once

#include <Processors/Chunk.h>

namespace DB
{
/// Daisy : starts. Introduced by Daisy
struct StreamingAggregatedChunkInfo final: public ChunkInfo
{
    enum class Event
    {
        LATE_EVENT,
        HEART_BEAT,
    };

    StreamingAggregatedChunkInfo(Event event_) : event(event_) {}
    ~StreamingAggregatedChunkInfo() override {}

    Event event;
};
}
