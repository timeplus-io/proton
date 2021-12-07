#pragma once

#include <string>

namespace DB
{
enum class IngestMode
{
    INVALID,
    None,
    SYNC,
    ASYNC,
    FIRE_AND_FORGET,
    ORDERED,
};

inline IngestMode toIngestMode(const std::string & mode)
{
    if (mode == "async")
            return IngestMode::ASYNC;
    else if (mode == "sync")
        return IngestMode::SYNC;
    else if (mode == "fire_and_forget")
        return IngestMode::FIRE_AND_FORGET;
    else if (mode == "ordered")
        return IngestMode::ORDERED;
    else if (mode.empty())
        return IngestMode::None;
    else
        return IngestMode::INVALID;
}
}
