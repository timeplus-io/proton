#pragma once

#include <Cluster/Common/AckSemantic.h>

#include <string>

namespace DB
{
enum class IngestMode : uint8_t
{
    Sync,
    Async,
};

inline IngestMode ingestMode(const std::string & mode, IngestMode default_mode)
{
    if (mode == "sync")
        return IngestMode::Sync;
    else if (mode == "async")
        return IngestMode::Async;
    else
        return default_mode;
}

inline cluster::AckSemantic ingestModeToAckSemantic(IngestMode ingest_mode)
{
    switch (ingest_mode)
    {
        case IngestMode::Sync:
            return cluster::AckSemantic::QuorumCommitted;
        case IngestMode::Async:
            return cluster::AckSemantic::AfterProposal;
    }
}
}
