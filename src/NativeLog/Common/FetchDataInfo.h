#pragma once

#include "FileRecords.h"
#include "LogOffsetMetadata.h"

namespace nlog
{
enum class FetchIsolation : uint8_t
{
    FETCH_LOG_END,
    FETCH_HIGH_WATERMARK,
    FETCH_TXN_COMMITTED,
};

struct FetchDataInfo
{
    LogOffsetMetadata fetch_offset_metadata{LogOffsetMetadata::UNKNOWN_OFFSET};
    FileRecordsPtr records;

    /// records can be empty if clients asking for latest offset
    bool isValid() const { return fetch_offset_metadata.isValid(); }
};
}
