#pragma once

#include "FileRecords.h"
#include "LogSequenceMetadata.h"

namespace nlog
{
enum class FetchIsolation : uint8_t
{
    FETCH_LOG_END,
    FETCH_HIGH_WATERMARK,
    FETCH_TXN_COMMITTED,
};

struct FetchDataDescription
{
    LogSequenceMetadata fetch_offset_metadata{LogSequenceMetadata::UNKNOWN_SEQUENCE};
    FileRecordsPtr records;

    /// records can be empty if clients asking for latest offset
    bool isValid() const { return fetch_offset_metadata.isValid(); }
};
}
