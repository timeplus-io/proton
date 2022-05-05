#pragma once

#include "FileRecords.h"
#include "LogSequenceMetadata.h"

namespace nlog
{
enum class FetchIsolation : uint8_t
{
    FETCH_LOG_END,
    /// FETCH_HIGH_WATERMARK,
    FETCH_COMMITTED,
};

struct FetchDataDescription
{
    LogSequenceMetadata fetch_sn_metadata{LogSequenceMetadata::UNKNOWN_SEQUENCE};
    FileRecordsPtr records;

    /// Tell client the fetch reaches the end of the current segment
    bool eof = false;

    /// records can be empty if clients asking for latest offset
    bool isValid() const { return fetch_sn_metadata.isValid(); }
};
}
