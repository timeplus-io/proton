#pragma once

#include <Cluster/Common/Constants.h>
#include <Cluster/Common/FetchHint.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/SerdeTag.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::nlog
{
SERDE struct LogSequenceMetadata
{
    /// No epoch tracking needed in  mode
    int64_t entry_sn = 0;
    std::optional<int64_t> segment_base_sn;
    /// Physical position in segment file
    std::optional<uint64_t> file_position;

    LogSequenceMetadata() = default;

    /// constructor
    LogSequenceMetadata(
        int64_t entry_sn_, std::optional<int64_t> segment_base_sn_ = {}, std::optional<uint64_t> file_position_ = {})
        : entry_sn(entry_sn_), segment_base_sn(std::move(segment_base_sn_)), file_position(std::move(file_position_))
    {
    }

    /// Legacy constructor for compatibility (epoch parameter ignored)
    LogSequenceMetadata(
        uint64_t /*epoch_*/, int64_t entry_sn_, std::optional<int64_t> segment_base_sn_ = {}, std::optional<uint64_t> file_position_ = {})
        : entry_sn(entry_sn_), segment_base_sn(std::move(segment_base_sn_)), file_position(std::move(file_position_))
    {
    }

    void serialize(DB::WriteBuffer & wb) const;

    void deserialize(DB::ReadBuffer & rb);

    std::string string() const;

    bool isValid() const { return entry_sn >= Constants::LogStartSN || segment_base_sn || file_position; }

    bool operator==(const LogSequenceMetadata & rhs) const
    {
        return isValid() && rhs.isValid() && entry_sn == rhs.entry_sn && file_position == rhs.file_position
            && segment_base_sn == rhs.segment_base_sn;
    }

    FetchHint toFetchHint() const noexcept
    {
        FetchHint fetch_hint;
        fetch_hint.sn = entry_sn;

        if (segment_base_sn)
            fetch_hint.segment_base_sn = segment_base_sn.value();

        if (file_position)
            fetch_hint.file_position = file_position.value();

        return fetch_hint;
    }
};
}
