#pragma once

#include "IndexEntry.h"

#include <NativeLog/Base/Stds.h>

#include <boost/noncopyable.hpp>
#include <rocksdb/db.h>

namespace Poco
{
class Logger;
}

namespace nlog
{
/// `indexes` contain
/// - offset to physical file position mapping index
/// - physical file position to offset mapping index
/// - event time to offset mapping index
/// - append time to offset mapping index
class Indexes final : private boost::noncopyable
{
public:
    Indexes(const fs::path & index_dir, int64_t base_offset, Poco::Logger * logger_);
    ~Indexes();

    TimestampOffset lastIndexedAppendTimeOffset();

    TimestampOffset lastIndexedEventTimeOffset();

    OffsetPosition lastIndexedOffsetPosition();

    PositionOffset lastIndexedPositionOffset();

    /// Find the largest offset less than or equal to the given offset
    /// @param offset The offset to lookup
    /// @return The offset found and the corresponding position for this offset.
    /// If the target offset is smaller than the least entry in the index (or the index is empty)
    /// OffsetPotion(base_offset, 0) is returned
    OffsetPosition lowerBoundOffsetPosition(int64_t offset);

    /// Index the logical offset to physical offset mapping, append timestamp to logical offset mapping and
    /// event timestamp to logical offset mapping in one go atomically
    void index(
        int64_t largest_offset,
        int64_t physical_position,
        const TimestampOffset & max_etimestamp_offset,
        const TimestampOffset & max_atimestamp_offset);

    /// Index append timestamp to logical offset mapping and
    /// event timestamp to logical offset mapping in one go atomically to index
    void index(const TimestampOffset & max_etimestamp_offset, const TimestampOffset & max_atimestamp_offset);

    /// Flush in-memory offset mappings to persistent store to make it crash consistent
    void sync();

private:
    IndexEntry lastIndexedEntry(rocksdb::ColumnFamilyHandle * cf_handle) const;
    IndexEntry lowerBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const;
    void index(const TimestampOffset & max_etimestamp_offset, const TimestampOffset & max_atimestamp_offset, rocksdb::WriteBatch & batch);

private:
    std::unique_ptr<rocksdb::DB> indexes;
    rocksdb::ColumnFamilyHandle * offset_position_cf_handle;
    rocksdb::ColumnFamilyHandle * position_offset_cf_handle;
    rocksdb::ColumnFamilyHandle * etime_offset_cf_handle;
    rocksdb::ColumnFamilyHandle * atime_offset_cf_handle;

    int64_t base_offset;

    TimestampOffset last_indexed_etimestamp;
    TimestampOffset last_indexed_atimestamp;

    Poco::Logger * logger;
};
}
