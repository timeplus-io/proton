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
/// - sn to physical file position mapping index
/// - physical file position to sn mapping index
/// - event time to sn mapping index
/// - append time to sn mapping index
class Indexes final : private boost::noncopyable
{
public:
    Indexes(fs::path index_dir_, int64_t base_sn_, Poco::Logger * logger_);
    ~Indexes();

    std::optional<TimestampSequence> lastIndexedAppendTimeSequence() const;

    std::optional<TimestampSequence> lastIndexedEventTimeSequence() const;

    std::optional<SequencePosition> lastIndexedSequencePosition() const;

    std::optional<PositionSequence> lastIndexedPositionSequence() const;

    /// Find the largest sn less than or equal to the given sn
    /// @param sn The sn to lookup
    /// @return The sn found and the corresponding position for this sn.
    ///         If the target sn is smaller than the least entry in the index (or the index is empty)
    ///         SequencePosition(base_sn, 0) is returned
    SequencePosition lowerBoundPositionForSequence(int64_t sn) const;

    /// Find the largest sn which has position great or equal to the given position
    /// @param position The physical position to lookup
    /// @return The position found. If the given position pass the end of segment, {} will be returned
    std::optional<PositionSequence> upperBoundSequenceForPosition(int64_t position) const;

    /// Find the largest sn less than or equal to the given sn
    /// @param ts The timestamp to lookup
    /// @param append_time ts is append time if true otherwise event time
    /// @return The sn found and the corresponding timestamp for this ts.
    std::optional<TimestampSequence> lowerBoundSequenceForTimestamp(int64_t ts, bool append_time) const;

    /// Index the logical sn to physical sn mapping, append timestamp to logical sn mapping and
    /// event timestamp to logical sn mapping in one go atomically
    void index(
        int64_t largest_sn,
        int64_t physical_position,
        const TimestampSequence & max_etimestamp_sn,
        const TimestampSequence & max_atimestamp_sn);

    /// Index append timestamp to logical sn mapping and
    /// event timestamp to logical sn mapping in one go atomically to index
    void index(const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn);

    /// Flush in-memory sn mappings to persistent store to make it crash consistent
    void sync();

    void close();

    const fs::path & indexDir() const { return index_dir; }

    void renameTo(fs::path new_index_dir, std::error_code & err)
    {
        close();
        fs::rename(index_dir, new_index_dir, err);

        if (!err)
            index_dir.swap(new_index_dir);
    }

    void updateParentDir(const fs::path & parent_dir)
    {
        /// We are assuming `index_dir` doesn't end with `/`
        index_dir = parent_dir / index_dir.filename();
    }

private:
    inline std::optional<IndexEntry> lastIndexedEntry(rocksdb::ColumnFamilyHandle * cf_handle) const;
    inline std::optional<IndexEntry> lowerBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const;
    inline std::optional<IndexEntry> upperBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const;
    inline void index(const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn, rocksdb::WriteBatch & batch);

private:
    std::unique_ptr<rocksdb::DB> indexes;
    rocksdb::ColumnFamilyHandle * sn_position_cf_handle;
    rocksdb::ColumnFamilyHandle * position_sn_cf_handle;
    rocksdb::ColumnFamilyHandle * etime_sn_cf_handle;
    rocksdb::ColumnFamilyHandle * atime_sn_cf_handle;

    fs::path index_dir;
    int64_t base_sn;

    std::optional<TimestampSequence> last_indexed_etimestamp;
    std::optional<TimestampSequence> last_indexed_atimestamp;

    std::atomic_flag closed;

    Poco::Logger * logger;
};
}
