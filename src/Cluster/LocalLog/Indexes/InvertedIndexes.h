#pragma once

#include <Cluster/LocalLog/Indexes/InvertedIndexEntry.h>

#include <Common/SharedMutex.h>

#include <rocksdb/db.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace Poco
{
class Logger;
}
using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace cluster::nlog
{
/// A `InvertedIndexes` contain
/// - sn to physical file position mapping index
/// - physical file position to sn mapping index
/// - event time to sn mapping index
/// - append time to sn mapping index
/// Not thread-safe
class InvertedIndexes final
{
    using SequencePosition = InvertedIndexEntry;
    using PositionSequence = InvertedIndexEntry;
    using TimestampSequence = InvertedIndexEntry;

public:
    InvertedIndexes(fs::path index_dir_, LoggerPtr logger_);

    ~InvertedIndexes();

    TimestampSequence lastIndexedAppendTimeSequenceCached() const noexcept { return last_indexed_atimestamp; }

    TimestampSequence lastIndexedEventTimeSequenceCached() const noexcept { return last_indexed_etimestamp; }

    std::optional<TimestampSequence> lastIndexedAppendTimeSequence() const;

    std::optional<TimestampSequence> lastIndexedEventTimeSequence() const;

    std::optional<PositionSequence> lastIndexedPositionSequence() const;

    /// Find the earliest entry whose sequence is less or equal to the sn passed in
    /// \param sn The sn to lookup
    /// \return The sn found and the corresponding position for this sn.
    ///         If the target sn is smaller than the least entry in the index (or the index is empty)
    ///         {} is returned
    std::optional<SequencePosition> lowerBoundPositionForSequence(int64_t sn) const;

    /// Find the largest sn which has position great or equal to the given position
    /// \param position The physical position to lookup
    /// \return The position found. If the given position pass the end of segment, {} will be returned
    std::optional<PositionSequence> upperBoundSequenceForPosition(int64_t position) const;

    /// Find the earliest entry whose timestamp is less than or equal to (<=) the given sn
    /// \param ts The timestamp to lookup
    /// \param append_time ts is append time if true otherwise event time
    /// \return The sn found and the corresponding timestamp for this ts.
    std::optional<TimestampSequence> lowerBoundSequenceForTimestamp(int64_t ts, bool append_time) const;

    /// Index the logical sn to physical sn mapping, append timestamp to logical sn mapping and
    /// event timestamp to logical sn mapping in one go atomically
    void
    index(int64_t sn, int64_t physical_position, const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn);

    /// Index append timestamp to logical sn mapping and
    /// event timestamp to logical sn mapping in one go atomically to index
    void index(const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn);

    /// Flush in-memory sn mappings to persistent store to make it crash consistent
    void sync();

    void close();

    /// Remove all inverted index entries which have sequence number in range [sn, ...)
    void trim(int64_t sn);

    /// Remove all inverted index entries which have sequence number in range [..., sn)
    void trimHead(int64_t sn);

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
    void loadMaxTimestamps();

    inline std::optional<InvertedIndexEntry> lastIndexedEntry(rocksdb::ColumnFamilyHandle * cf_handle) const;
    inline std::optional<InvertedIndexEntry> lowerBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const;
    inline std::optional<InvertedIndexEntry> upperBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const;

    inline void
    index(const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn, rocksdb::WriteBatch & batch);

    inline void indexWithLockHeld(
        const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn, rocksdb::WriteBatch & batch);

    inline void trimWithLockHeld(
        int64_t sn,
        rocksdb::ColumnFamilyHandle * sn_cf_handle,
        rocksdb::ColumnFamilyHandle * non_sn_cf_handle,
        rocksdb::WriteBatch & delete_batch);

    inline void trimHeadWithLockHeld(
        int64_t sn,
        rocksdb::ColumnFamilyHandle * sn_cf_handle,
        rocksdb::ColumnFamilyHandle * non_sn_cf_handle,
        rocksdb::WriteBatch & delete_batch);

private:
    mutable DB::SharedMutex rw_mu;

    std::unique_ptr<rocksdb::DB> indexes;
    /// sequence <-> file position
    rocksdb::ColumnFamilyHandle * sn_position_cf_handle;
    rocksdb::ColumnFamilyHandle * position_sn_cf_handle;

    /// sequence <-> etime
    rocksdb::ColumnFamilyHandle * sn_etime_cf_handle;
    rocksdb::ColumnFamilyHandle * etime_sn_cf_handle;

    /// sequence <-> atime
    rocksdb::ColumnFamilyHandle * sn_atime_cf_handle;
    rocksdb::ColumnFamilyHandle * atime_sn_cf_handle;

    fs::path index_dir;

    TimestampSequence last_indexed_etimestamp;
    TimestampSequence last_indexed_atimestamp;

    bool closed = false;

    LoggerPtr logger;
};

using InvertedIndexesPtr = std::shared_ptr<InvertedIndexes>;
}
