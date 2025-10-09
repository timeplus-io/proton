#include <Cluster/LocalLog/Indexes/InvertedIndexes.h>
#include <Cluster/Rocks/IntegerComparator.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
const extern int CANNOT_OPEN_DATABASE;
const extern int DELETE_KEY_VALUE_FAILED;
}
}

namespace cluster::nlog
{
InvertedIndexes::InvertedIndexes(fs::path index_dir_, LoggerPtr logger_) : index_dir(std::move(index_dir_)), logger(logger_)
{
    /// We are expecting `index_dir` doesn't end with `/`
    assert(index_dir.has_filename());

    /// FIXME, lazy init index db since if there are lots of segments, init may take a long time
    rocksdb::Options options;
    /// Since index entry is append only, we don't need compaction, keys are always unique and monotonically increasing
    options.num_levels = 1;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.atomic_flush = true;
    options.comparator = integerComparator<int64_t>();

    rocksdb::ColumnFamilyOptions cf_options;
    cf_options.comparator = integerComparator<int64_t>();

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.reserve(6);

    /// default cf : sn to physical position mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, cf_options));
    /// position_sn cf : physical position to sn mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("position_sn", cf_options));

    /// etime_sn cf : event time to sn mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("sn_etime", cf_options));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("etime_sn", cf_options));

    /// atime_sn cf : append time to sn mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("sn_atime", cf_options));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("atime_sn", cf_options));

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    cf_handles.reserve(6);

    rocksdb::DB * db;
    /// Opening rocksdb can be very expensive: around 1 second
    auto status = rocksdb::DB::Open(options, index_dir, column_families, &cf_handles, &db);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_DATABASE, "Filed to open inverted index db, {}", status.ToString());

    indexes.reset(db);
    sn_position_cf_handle = cf_handles[0];
    position_sn_cf_handle = cf_handles[1];

    sn_etime_cf_handle = cf_handles[2];
    etime_sn_cf_handle = cf_handles[3];

    sn_atime_cf_handle = cf_handles[4];
    atime_sn_cf_handle = cf_handles[5];

    loadMaxTimestamps();
}

void InvertedIndexes::loadMaxTimestamps()
{
    if (auto last_etimestamp = lastIndexedEventTimeSequence(); last_etimestamp)
        last_indexed_etimestamp = *last_etimestamp;

    if (auto last_atimestamp = lastIndexedAppendTimeSequence(); last_atimestamp)
        last_indexed_atimestamp = *last_atimestamp;
}

InvertedIndexes::~InvertedIndexes()
{
    close();
}

void InvertedIndexes::close()
{
    {
        std::shared_lock rlock{rw_mu};

        if (closed)
            /// Already closed
            return;

        closed = true;
    }

    if (!indexes)
        return;

    sync();

    {
        std::unique_lock wlock{rw_mu};

        for (auto & cf_handle :
             {std::ref(sn_position_cf_handle),
              std::ref(position_sn_cf_handle),
              std::ref(sn_etime_cf_handle),
              std::ref(etime_sn_cf_handle),
              std::ref(sn_atime_cf_handle),
              std::ref(atime_sn_cf_handle)})
        {
            if (cf_handle.get() != nullptr)
            {
                auto status = indexes->DestroyColumnFamilyHandle(cf_handle.get());
                if (!status.ok())
                    LOG_ERROR(logger, "Failed to destroy column family handle");

                cf_handle.get() = nullptr;
            }
        }

        auto status = indexes->Close();
        if (!status.ok())
            LOG_ERROR(logger, "Failed to close inverted indexes db");

        indexes.reset();
    }

    LOG_INFO(logger, "Closed inverted indexes successfully");
}

std::optional<InvertedIndexes::TimestampSequence> InvertedIndexes::lastIndexedAppendTimeSequence() const
{
    return lastIndexedEntry(atime_sn_cf_handle);
}

std::optional<InvertedIndexes::TimestampSequence> InvertedIndexes::lastIndexedEventTimeSequence() const
{
    return lastIndexedEntry(etime_sn_cf_handle);
}

std::optional<InvertedIndexes::PositionSequence> InvertedIndexes::lastIndexedPositionSequence() const
{
    return lastIndexedEntry(position_sn_cf_handle);
}

std::optional<InvertedIndexEntry> InvertedIndexes::lastIndexedEntry(rocksdb::ColumnFamilyHandle * cf_handle) const
{
    std::shared_lock rlock{rw_mu};
    if (closed)
        return {};

    std::unique_ptr<rocksdb::Iterator> iter{indexes->NewIterator(rocksdb::ReadOptions{}, cf_handle)};
    iter->SeekToLast();
    if (iter->Valid())
    {
        assert(iter->key().size() == sizeof(int64_t));
        auto key = *reinterpret_cast<const int64_t *>(iter->key().data());
        assert(iter->value().size() == sizeof(int64_t));
        auto value = *reinterpret_cast<const int64_t *>(iter->value().data());

        return InvertedIndexEntry{key, value};
    }
    return {};
}

std::optional<InvertedIndexes::SequencePosition> InvertedIndexes::lowerBoundPositionForSequence(int64_t sn) const
{
    auto entry = lowerBound(sn, sn_position_cf_handle);
    if (entry)
        return entry;

    return {};
}

std::optional<InvertedIndexes::TimestampSequence> InvertedIndexes::lowerBoundSequenceForTimestamp(int64_t ts, bool append_time) const
{
    if (append_time)
        return lowerBound(ts, atime_sn_cf_handle);
    else
        return lowerBound(ts, etime_sn_cf_handle);
}

/// IndexEntry.key <= key
/// \return IndexEntry which has key less or equal to key specified in the parameter if found such entry
/// Otherwise return {}
std::optional<InvertedIndexEntry> InvertedIndexes::lowerBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const
{
    std::shared_lock rlock{rw_mu};
    if (closed)
        return {};

    std::unique_ptr<rocksdb::Iterator> iter{indexes->NewIterator(rocksdb::ReadOptions{}, cf_handle)};
    rocksdb::Slice key_s{reinterpret_cast<const char *>(&key), sizeof(key)};
    iter->SeekForPrev(key_s);
    if (iter->Valid())
    {
        assert(iter->key().size() == sizeof(int64_t));
        auto k = *reinterpret_cast<const int64_t *>(iter->key().data());
        assert(iter->value().size() == sizeof(int64_t));
        auto v = *reinterpret_cast<const int64_t *>(iter->value().data());
        assert(k <= key);
        return InvertedIndexEntry{k, v};
    }
    return {};
}

std::optional<InvertedIndexes::PositionSequence> InvertedIndexes::upperBoundSequenceForPosition(int64_t position) const
{
    return upperBound(position, position_sn_cf_handle);
}

/// IndexEntry.key >= key
/// \return IndexEntry which has key great or equal to key specified in the parameter if found such entry
/// Otherwise return {}
std::optional<InvertedIndexEntry> InvertedIndexes::upperBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const
{
    std::shared_lock rlock{rw_mu};
    if (closed)
        return {};

    std::unique_ptr<rocksdb::Iterator> iter{indexes->NewIterator(rocksdb::ReadOptions{}, cf_handle)};
    rocksdb::Slice key_s{reinterpret_cast<const char *>(&key), sizeof(key)};
    iter->Seek(key_s);
    if (iter->Valid())
    {
        assert(iter->key().size() == sizeof(int64_t));
        auto k = *reinterpret_cast<const int64_t *>(iter->key().data());
        assert(iter->value().size() == sizeof(int64_t));
        auto v = *reinterpret_cast<const int64_t *>(iter->value().data());
        assert(k >= key);
        return InvertedIndexEntry{k, v};
    }
    return {};
}

void InvertedIndexes::index(
    int64_t sn, int64_t physical_position, const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn)
{
    rocksdb::Slice sn_s{reinterpret_cast<const char *>(&sn), sizeof(sn)};
    rocksdb::Slice position_s{reinterpret_cast<const char *>(&physical_position), sizeof(physical_position)};

    rocksdb::WriteBatch batch;

    std::shared_lock rlock{rw_mu};
    if (closed)
        return;

    /// Insert sn -> physical position mapping
    batch.Put(sn_position_cf_handle, sn_s, position_s);

    /// Insert physical position -> sn mapping
    batch.Put(position_sn_cf_handle, position_s, sn_s);

    indexWithLockHeld(max_etimestamp_sn, max_atimestamp_sn, batch);
}

void InvertedIndexes::index(const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn)
{
    rocksdb::WriteBatch batch;

    std::shared_lock rlock{rw_mu};
    if (closed)
        return;

    indexWithLockHeld(max_etimestamp_sn, max_atimestamp_sn, batch);
}

void InvertedIndexes::index(
    const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn, rocksdb::WriteBatch & batch)
{
    std::shared_lock rlock{rw_mu};
    if (closed)
        return;

    indexWithLockHeld(max_etimestamp_sn, max_atimestamp_sn, batch);
}

void InvertedIndexes::indexWithLockHeld(
    const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn, rocksdb::WriteBatch & batch)
{
    if (max_etimestamp_sn > last_indexed_etimestamp)
    {
        rocksdb::Slice event_time_s{reinterpret_cast<const char *>(&max_etimestamp_sn.key), sizeof(max_etimestamp_sn.key)};
        rocksdb::Slice event_sn_s{reinterpret_cast<const char *>(&max_etimestamp_sn.value), sizeof(max_etimestamp_sn.value)};
        /// sn -> event timestamp
        batch.Put(sn_etime_cf_handle, event_sn_s, event_time_s);
        /// event timestamp -> sn
        batch.Put(etime_sn_cf_handle, event_time_s, event_sn_s);
    }

    if (max_atimestamp_sn > last_indexed_atimestamp)
    {
        rocksdb::Slice append_time_s{reinterpret_cast<const char *>(&max_atimestamp_sn.key), sizeof(max_atimestamp_sn.key)};
        rocksdb::Slice event_sn_s{reinterpret_cast<const char *>(&max_atimestamp_sn.value), sizeof(max_atimestamp_sn.value)};
        /// sn -> append timestamp
        batch.Put(sn_atime_cf_handle, event_sn_s, append_time_s);
        /// append timestamp -> sn
        batch.Put(atime_sn_cf_handle, append_time_s, event_sn_s);
    }

    if (!batch.HasPut())
        return;

    rocksdb::WriteOptions write_options;
    /// write_options.disableWAL = true;

    auto status = indexes->Write(write_options, &batch);
    if (!status.ok())
        /// We just log the error since data is already appended to the log,
        /// we can just ignore this reverted index entry for this case
        LOG_ERROR(logger, "Failed to update indexes, error={}", status.ToString());

    /// sync();

    if (max_etimestamp_sn > last_indexed_etimestamp)
        last_indexed_etimestamp = max_etimestamp_sn;

    if (max_atimestamp_sn > last_indexed_atimestamp)
        last_indexed_atimestamp = max_atimestamp_sn;
}

void InvertedIndexes::sync()
{
    std::shared_lock rlock{rw_mu};
    if (closed)
        return;

    auto status = indexes->FlushWAL(true);
    if (!status.ok())
        LOG_ERROR(logger, "Failed to flush indexes, {}", status.ToString());
}

/// trim [sn, ...)
void InvertedIndexes::trim(int64_t sn)
{
    rocksdb::WriteBatch delete_batch;
    {
        std::shared_lock rlock{rw_mu};
        if (closed)
            return;

        /// sequence <-> file position
        trimWithLockHeld(sn, sn_position_cf_handle, position_sn_cf_handle, delete_batch);

        /// sequence <-> etime
        trimWithLockHeld(sn, sn_etime_cf_handle, etime_sn_cf_handle, delete_batch);

        /// sequence <-> atime
        trimWithLockHeld(sn, sn_atime_cf_handle, atime_sn_cf_handle, delete_batch);
    }

    if (!delete_batch.HasDeleteRange())
        return;

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    {
        std::shared_lock rlock{rw_mu};
        if (closed)
            return;

        auto status = indexes->Write(write_options, &delete_batch);
        if (!status.ok())
        {
            LOG_ERROR(logger, "Failed to trim indexes to sn={} error={}", sn, status.ToString());
            throw DB::Exception(
                DB::ErrorCodes::DELETE_KEY_VALUE_FAILED,
                "Failed to trim indexes to sn={} error={}",
                sn,
                status.ToString());
        }
    }

    /// Max timestamps may be changed because of the deletion, reload them
    loadMaxTimestamps();
}

/// trimHead [..., sn)
void InvertedIndexes::trimHead(int64_t sn)
{
    rocksdb::WriteBatch delete_batch;
    {
        std::shared_lock rlock{rw_mu};
        if (closed)
            return;

        /// sequence <-> file position
        trimHeadWithLockHeld(sn, sn_position_cf_handle, position_sn_cf_handle, delete_batch);

        /// sequence <-> etime
        trimHeadWithLockHeld(sn, sn_etime_cf_handle, etime_sn_cf_handle, delete_batch);

        /// sequence <-> atime
        trimHeadWithLockHeld(sn, sn_atime_cf_handle, atime_sn_cf_handle, delete_batch);
    }

    if (!delete_batch.HasDeleteRange())
        return;

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    {
        std::shared_lock rlock{rw_mu};
        if (closed)
            return;

        auto status = indexes->Write(write_options, &delete_batch);
        if (!status.ok())
        {
            LOG_ERROR(logger, "Failed to trim indexes to sn={} error={}", sn, status.ToString());
            throw DB::Exception(
                DB::ErrorCodes::DELETE_KEY_VALUE_FAILED,
                "Failed to trim indexes to sn={} error={}",
                sn,
                status.ToString());
        }
    }

    /// Max timestamps may be changed because of the deletion, reload them
    loadMaxTimestamps();
}

void InvertedIndexes::trimWithLockHeld(
    int64_t sn,
    rocksdb::ColumnFamilyHandle * sn_cf_handle,
    rocksdb::ColumnFamilyHandle * non_sn_cf_handle,
    rocksdb::WriteBatch & delete_batch)
{
    std::unique_ptr<rocksdb::Iterator> iter{indexes->NewIterator(rocksdb::ReadOptions{}, sn_cf_handle)};
    rocksdb::Slice key_s{reinterpret_cast<const char *>(&sn), sizeof(sn)};
    iter->Seek(key_s);
    if (iter->Valid())
    {
        auto max_key = std::numeric_limits<int64_t>::max();
        auto max_key_s = rocksdb::Slice{reinterpret_cast<const char *>(&max_key), sizeof(max_key)};
        delete_batch.DeleteRange(sn_cf_handle, iter->key(), max_key_s);
        delete_batch.DeleteRange(non_sn_cf_handle, iter->value(), max_key_s);
    }
}

/// [..., sn)
void InvertedIndexes::trimHeadWithLockHeld(
    int64_t sn,
    rocksdb::ColumnFamilyHandle * sn_cf_handle,
    rocksdb::ColumnFamilyHandle * non_sn_cf_handle,
    rocksdb::WriteBatch & delete_batch)
{
    std::unique_ptr<rocksdb::Iterator> iter{indexes->NewIterator(rocksdb::ReadOptions{}, sn_cf_handle)};
    rocksdb::Slice key_s{reinterpret_cast<const char *>(&sn), sizeof(sn)};
    iter->Seek(key_s);
    if (iter->Valid())
    {
        int64_t min_key = 0;
        auto min_key_s = rocksdb::Slice{reinterpret_cast<const char *>(&min_key), sizeof(min_key)};
        delete_batch.DeleteRange(sn_cf_handle, min_key_s, iter->key());
        delete_batch.DeleteRange(non_sn_cf_handle, min_key_s, iter->value());
    }
}
}
