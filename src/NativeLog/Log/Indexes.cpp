#include "Indexes.h"

#include <NativeLog/Rocks/IntegerComparator.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int CANNOT_OPEN_DATABASE;
}
}

namespace nlog
{
Indexes::Indexes(fs::path index_dir_, int64_t base_sn_, Poco::Logger * logger_)
    : index_dir(std::move(index_dir_)), base_sn(base_sn_), logger(logger_)
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
    column_families.reserve(4);

    /// default cf : sn to physical position mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, cf_options));
    /// position_sn cf : physical position to sn mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("position_sn", cf_options));
    /// atime_sn cf : append time to sn mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("atime_sn", cf_options));
    /// etime_sn cf : event time to sn mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("etime_sn", cf_options));

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    cf_handles.reserve(4);

    rocksdb::DB * db;
    /// Opening rocksdb can be very expensive: around 1 second
    auto status = rocksdb::DB::Open(options, index_dir, column_families, &cf_handles, &db);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_DATABASE, "Filed to open index db, {}", status.ToString());

    indexes.reset(db);
    sn_position_cf_handle = cf_handles[0];
    position_sn_cf_handle = cf_handles[1];
    etime_sn_cf_handle = cf_handles[2];
    atime_sn_cf_handle = cf_handles[3];

    last_indexed_etimestamp = lastIndexedEventTimeSequence();
    last_indexed_atimestamp = lastIndexedAppendTimeSequence();
}

Indexes::~Indexes()
{
    close();
}

void Indexes::close()
{
    if (closed.test_and_set())
        /// Already closed
        return;

    if (!indexes)
        return;

    sync();

    for (auto * cf_handle : {sn_position_cf_handle, position_sn_cf_handle, etime_sn_cf_handle, atime_sn_cf_handle})
    {
        if (cf_handle != nullptr)
        {
            auto status = indexes->DestroyColumnFamilyHandle(cf_handle);
            if (!status.ok())
                LOG_ERROR(logger, "Failed to destroy column family handle");
        }
    }

    auto status = indexes->Close();
    if (!status.ok())
        LOG_ERROR(logger, "Failed to close indexes db");

    LOG_INFO(logger, "Closed indexes successfully");
}

std::optional<TimestampSequence> Indexes::lastIndexedAppendTimeSequence() const
{
    return lastIndexedEntry(atime_sn_cf_handle);
}

std::optional<TimestampSequence> Indexes::lastIndexedEventTimeSequence() const
{
    return lastIndexedEntry(etime_sn_cf_handle);
}

std::optional<SequencePosition> Indexes::lastIndexedSequencePosition() const
{
    return lastIndexedEntry(sn_position_cf_handle);
}

std::optional<PositionSequence> Indexes::lastIndexedPositionSequence() const
{
    return lastIndexedEntry(position_sn_cf_handle);
}

std::optional<IndexEntry> Indexes::lastIndexedEntry(rocksdb::ColumnFamilyHandle * cf_handle) const
{
    std::unique_ptr<rocksdb::Iterator> iter{indexes->NewIterator(rocksdb::ReadOptions{}, cf_handle)};
    iter->SeekToLast();
    if (iter->Valid())
    {
        assert(iter->key().size() == sizeof(int64_t));
        auto key = *reinterpret_cast<const int64_t *>(iter->key().data());
        assert(iter->value().size() == sizeof(int64_t));
        auto value = *reinterpret_cast<const int64_t *>(iter->value().data());

        return IndexEntry{key, value};
    }
    return {};
}

SequencePosition Indexes::lowerBoundPositionForSequence(int64_t sn) const
{
    auto entry = lowerBound(sn, sn_position_cf_handle);
    if (entry)
        return *entry;

    return {base_sn, 0};
}

std::optional<TimestampSequence> Indexes::lowerBoundSequenceForTimestamp(int64_t ts, bool append_time) const
{
    if (append_time)
        return lowerBound(ts, atime_sn_cf_handle);
    else
        return lowerBound(ts, etime_sn_cf_handle);
}

/// IndexEntry.key <= key
/// @return IndexEntry which has key less or equal to key specified in the parameter if found such entry
/// Otherwise return {}
std::optional<IndexEntry> Indexes::lowerBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const
{
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
        return IndexEntry{k, v};
    }
    return {};
}

std::optional<PositionSequence> Indexes::upperBoundSequenceForPosition(int64_t position) const
{
    return upperBound(position, position_sn_cf_handle);
}

/// IndexEntry.key >= key
/// @return IndexEntry which has key great or equal to key specified in the parameter if found such entry
/// Otherwise return {}
std::optional<IndexEntry> Indexes::upperBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const
{
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
        return IndexEntry{k, v};
    }
    return {};
}

void Indexes::index(
    int64_t largest_sn,
    int64_t physical_position,
    const TimestampSequence & max_etimestamp_sn,
    const TimestampSequence & max_atimestamp_sn)
{
    rocksdb::Slice sn_s{reinterpret_cast<const char *>(&largest_sn), sizeof(largest_sn)};
    rocksdb::Slice position_s{reinterpret_cast<const char *>(&physical_position), sizeof(physical_position)};

    rocksdb::WriteBatch batch;

    /// Insert sn -> physical position mapping
    batch.Put(sn_position_cf_handle, sn_s, position_s);

    /// Insert physical position -> sn mapping
    batch.Put(position_sn_cf_handle, position_s, sn_s);

    index(max_etimestamp_sn, max_atimestamp_sn, batch);
}

void Indexes::index(const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn)
{
    rocksdb::WriteBatch batch;
    index(max_etimestamp_sn, max_atimestamp_sn, batch);
}

void Indexes::index(
    const TimestampSequence & max_etimestamp_sn, const TimestampSequence & max_atimestamp_sn, rocksdb::WriteBatch & batch)
{
    if (max_etimestamp_sn > last_indexed_etimestamp)
    {
        /// event timestamp -> sn mapping
        rocksdb::Slice event_time_s{reinterpret_cast<const char *>(&max_etimestamp_sn.key), sizeof(max_etimestamp_sn.key)};
        rocksdb::Slice event_sn_s{reinterpret_cast<const char *>(&max_etimestamp_sn.value), sizeof(max_etimestamp_sn.value)};
        batch.Put(etime_sn_cf_handle, event_time_s, event_sn_s);
    }

    if (max_atimestamp_sn > last_indexed_atimestamp)
    {
        /// append timestamp -> sn mapping
        rocksdb::Slice append_time_s{reinterpret_cast<const char *>(&max_atimestamp_sn.key), sizeof(max_atimestamp_sn.key)};
        rocksdb::Slice event_sn_s{reinterpret_cast<const char *>(&max_atimestamp_sn.value), sizeof(max_atimestamp_sn.value)};
        batch.Put(atime_sn_cf_handle, append_time_s, event_sn_s);
    }

    if (!batch.HasPut())
        return;

    rocksdb::WriteOptions write_options;
    /// write_options.disableWAL = true;

    auto status = indexes->Write(write_options, &batch);
    if (!status.ok())
        LOG_ERROR(logger, "Failed to update indexes, {}", status.ToString());

    /// sync();

    if (max_etimestamp_sn > last_indexed_etimestamp)
        last_indexed_etimestamp = max_etimestamp_sn;

    if (max_atimestamp_sn > last_indexed_atimestamp)
        last_indexed_atimestamp = max_atimestamp_sn;
}

void Indexes::sync()
{
    auto status = indexes->FlushWAL(true);
    if (!status.ok())
        LOG_ERROR(logger, "Failed to flush indexes, {}", status.ToString());
}
}
