#include "Indexes.h"

#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    const extern int CANNOT_OPEN_DATABASE;
}
}

namespace nlog
{
Indexes::Indexes(const fs::path & index_dir, int64_t base_offset_, Poco::Logger * logger_)
    : base_offset(base_offset_), last_indexed_etimestamp(-1, -1), last_indexed_atimestamp(-1, -1), logger(logger_)
{
    /// FIXME, lazy init index db since if there are lots of segments, init may take a long time
    rocksdb::Options options;
    /// Since index entry is append only, we don't need compaction, keys are always unique and monotonically increasing
    options.num_levels = 1;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.atomic_flush = true;

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    /// default cf : offset to physical position mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    /// position_offset cf : physical position to offset mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("position_offset", rocksdb::ColumnFamilyOptions()));
    /// atime_offset cf : append time to offset mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("atime_offset", rocksdb::ColumnFamilyOptions()));
    /// etime_offset cf : event time to offset mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("etime_offset", rocksdb::ColumnFamilyOptions()));

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;

    rocksdb::DB * db;
    auto status = rocksdb::DB::Open(options, index_dir, column_families, &cf_handles, &db);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_DATABASE, "Filed to open index db, {}", status.ToString());

    indexes.reset(db);
    offset_position_cf_handle = cf_handles[0];
    position_offset_cf_handle = cf_handles[1];
    etime_offset_cf_handle = cf_handles[2];
    atime_offset_cf_handle = cf_handles[3];

    last_indexed_etimestamp = lastIndexedEventTimeOffset();
    last_indexed_atimestamp = lastIndexedAppendTimeOffset();
}

Indexes::~Indexes()
{
    for (auto * cf_handle : {offset_position_cf_handle, position_offset_cf_handle, etime_offset_cf_handle, atime_offset_cf_handle})
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
}

TimestampOffset Indexes::lastIndexedAppendTimeOffset()
{
    return lastIndexedEntry(atime_offset_cf_handle);
}

TimestampOffset Indexes::lastIndexedEventTimeOffset()
{
    return lastIndexedEntry(etime_offset_cf_handle);
}

OffsetPosition Indexes::lastIndexedOffsetPosition()
{
    return lastIndexedEntry(offset_position_cf_handle);
}

PositionOffset Indexes::lastIndexedPositionOffset()
{
    return lastIndexedEntry(position_offset_cf_handle);
}

IndexEntry Indexes::lastIndexedEntry(rocksdb::ColumnFamilyHandle * cf_handle) const
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
    return {-1, -1};
}

OffsetPosition Indexes::lowerBoundOffsetPosition(int64_t offset)
{
    auto entry = lowerBound(offset, offset_position_cf_handle);
    if (!entry.isInvalid())
        return entry;

    return {base_offset, 0};
}

/// IndexEntry.key <= key
/// @return IndexEntry which has key less or equal to key specified in the parameter if found such entry
/// Otherwise return IndexEntry{-1, -1}
IndexEntry Indexes::lowerBound(int64_t key, rocksdb::ColumnFamilyHandle * cf_handle) const
{
    std::unique_ptr<rocksdb::Iterator> iter{indexes->NewIterator(rocksdb::ReadOptions{}, cf_handle)};
    rocksdb::Slice key_s{reinterpret_cast<char *>(&key), sizeof(key)};
    iter->SeekForPrev(key_s);
    if (iter->Valid())
    {
        assert(iter->key().size() == sizeof(int64_t));
        auto k = *reinterpret_cast<const int64_t *>(iter->key().data());
        assert(iter->value().size() == sizeof(int64_t));
        auto v = *reinterpret_cast<const int64_t *>(iter->value().data());
        return {k, v};
    }
    return {-1, -1};
}

void Indexes::index(
    int64_t largest_offset,
    int64_t physical_position,
    const TimestampOffset & max_etimestamp_offset,
    const TimestampOffset & max_atimestamp_offset)
{
    rocksdb::Slice offset_s{reinterpret_cast<char *>(&largest_offset), sizeof(largest_offset)};
    rocksdb::Slice position_s{reinterpret_cast<char *>(&physical_position), sizeof(physical_position)};

    rocksdb::WriteBatch batch;

    /// Insert offset -> physical position mapping
    batch.Put(offset_position_cf_handle, offset_s, position_s);

    /// Insert physical position -> offset mapping
    batch.Put(position_offset_cf_handle, position_s, offset_s);

    index(max_etimestamp_offset, max_atimestamp_offset, batch);
}

void Indexes::index(const TimestampOffset & max_etimestamp_offset, const TimestampOffset & max_atimestamp_offset)
{
    rocksdb::WriteBatch batch;
    index(max_etimestamp_offset, max_atimestamp_offset, batch);
}

void Indexes::index(
    const TimestampOffset & max_etimestamp_offset, const TimestampOffset & max_atimestamp_offset, rocksdb::WriteBatch & batch)
{
    if (max_etimestamp_offset > last_indexed_etimestamp)
    {
        /// event timestamp -> offset mapping
        rocksdb::Slice event_time_s{reinterpret_cast<const char *>(&max_etimestamp_offset.key), sizeof(max_etimestamp_offset.key)};
        rocksdb::Slice event_offset_s{reinterpret_cast<const char *>(&max_etimestamp_offset.value), sizeof(max_etimestamp_offset.value)};
        batch.Put(etime_offset_cf_handle, event_time_s, event_offset_s);
    }

    if (max_atimestamp_offset > last_indexed_atimestamp)
    {
        /// append timestamp -> offset mapping
        rocksdb::Slice append_time_s{reinterpret_cast<const char *>(&max_atimestamp_offset.key), sizeof(max_atimestamp_offset.key)};
        rocksdb::Slice event_offset_s{reinterpret_cast<const char *>(&max_atimestamp_offset.value), sizeof(max_atimestamp_offset.value)};
        batch.Put(atime_offset_cf_handle, append_time_s, event_offset_s);
    }

    rocksdb::WriteOptions write_options;
    /// write_options.disableWAL = true;

    auto status = indexes->Write(write_options, &batch);
    if (!status.ok())
        LOG_ERROR(logger, "Failed to update indexes, {}", status.ToString());

    /// sync();

    if (max_etimestamp_offset > last_indexed_etimestamp)
        last_indexed_etimestamp = max_etimestamp_offset;

    if (max_atimestamp_offset > last_indexed_atimestamp)
        last_indexed_atimestamp = max_atimestamp_offset;
}

void Indexes::sync()
{
    auto status = indexes->FlushWAL(true);
    if (!status.ok())
        LOG_ERROR(logger, "Failed to flush indexes, {}", status.ToString());
}
}
