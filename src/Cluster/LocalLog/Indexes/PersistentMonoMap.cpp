#include <Cluster/LocalLog/Indexes/PersistentMonoMap.h>
#include <Cluster/LocalLog/Indexes/SequencePosition.h>
#include <Cluster/LocalLog/Indexes/TimestampSequence.h>

#include <Cluster/Common/EpochSequence.h>
#include <Cluster/Common/serde.h>
#include <base/errnoToString.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>

#include <charconv>

namespace DB::ErrorCodes
{
extern const int OK;
extern const int ATOMIC_RENAME_FAIL;
extern const int LOGICAL_ERROR;
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int CANNOT_READ_FILE;
extern const int CANNOT_DELETE_FILE;
extern const int CANNOT_FLUSH_FILE;
extern const int CANNOT_FSTAT;
extern const int CANNOT_TRUNCATE_FILE;
extern const int FILE_CORRUPTION;
extern const int INCORRECT_FILE_NAME;
extern const int DIRECTORY_DOESNT_EXIST;
extern const int FILE_DOESNT_EXIST;
}

namespace cluster::nlog
{

namespace
{
inline fs::path segmentLogPath(fs::path segment_path, uint64_t segment_id)
{
    segment_path += fmt::format(".{}", segment_id);
    return segment_path;
}
}

template <typename KeyValue>
PersistentMonoMap<KeyValue>::PersistentMonoMap(
    const fs::path & filename, uint64_t file_max_size_, bool preallocate_, std::string app_, LoggerPtr logger_)
    : app(std::move(app_))
    , max_size(file_max_size_)
    , preallocate(preallocate_)
    , log_file(std::make_shared<AppendOnlyFile>(filename, file_max_size_, preallocate_))
    , logger(logger_)
{
}

template <typename KeyValue>
CallResult<bool> PersistentMonoMap<KeyValue>::maybeIndex(KeyValue entry, bool sync)
{
    if (!current_log_seg_loaded)
    {
        /// We like lazy init the map because we like to bootstrap Proton asap.
        /// This may also make memory efficient if the mono map is not used /referenced.
        if (auto err = recover(); err != DB::ErrorCodes::OK)
            return CallResult<bool>{false, err};
    }

    if (!cache.empty() && entry < cache.back().entry)
        return CallResult<bool>{false};

    bool need_update = cache.empty() || (cache.back().entry.key() < entry.key());
    if (!need_update)
        return CallResult<bool>{false};

    if (log_size >= max_size)
        if (auto err = roll(entry, sync); err != DB::ErrorCodes::OK)
            return CallResult<bool>{false, err};

    auto data = cluster::serialize<ByteVector>(entry, /*version=*/1);
    auto res = log_file->append(data.data(), data.size());
    if (res.hasError())
    {
        LOG_ERROR(logger, "Failed to save {} to {} log file, error={}", entry.string(), app, errnoToString(res.error_code));
        return CallResult{false, DB::ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR};
    }

    assert(res.result == data.size());

    if (sync)
    {
        if (auto err = log_file->syncTail(log_size); err != 0)
        {
            LOG_ERROR(logger, "Failed to sync tail {} log for {}, error={}", app, entry.string(), errnoToString(err));
            return CallResult<bool>{false, DB::ErrorCodes::CANNOT_FLUSH_FILE};
        }
    }

    cache.emplace_back(entry, log_size);

    log_size += data.size();

    return CallResult<bool>{true};
}

/// Find the first entry whose key is >= key passed in.
template <typename KeyValue>
CallResult<std::optional<typename PersistentMonoMap<KeyValue>::FoundKeyValue>>
PersistentMonoMap<KeyValue>::findFirstEntryByKey_EqualToOrGreaterThan(KeyType key)
{
    if (!cache.empty() && cache.back().entry.key() < key)
        return CallResult{std::optional<FoundKeyValue>{}};

    if (!cache.empty() && cache.front().entry.key() <= key)
    {
        /// The target key is contained in the current active segment (cached)
        /// We don't need try rolled log segs since the rolled ones have even smaller keys
        /// Binary search to find it out target entry.key() >= key_to_find
        auto iter = std::lower_bound(cache.begin(), cache.end(), key, [](const auto & entry_with_offset, auto key_passed_in) {
            return entry_with_offset.entry.key() < key_passed_in;
        });

        assert(iter != cache.end());
        return CallResult{std::optional{FoundKeyValue{*iter, /*segment_id_=*/0, &cache, /*truncation_point_=*/iter}}};
    }

    assert(cache.empty() || cache.front().entry.key() > key);

    /// There are 2 cases :
    /// 1) The current cached contains keys which are all greater than the key passed in which we will revisit later
    ///    since this may not be the first entry
    /// 2) The first entry may be in some rolled log segs
    /// We will need try case 2 first to find the first match and then 1) if 2) find nothing
    /// Slow path, if we have rolled log seg try to find there
    for (auto riter = seg_id_to_entry_offset_map.rbegin(), riter_end = seg_id_to_entry_offset_map.rend(); riter != riter_end; ++riter)
    {
        auto first_entry = riter->second.begin();
        if (first_entry->entry.key() == key)
        {
            return CallResult{std::optional{FoundKeyValue{*first_entry, riter->first, &riter->second, first_entry}}};
        }
        else if (first_entry->entry.key() < key)
        {
            /// This is the first rolled log seg whose first entry key is less than the key passed in.
            /// This log seg may contain an equal entry key
            if (auto err = loadRolledLogSegmentFully(riter->first, riter->second); err != DB::ErrorCodes::OK)
                return CallResult<std::optional<FoundKeyValue>>{err};

            {
                auto & entry_cache = riter->second;

                /// Binary search to find it out target entry.key() >= key_to_find
                auto iter
                    = std::lower_bound(entry_cache.begin(), entry_cache.end(), key, [](const auto & entry_with_offset, auto key_passed_in) {
                          return entry_with_offset.entry.key() < key_passed_in;
                      });

                if (iter != entry_cache.end())
                    return CallResult{std::optional{FoundKeyValue{*iter, riter->first, &entry_cache, /*truncation_point_=*/iter}}};
            }

            {
                /// All keys in this rolled log seg are smaller than the key passed in
                /// First try prev log segment if there is, otherwise then try the current active log segment
                if (riter != seg_id_to_entry_offset_map.rbegin())
                {
                    --riter; /// Move to next bigger log segment

                    auto & entry_cache = riter->second;
                    assert(!entry_cache.empty() && entry_cache.front().entry.key() > key);
                    return CallResult{std::optional{
                        FoundKeyValue{entry_cache.front(), riter->first, &entry_cache, /*truncation_point_=*/entry_cache.begin()}}};
                }
                else
                {
                    return CallResult{
                        std::optional{FoundKeyValue{cache.front(), /*segment_id_=*/0, &cache, /*truncation_point_=*/cache.begin()}}};
                }
            }
        }
    }

    /// When the logic reaches here, values in all rolled log segs are greater than the value passed in
    /// Just return the very first entry in the earliest rolled log if there are rolled logs; otherwise
    /// return the first entry of the first entry of the current active log seg.
    if (!seg_id_to_entry_offset_map.empty())
    {
        auto first_rolled = seg_id_to_entry_offset_map.begin();
        auto & entry_cache = first_rolled->second;
        assert(!entry_cache.empty() && entry_cache.front().entry.key() > key);
        return CallResult{std::optional{FoundKeyValue{
            entry_cache.front(), /*segment_id_=*/first_rolled->first, &entry_cache, /*truncation_point_=*/entry_cache.begin()}}};
    }
    else if (!cache.empty())
        return CallResult{std::optional{FoundKeyValue{cache.front(), /*segment_id_=*/0, &cache, /*truncation_point_=*/cache.begin()}}};
    else
        return CallResult{std::optional<FoundKeyValue>{}};
}

template <typename KeyValue>
CallResult<std::optional<typename PersistentMonoMap<KeyValue>::FoundKeyValue>>
PersistentMonoMap<KeyValue>::findFirstEntryByKey_LessThanOrEqualTo(KeyType key)
{
    auto bin_search = [](EntryContainer & entry_cache, KeyType key_to_find, uint64_t segment_id) {
        auto iter
            = std::lower_bound(entry_cache.begin(), entry_cache.end(), key_to_find, [](const auto & entry_with_offset, auto key_passed_in) {
                  return entry_with_offset.entry.key() < key_passed_in;
              });

        if (iter == entry_cache.end())
            /// All keys are less than the value to find, use the last key
            return CallResult{
                std::optional{FoundKeyValue{entry_cache.back(), segment_id, &entry_cache, /*truncation_point_=*/entry_cache.end()}}};

        if (iter->entry.key() == key_to_find)
            return CallResult{std::optional{FoundKeyValue{*iter, segment_id, &entry_cache, /*truncation_point_=*/iter}}};

        /// lower_bound return smallest key which is greater or equal to the key passed in
        /// So, we will need backoff one step. Since we have checked the target key is in the
        /// the current log segment, it is guaranteed that iter will not be the cache.begin()
        /// when the logic reaches here.
        assert(iter != entry_cache.begin());
        auto truncation_point = iter;
        --iter;
        return CallResult{std::optional{FoundKeyValue{*iter, segment_id, &entry_cache, /*truncation_point_=*/truncation_point}}};
    };

    /// First, search the current log segment
    if (!cache.empty() && cache.front().entry.key() <= key)
        /// The target key is contained in the current active segment (cached)
        return bin_search(cache, key, /*segment_id=*/0);

    /// All keys in the active log segment are greater than the key passed in.
    /// Then try slower path, check the rolled log segments
    for (auto riter = seg_id_to_entry_offset_map.rbegin(), riter_end = seg_id_to_entry_offset_map.rend(); riter != riter_end; ++riter)
    {
        auto first_entry = riter->second.begin();
        if (first_entry->entry.key() == key)
        {
            return CallResult{std::optional{FoundKeyValue{*first_entry, riter->first, &riter->second, first_entry}}};
        }
        else if (first_entry->entry.key() < key)
        {
            if (auto err = loadRolledLogSegmentFully(riter->first, riter->second); err != DB::ErrorCodes::OK)
                return CallResult<std::optional<FoundKeyValue>>{err};

            return bin_search(riter->second, key, riter->first);
        }
    }

    return CallResult{std::optional<FoundKeyValue>{}};
}

/// Find the first entry whose value is >= value passed in.
template <typename KeyValue>
CallResult<std::optional<typename PersistentMonoMap<KeyValue>::FoundKeyValue>>
PersistentMonoMap<KeyValue>::findFirstEntryByValue_EqualToOrGreaterThan(ValueType value)
{
    if (!cache.empty() && cache.back().entry.value() < value)
        return CallResult{std::optional<FoundKeyValue>{}};

    if (!cache.empty() && cache.front().entry.value() <= value)
    {
        /// The target value is contained in the current active segment (cached)
        /// We don't need try rolled log segs since the rolled ones have even smaller values
        /// Binary search to find it out target entry.value() >= value_to_find
        auto iter = std::lower_bound(cache.begin(), cache.end(), value, [](const auto & entry_with_offset, auto value_passed_in) {
            return entry_with_offset.entry.value() < value_passed_in;
        });

        assert(iter != cache.end());
        return CallResult{std::optional{FoundKeyValue{*iter, /*segment_id_=*/0, &cache, /*truncation_point_=*/iter}}};
    }

    assert(cache.empty() || cache.front().entry.value() > value);

    /// There are 2 cases :
    /// 1) The current cached contains values which are all greater than the value passed in which we will revisit later
    ///    since this may not be the first entry
    /// 2) The first entry may be in some rolled log segs
    /// We will need try case 2 first to find the first match and then 1) if 2) find nothing
    /// Slow path, if we have rolled log seg try to find there
    for (auto riter = seg_id_to_entry_offset_map.rbegin(), riter_end = seg_id_to_entry_offset_map.rend(); riter != riter_end; ++riter)
    {
        auto first_entry = riter->second.begin();
        if (first_entry->entry.value() == value)
        {
            return CallResult{std::optional{FoundKeyValue{*first_entry, riter->first, &riter->second, first_entry}}};
        }
        else if (first_entry->entry.value() < value)
        {
            /// This is the first rolled log seg whose first entry value is less than the value passed in.
            /// This log seg may contain an equal entry value
            if (auto err = loadRolledLogSegmentFully(riter->first, riter->second); err != DB::ErrorCodes::OK)
                return CallResult<std::optional<FoundKeyValue>>{err};

            {
                auto & entry_cache = riter->second;

                /// Binary search to find it out target entry.value() >= value_to_find
                auto iter = std::lower_bound(
                    entry_cache.begin(), entry_cache.end(), value, [](const auto & entry_with_offset, auto value_passed_in) {
                        return entry_with_offset.entry.value() < value_passed_in;
                    });

                if (iter != entry_cache.end())
                    return CallResult{std::optional{FoundKeyValue{*iter, riter->first, &entry_cache, /*truncation_point_=*/iter}}};
            }

            {
                /// All values in this rolled log seg are smaller than the value passed in
                /// First try prev log segment if there is, otherwise then try the current active log segment
                if (riter != seg_id_to_entry_offset_map.rbegin())
                {
                    --riter; /// Move to next bigger log segment

                    auto & entry_cache = riter->second;
                    assert(!entry_cache.empty() && entry_cache.front().entry.value() > value);
                    return CallResult{std::optional{
                        FoundKeyValue{entry_cache.front(), riter->first, &entry_cache, /*truncation_point_=*/entry_cache.begin()}}};
                }
                else
                {
                    return CallResult{
                        std::optional{FoundKeyValue{cache.front(), /*segment_id_=*/0, &cache, /*truncation_point_=*/cache.begin()}}};
                }
            }
        }
    }

    /// When the logic reaches here, values in all rolled log segs are greater than the value passed in
    /// Just return the very first entry in the earliest rolled log if there are rolled logs; otherwise
    /// return the first entry of the first entry of the current active log seg.
    if (!seg_id_to_entry_offset_map.empty())
    {
        auto first_rolled = seg_id_to_entry_offset_map.begin();
        auto & entry_cache = first_rolled->second;
        assert(!entry_cache.empty() && entry_cache.front().entry.value() > value);
        return CallResult{std::optional{FoundKeyValue{
            entry_cache.front(), /*segment_id_=*/first_rolled->first, &entry_cache, /*truncation_point_=*/entry_cache.begin()}}};
    }
    else if (!cache.empty())
        return CallResult{std::optional{FoundKeyValue{cache.front(), /*segment_id_=*/0, &cache, /*truncation_point_=*/cache.begin()}}};
    else
        return CallResult{std::optional<FoundKeyValue>{}};
}

/// Find the first entry whose value is less or equal to the value passed in.
template <typename KeyValue>
CallResult<std::optional<typename PersistentMonoMap<KeyValue>::FoundKeyValue>>
PersistentMonoMap<KeyValue>::findFirstEntryByValue_LessThanOrEqualTo(ValueType value)
{
    /// Since we value is monotonically increased, we can do binary search as well
    auto bin_search = [](EntryContainer & entry_cache, ValueType value_to_find, uint64_t segment_id) {
        /// Binary search to find it out
        auto iter = std::lower_bound(
            entry_cache.begin(), entry_cache.end(), value_to_find, [](const auto & entry_with_offset, auto value_passed_in) {
                return entry_with_offset.entry.value() < value_passed_in;
            });

        if (iter == entry_cache.end())
            /// All values are less than the value to find, use the last value
            return CallResult{
                std::optional{FoundKeyValue{entry_cache.back(), segment_id, &entry_cache, /*truncation_point_=*/entry_cache.end()}}};

        if (iter->entry.value() == value_to_find)
            return CallResult{std::optional{FoundKeyValue{*iter, segment_id, &entry_cache, /*truncation_point_=*/iter}}};

        /// lower_bound return first element which has value greater or equal to the value passed in
        /// So, we will need backoff one step. Since we have checked the target value is in the
        /// the current log segment, it is guaranteed that iter will not be the cache.begin()
        /// when the logic reaches here.
        assert(iter != entry_cache.begin());
        auto truncation_point = iter;
        --iter;
        return CallResult{std::optional{FoundKeyValue{*iter, segment_id, &entry_cache, /*truncation_point_=*/truncation_point}}};
    };

    if (!cache.empty() && cache.front().entry.value() <= value)
        /// The target value is contained in the current active segment (cached)
        return bin_search(cache, value, /*segment_id=*/0);

    /// Slow path, if we have rolled log seg, try to find there
    for (auto riter = seg_id_to_entry_offset_map.rbegin(), riter_end = seg_id_to_entry_offset_map.rend(); riter != riter_end; ++riter)
    {
        auto first_entry = riter->second.begin();
        if (first_entry->entry.value() == value)
        {
            return CallResult{std::optional{FoundKeyValue{*first_entry, riter->first, &riter->second, first_entry}}};
        }
        else if (first_entry->entry.value() < value)
        {
            if (auto err = loadRolledLogSegmentFully(riter->first, riter->second); err != DB::ErrorCodes::OK)
                return CallResult<std::optional<FoundKeyValue>>{err};

            return bin_search(riter->second, value, riter->first);
        }
    }

    return CallResult{std::optional<FoundKeyValue>{}};
}

template <typename KeyValue>
CallResult<std::optional<KeyValue>> PersistentMonoMap<KeyValue>::last()
{
    if (!current_log_seg_loaded)
        if (auto err = recover(); err != DB::ErrorCodes::OK)
            return CallResult<std::optional<KeyValue>>{err};

    if (!cache.empty())
        return CallResult{std::optional{cache.back().entry}};

    /// Check if we have rolled segment files, if yes, use the last entry
    if (!seg_id_to_entry_offset_map.empty())
    {
        auto last_segment = seg_id_to_entry_offset_map.rbegin();
        if (auto err = loadRolledLogSegmentFully(last_segment->first, last_segment->second); err != DB::ErrorCodes::OK)
            return CallResult<std::optional<KeyValue>>{err};

        return CallResult{std::optional{last_segment->second.back().entry}};
    }

    return CallResult{std::optional<KeyValue>{}};
}

template <typename KeyValue>
CallResult<std::optional<KeyValue>> PersistentMonoMap<KeyValue>::prevLast()
{
    if (!current_log_seg_loaded)
        if (auto err = recover(); err != DB::ErrorCodes::OK)
            return CallResult<std::optional<KeyValue>>{err};

    if (cache.size() >= 2)
        return CallResult{std::optional{(++cache.rbegin())->entry}};

    if (!seg_id_to_entry_offset_map.empty())
    {
        /// Try the last rolled segment
        auto last_segment = seg_id_to_entry_offset_map.rbegin();
        if (auto err = loadRolledLogSegmentFully(last_segment->first, last_segment->second); err != DB::ErrorCodes::OK)
            return CallResult<std::optional<KeyValue>>{err};

        const auto & rcache = last_segment->second;

        if (cache.size() == 1)
            /// Current cache already has one entry, so prev one is the last one in the rolled segment
            return CallResult{std::optional{(rcache.rbegin())->entry}};
        else
            return CallResult{std::optional{(++rcache.rbegin())->entry}};
    }

    return CallResult{std::optional<KeyValue>{}};
}

template <typename KeyValue>
CallResult<std::optional<typename KeyValue::KeyType>> PersistentMonoMap<KeyValue>::lastKey()
{
    auto res = last();
    if (!res.hasError())
    {
        if (res.result.has_value())
            return CallResult{std::optional{res.result->key()}};

        return CallResult{std::optional<KeyType>{}};
    }
    else
        return CallResult{std::optional<KeyType>{}, res.error_code};
}

template <typename KeyValue>
CallResult<std::optional<typename KeyValue::KeyType>> PersistentMonoMap<KeyValue>::prevLastKey()
{
    auto res = prevLast();
    if (!res.hasError())
    {
        if (res.result.has_value())
            return CallResult{std::optional{res.result->key()}};

        return CallResult{std::optional<KeyType>{}};
    }
    else
        return CallResult{std::optional<KeyType>{}, res.error_code};
}

template <typename KeyValue>
CallResult<std::optional<KeyValue>> PersistentMonoMap<KeyValue>::first()
{
    if (!current_log_seg_loaded)
        if (auto err = recover(); err != DB::ErrorCodes::OK)
            return CallResult<std::optional<KeyValue>>{err};

    if (!seg_id_to_entry_offset_map.empty())
        return CallResult{std::optional{seg_id_to_entry_offset_map.begin()->second.front().entry}};
    else if (!cache.empty())
        return CallResult{std::optional{cache.front().entry}};
    else
        return CallResult{std::optional<KeyValue>{}};
}
/// trim tail [start_key, ...)
template <typename KeyValue>
int PersistentMonoMap<KeyValue>::trim(ValueType start_key)
{
    LOG_INFO(logger, "Trimming tail {} log starting at {}={}", app, KeyValue::keyName(), start_key);

    if (!current_log_seg_loaded)
        if (auto err = recover(); err != DB::ErrorCodes::OK)
            return err;

    auto res = findFirstEntryByKey_EqualToOrGreaterThan(start_key);
    if (res.hasError() || !res.result.has_value())
        return res.error_code;

    auto trim_log = [](const AppendOnlyFile & segment_file,
                       size_t segment_file_size,
                       KeyType begin_key,
                       KeyValueWithLogOffset & kvo,
                       const std::string & app_,
                       LoggerPtr logger_) {
        assert(kvo.offset <= segment_file_size);
        if (kvo.offset == segment_file_size)
            /// Nothing to truncate
            return DB::ErrorCodes::OK;

        if (auto err = segment_file.truncate(kvo.offset); err != 0)
        {
            LOG_ERROR(
                logger_,
                "Failed to truncate {} log file={} to {}={} with bytes range [{}, {}] which has {}, error={}",
                app_,
                segment_file.getFilename().c_str(),
                KeyValue::keyName(),
                begin_key,
                kvo.offset,
                segment_file_size,
                kvo.entry.string(),
                errnoToString(err));

            return DB::ErrorCodes::CANNOT_TRUNCATE_FILE;
        }

        LOG_INFO(
            logger_,
            "Truncated {} log file={} to {}={} with bytes range [{}, {}] which has {}",
            app_,
            segment_file.getFilename().c_str(),
            KeyValue::keyName(),
            begin_key,
            kvo.offset,
            segment_file_size,
            kvo.entry.string());

        return DB::ErrorCodes::OK;
    };

    /// We don't use res.result->truncation_point for file truncation since it is not convenient
    /// to calculate
    auto & kvo = res.result->entry_offset;
    if (res.result->segment_id == 0)
    {
        // First trim current log
        if (auto err = trim_log(*log_file, log_size, start_key, kvo, app, logger); err != DB::ErrorCodes::OK)
            return err;

        /// Then trim the cache as well
        if (res.result->cache_to_truncate != &cache)
            return DB::ErrorCodes::LOGICAL_ERROR;

        cache.erase(res.result->truncation_point, cache.end());

        /// Update log size
        log_size = kvo.offset;
        assert((cache.empty() && log_size == 0) || (!cache.empty() && log_size != 0));

        /// When log size becomes zero, we don't gc the current log segment file since it will be used soon
        return DB::ErrorCodes::OK;
    }
    else
    {
        /// Found key in rolled segment files, then remove all segment files from the truncation point
        /// to the current [truncation_point segment_id (maybe partial trim), segment_id + 1, current log_file]
        auto iter = seg_id_to_entry_offset_map.find(res.result->segment_id);
        assert(iter != seg_id_to_entry_offset_map.end());

        auto segment_filepath = segmentLogPath(log_file->getFilename(), res.result->segment_id);
        AppendOnlyFile segment_file(
            segment_filepath, /*file_already_exists=*/true, /*read_only_=*/false, /*initial_file_size=*/0, /*preallocate=*/false);

        auto file_size = segment_file.size();
        if (file_size.hasError())
        {
            LOG_ERROR(
                logger, "Failed to get {} file={} size, error={}", app, segment_filepath.c_str(), errnoToString(file_size.error_code));

            return DB::ErrorCodes::CANNOT_FSTAT;
        }

        /// First trim log
        if (auto err = trim_log(segment_file, file_size.result, start_key, kvo, app, logger); err != DB::ErrorCodes::OK)
            return err;

        /// Then trim the cache as well
        if (res.result->cache_to_truncate != &iter->second)
            return DB::ErrorCodes::LOGICAL_ERROR;

        iter->second.erase(res.result->truncation_point, iter->second.end());

        /// Remove the remaining segment log files [next_segment_id, ... current segment] and make
        /// current log_file to point the last segment file if necessary
        ++iter;
        if (iter != seg_id_to_entry_offset_map.end())
            return remove(iter->first);
        else
            /// It was the largest rolled segment which got truncated, remove nothing
            /// from the map by passing a large segment id than the latest one.
            /// The remove will remove the current log segment and then make the current log
            /// segment point to the largest rolled segment file by renaming.
            return remove(seg_id_to_entry_offset_map.rbegin()->first + 1);
    }
}

/// trim tail [start_value, ...)
template <typename KeyValue>
int PersistentMonoMap<KeyValue>::trimByValue(ValueType start_value)
{
    LOG_INFO(logger, "Trimming tail {} log starting at {}={}", app, KeyValue::valueName(), start_value);

    if (!current_log_seg_loaded)
        if (auto err = recover(); err != DB::ErrorCodes::OK)
            return err;

    auto res = findFirstEntryByValue_EqualToOrGreaterThan(start_value);
    if (res.hasError() || !res.result.has_value())
        return res.error_code;

    auto trim_log = [](const AppendOnlyFile & segment_file,
                       size_t segment_file_size,
                       ValueType begin_value,
                       KeyValueWithLogOffset & kvo,
                       const std::string & app_,
                       LoggerPtr logger_) {
        assert(kvo.offset <= segment_file_size);
        if (kvo.offset == segment_file_size)
            /// Nothing to truncate because it is beyond the current file size
            return DB::ErrorCodes::OK;

        if (auto err = segment_file.truncate(kvo.offset); err != 0)
        {
            LOG_ERROR(
                logger_,
                "Failed to truncate {} log file={} to {}={} with bytes range [{}, {}] which has {}, error={}",
                app_,
                segment_file.getFilename().c_str(),
                KeyValue::valueName(),
                begin_value,
                kvo.offset,
                segment_file_size,
                kvo.entry.string(),
                errnoToString(err));

            return DB::ErrorCodes::CANNOT_TRUNCATE_FILE;
        }

        LOG_INFO(
            logger_,
            "Truncated {} log file={} to {}={} with bytes range [{}, {}] which has {}",
            app_,
            segment_file.getFilename().c_str(),
            KeyValue::valueName(),
            begin_value,
            kvo.offset,
            segment_file_size,
            kvo.entry.string());

        return DB::ErrorCodes::OK;
    };

    /// We don't use res.result->truncation_point for file truncation since it is not convenient
    /// to calculate
    auto & kvo = res.result->entry_offset;
    if (res.result->segment_id == 0)
    {
        // First trim current log
        if (auto err = trim_log(*log_file, log_size, start_value, kvo, app, logger); err != DB::ErrorCodes::OK)
            return err;

        /// Then trim the cache as well
        if (res.result->cache_to_truncate != &cache)
            return DB::ErrorCodes::LOGICAL_ERROR;

        cache.erase(res.result->truncation_point, cache.end());

        /// Update log size
        log_size = kvo.offset;
        assert((cache.empty() && log_size == 0) || (!cache.empty() && log_size != 0));

        /// When log size becomes zero, we don't gc the current log segment file since it will be used soon
        return DB::ErrorCodes::OK;
    }
    else
    {
        /// Found sn in rolled segment files, then remove all segment files to the current
        /// [segment_id (partial trim), segment_id + 1, current log_file]
        auto iter = seg_id_to_entry_offset_map.find(res.result->segment_id);
        assert(iter != seg_id_to_entry_offset_map.end());

        auto segment_filepath = segmentLogPath(log_file->getFilename(), res.result->segment_id);
        AppendOnlyFile segment_file(
            segment_filepath, /*file_already_exists=*/true, /*read_only_=*/false, /*initial_file_size=*/0, /*preallocate=*/false);

        auto file_size = segment_file.size();
        if (file_size.hasError())
        {
            LOG_ERROR(
                logger, "Failed to get {} file={} size, error={}", app, segment_filepath.c_str(), errnoToString(file_size.error_code));

            return DB::ErrorCodes::CANNOT_FSTAT;
        }

        /// First trim log
        if (auto err = trim_log(segment_file, file_size.result, start_value, kvo, app, logger); err != DB::ErrorCodes::OK)
            return err;

        /// Then trim the cache as well
        if (res.result->cache_to_truncate != &iter->second)
            return DB::ErrorCodes::LOGICAL_ERROR;

        iter->second.erase(res.result->truncation_point, iter->second.end());

        /// Remove the remaining segment log files [next_segment_id, ... current segment] and make
        /// current log_file to point the last segment file if necessary
        ++iter;
        if (iter != seg_id_to_entry_offset_map.end())
            return remove(iter->first);
        else
            /// It was the largest rolled segment which got truncated, remove nothing
            /// from the map by passing a large segment id than the latest one.
            /// The remove will remove the current log segment and then make the current log
            /// segment point to the largest rolled segment file by renaming.
            return remove(seg_id_to_entry_offset_map.rbegin()->first + 1);
    }
}

/// trim head [..., target_key]
/// Best effort, may be trim a prefix sub-range of [..., target_key].
template <typename KeyValue>
int PersistentMonoMap<KeyValue>::trimHead(KeyType target_key)
{
    LOG_INFO(logger, "Trimming head {} log to target {}={}", app, KeyValue::keyName(), target_key);

    if (!current_log_seg_loaded)
        if (auto err = recover(); err != DB::ErrorCodes::OK)
            return err;

    if (!cache.empty() && cache.front().entry.key() <= target_key + 1) /// key is monotonically increased, so +1 is still safe
    {
        /// We can clean all of the rolled log segments, but keep the current active segment
        if (!seg_id_to_entry_offset_map.empty())
        {
            LOG_INFO(logger, "Removing all {} logs for trimming head to target {}={}", app, KeyValue::keyName(), target_key);
            return removeRange(seg_id_to_entry_offset_map.begin(), seg_id_to_entry_offset_map.end());
        }
        return DB::ErrorCodes::OK;
    }
    else
    {
        /// Compared leftLowerBound(key), we like to forward find instead reverse
        /// Find rolled log segments to delete
        auto iter_start = seg_id_to_entry_offset_map.begin();
        auto iter_end = seg_id_to_entry_offset_map.end();
        auto iter = iter_start;
        for (; iter != iter_end; ++iter)
            if (iter->second.front().entry.key() >= target_key)
                break;

        if (iter == iter_start)
            /// Nothing to remove
            return DB::ErrorCodes::OK;

        if (iter == iter_end || iter->second.front().entry.key() > target_key + 1)
        {
            auto prev_iter = iter;
            --prev_iter;

            /// Special case, we didn't find a rolled segment containing target key by using **first key** of the log segs.
            /// However we are not sure if the last log segment contains the target key.
            /// We can check by fully populate / load the last log segment by this may be expensive, so we will skip
            /// the load the last log segment into memory. However if the last log segment is already in memory, then
            /// do the full check.
            /// For example, seg1 [1, 2, 3], seg2 [4, 5, 6], active seg [11, 12], for trimHead(8), we will need find
            /// out if seg2 can be fully GCed. for this example, yes, but if seg2 contains [4, 7, 9], then no.
            if (auto err = loadRolledLogSegmentFully(prev_iter->first, prev_iter->second); err != DB::ErrorCodes::OK)
                return err;

            if (prev_iter->second.back().entry.key() > target_key)
            {
                /// We will need backoff one segment since the last rolled segment contains other entry
                /// which has key > target key, we can't completely garbage collect it.
                --iter;

                if (iter == iter_start)
                    return DB::ErrorCodes::OK;
            }
        }

        LOG_INFO(
            logger,
            "Removing {} logs in segment id range=[{}, {}) for trimming head to target {}={}",
            app,
            iter_start->first,
            (iter != iter_end) ? iter->first : seg_id_to_entry_offset_map.rbegin()->first,
            KeyValue::keyName(),
            target_key);

        return removeRange(iter_start, iter);
    }
}

/// trim head [..., target_value]
/// Best effort, may be trim a prefix sub-range of [..., target_value].
template <typename KeyValue>
int PersistentMonoMap<KeyValue>::trimHeadByValue(ValueType target_value)
{
    LOG_INFO(logger, "Trimming head {} log to target {}={}", app, KeyValue::valueName(), target_value);

    if (!current_log_seg_loaded)
        if (auto err = recover(); err != DB::ErrorCodes::OK)
            return err;

    if (!cache.empty() && cache.front().entry.value() <= target_value + 1) /// value is monotonically increased, so +1 is still safe
    {
        /// We can clean all of the rolled log segments, but keep  the current active segment
        if (!seg_id_to_entry_offset_map.empty())
        {
            LOG_INFO(logger, "Removing all {} logs for trimming head to target {}={}", app, KeyValue::valueName(), target_value);
            return removeRange(seg_id_to_entry_offset_map.begin(), seg_id_to_entry_offset_map.end());
        }
        return DB::ErrorCodes::OK;
    }
    else
    {
        /// Compared leftLowerBound(value), we like to forward find instead reverse
        /// Find rolled log segments to delete
        auto iter_start = seg_id_to_entry_offset_map.begin();
        auto iter_end = seg_id_to_entry_offset_map.end();
        auto iter = iter_start;
        for (; iter != iter_end; ++iter)
            if (iter->second.front().entry.value() >= target_value)
                break;

        if (iter == iter_start)
            /// Nothing to remove
            return DB::ErrorCodes::OK;

        if (iter == iter_end || iter->second.front().entry.value() > target_value + 1)
        {
            auto prev_iter = iter;
            --prev_iter;

            /// Special case, we didn't find a rolled segment containing target value by using `first value` of the log segs.
            /// However we are not sure if the last log segment contains the target value.
            /// We can check by fully populate / load the last log segment by this may be expensive, so we will skip
            /// the load the last log segment into memory. However if the last log segment is already in memory, then
            /// do the full check.
            /// For example, seg1 [1, 2, 3], seg2 [4, 5, 6], active seg [11, 12], for trimHead(8), we will need find
            /// out if seg2 can be fully GCed. for this example, yes, but if seg2 contains [4, 7, 9], then no.
            if (auto err = loadRolledLogSegmentFully(prev_iter->first, prev_iter->second); err != DB::ErrorCodes::OK)
                return err;

            if (prev_iter->second.back().entry.value() > target_value)
            {
                /// We will need step back one segment since the last rolled segment contains other entry
                /// which has sequence > target sn, we can't completely garbage collect it.
                --iter;

                if (iter == iter_start)
                    return DB::ErrorCodes::OK;
            }
        }

        LOG_INFO(
            logger,
            "Removing {} logs in segment id range=[{}, {}) for trimming head to target {}={}",
            app,
            iter_start->first,
            (iter != iter_end) ? iter->first : seg_id_to_entry_offset_map.rbegin()->first,
            KeyValue::valueName(),
            target_value);

        return removeRange(iter_start, iter);
    }
}

/// Remove segments in range [segment_id, ... current segment]
template <typename KeyValue>
int PersistentMonoMap<KeyValue>::remove(std::optional<uint64_t> segment_id)
{
    if (!current_log_seg_loaded)
        if (auto err = recover(); err != DB::ErrorCodes::OK)
            return err;

    std::error_code ec;

    auto iter_start = seg_id_to_entry_offset_map.begin();
    if (segment_id)
        iter_start = seg_id_to_entry_offset_map.lower_bound(segment_id.value());

    if (auto err = removeRange(iter_start, seg_id_to_entry_offset_map.end()); err != DB::ErrorCodes::OK)
        return err;

    LOG_INFO(logger, "Removing {} log file={}", app, log_file->getFilename().c_str());

    log_file->close();
    fs::remove(log_file->getFilename(), ec);
    if (ec.value() != 0)
    {
        LOG_ERROR(logger, "Failed to remove {} log file={}, error={}", app, log_file->getFilename().c_str(), ec.message());
        return DB::ErrorCodes::CANNOT_DELETE_FILE;
    }

    if (!seg_id_to_entry_offset_map.empty())
    {
        auto last_segment_iter = seg_id_to_entry_offset_map.rbegin();
        /// Make log_file to point to the last log segment file
        auto segment_file = std::make_shared<AppendOnlyFile>(
            segmentLogPath(log_file->getFilename(), last_segment_iter->first),
            /*file_already_exists=*/true,
            /*read_only_*/ false,
            /*initial_file_size=*/max_size,
            preallocate);

        segment_file->renameTo(log_file->getFilename(), ec);
        if (ec.value() != 0)
        {
            LOG_ERROR(logger, "Failed to rename {} log file={}, error={}", app, segment_file->getFilename().c_str(), ec.message());
            return DB::ErrorCodes::ATOMIC_RENAME_FAIL;
        }

        log_file.swap(segment_file);
        cache.swap(last_segment_iter->second);

        /// Remove the segment from the map sine it is moved to the current log file
        seg_id_to_entry_offset_map.erase(last_segment_iter->first);
    }
    else
    {
        current_log_seg_loaded = false;
        log_file = std::make_shared<AppendOnlyFile>(log_file->getFilename(), max_size, preallocate);
        log_size = 0;
        cache.clear();
    }

    return DB::ErrorCodes::OK;
}

/// Remove segment range [iter_start, iter_end)
template <typename KeyValue>
int PersistentMonoMap<KeyValue>::removeRange(SegmentIDEntryOffsetMap::iterator iter_start, SegmentIDEntryOffsetMap::iterator iter_end)
{
    std::error_code ec;
    auto iter = iter_start;

    SCOPE_EXIT({ seg_id_to_entry_offset_map.erase(iter_start, iter); });

    for (; iter != iter_end; ++iter)
    {
        auto segfile = segmentLogPath(log_file->getFilename(), iter->first);

        auto file_exists = fs::exists(segfile, ec);
        if (ec.value() != 0)
        {
            LOG_ERROR(logger, "Failed to check if {} log file={} exists, error={}", app, segfile.c_str(), ec.message());
            return DB::ErrorCodes::CANNOT_DELETE_FILE;
        }

        if (file_exists)
        {
            LOG_INFO(logger, "Removing {} log file={}", app, segfile.c_str());

            fs::remove(segfile, ec);
            if (ec.value() != 0)
            {
                LOG_ERROR(logger, "Failed to remove {} log file={}, error={}", app, segfile.c_str(), ec.message());
                return DB::ErrorCodes::CANNOT_DELETE_FILE;
            }
        }
    }

    return DB::ErrorCodes::OK;
}

/// Recover scans the the checkpoint folder to load the latest checkpoint log into memory and
/// In the meanwhile, build entry to log map for old checkpoint logs
/// \return OK if success, otherwise other error code
template <typename KeyValue>
int PersistentMonoMap<KeyValue>::recover()
{
    const auto & log_file_name = log_file->getFilename();
    std::string_view filename{log_file_name.c_str()};
    auto ckpt_dir = log_file_name.parent_path();

    if (!fs::exists(ckpt_dir))
    {
        LOG_ERROR(logger, "{} checkpoint directory does not exist: {}", app, ckpt_dir.string());
        return DB::ErrorCodes::DIRECTORY_DOESNT_EXIST;
    }

    if (!fs::is_directory(ckpt_dir))
    {
        LOG_ERROR(logger, "{} checkpoint path is not a directory: {}", app, ckpt_dir.string());
        return DB::ErrorCodes::FILE_DOESNT_EXIST;
    }

    std::vector<fs::path> log_segment_filenames;
    for (const auto & dir_entry : fs::directory_iterator{ckpt_dir})
    {
        if (!dir_entry.is_regular_file())
            continue;

        const auto & current_filepath = dir_entry.path();
        auto current_file = current_filepath.string();
        if (!current_file.starts_with(filename))
            continue;

        if (current_file == filename)
        {
            if (auto res = loadLogSegment(*log_file, cache, app, logger); res.hasError())
                return res.error_code;
            else
                log_size = res.result;

            current_log_seg_loaded = true;
        }
        else
        {
            if (auto err = loadRolledLogSegment(current_filepath, current_file); err != DB::ErrorCodes::OK)
                return err;
        }
    }

    /// Sanity validation
    if (!seg_id_to_entry_offset_map.empty())
    {
        auto iter = seg_id_to_entry_offset_map.begin();
        const auto iter_end = seg_id_to_entry_offset_map.end();
        for (auto iter_prev = iter; iter != iter_end; ++iter)
        {
            if (iter != iter_prev)
            {
                if (iter_prev->second.front().entry >= iter->second.front().entry)
                {
                    LOG_ERROR(
                        logger,
                        "{} log segment_id={} has higher or equal starting {} than segment_id={} which has starting {}",
                        app,
                        iter_prev->first,
                        iter_prev->second.front().entry.string(),
                        iter->first,
                        iter->second.front().entry.string());

                    return DB::ErrorCodes::LOGICAL_ERROR;
                }
            }

            iter_prev = iter;
        }
    }
    else
    {
        LOG_INFO(logger, "{} has no segments found during recovery", app);
    }

    return DB::ErrorCodes::OK;
}

template <typename KeyValue>
int PersistentMonoMap<KeyValue>::loadRolledLogSegmentFully(uint64_t segment_id, EntryContainer & entry_cache)
{
    if (entry_cache.size() > 1)
        /// We assume each segment contains at least more than one entry since segment file is usually at MB size
        return DB::ErrorCodes::OK;

    /// Not fully loaded
    /// Slower path
    /// Has only first entry, load all of the entries
    auto segment_filename = segmentLogPath(log_file->getFilename(), segment_id);
    AppendOnlyFile segment_file(
        segment_filename, /*file_already_exists=*/true, /*read_only_=*/true, /*initial_file_size=*/0, /*preallocate=*/false);
    std::deque<KeyValueWithLogOffset> rolled_cache;
    if (auto res = loadLogSegment(segment_file, rolled_cache, app, logger); res.hasError())
        return res.error_code;

    entry_cache.swap(rolled_cache);

    return DB::ErrorCodes::OK;
}

template <typename KeyValue>
CallResult<uint64_t> PersistentMonoMap<KeyValue>::loadLogSegment(
    const AppendOnlyFile & log_segment, std::deque<KeyValueWithLogOffset> & entry_cache, const std::string & app_, LoggerPtr logger_)
{
    const auto * filename = log_segment.getFilename().c_str();
    /// We like to load the whole file to memory
    auto res = log_segment.size();
    if (res.hasError())
    {
        LOG_ERROR(logger_, "Failed to get the size of the {} log file={}, error={}", app_, filename, errnoToString(res.error_code));
        return {0, DB::ErrorCodes::CANNOT_FSTAT};
    }
    auto bytes = res.result;

    auto entry_size = KeyValue::exactSerializedSize();
    auto num_entries = bytes / entry_size;

    if (auto remaining = bytes % entry_size; remaining)
    {
        if (log_segment.isReadOnly())
        {
            LOG_ERROR(
                logger_,
                "Found partial {} log file={}, total_bytes={} entries={} trailing_bytes={}",
                app_,
                filename,
                bytes,
                num_entries,
                remaining);
        }
        else
        {
            LOG_WARNING(
                logger_,
                "Found partial {} log file={}, total_bytes={} entries={}. Truncating trailing_bytes={}",
                app_,
                filename,
                bytes,
                num_entries,
                remaining);

            if (auto err = log_segment.truncate(num_entries * entry_size); err != 0)
            {
                LOG_ERROR(logger_, "Failed to truncate {} log file={}, error={}", app_, filename, errnoToString(err));
                return {0, DB::ErrorCodes::CANNOT_TRUNCATE_FILE};
            }
        }

        bytes = num_entries * entry_size;
    }
    else
    {
        LOG_INFO(logger_, "Found complete {} log file={}, total_bytes={} entries={}", app_, filename, bytes, num_entries);
    }

    uint64_t file_offset = 0;

    std::vector<char> read_buf;
    read_buf.resize(KeyValue::exactSerializedSize() * std::min<size_t>(num_entries, 65536));

    bool early_exit = false;
    uint64_t read_offset = 0;
    while (read_offset < bytes && !early_exit)
    {
        auto r = log_segment.read(read_buf.data(), read_buf.size(), read_offset);
        if (r.hasError())
        {
            LOG_ERROR(logger_, "Failed to read data from {} log file={}, error={}", app_, filename, errnoToString(r.error_code));
            return {0, DB::ErrorCodes::CANNOT_READ_FILE};
        }

        /// Partial read is not possible since we already handled partial write above
        assert(r.result % entry_size == 0);

        auto num_entries_read = r.result / entry_size;

        /// Parse data
        DB::ReadBufferFromMemory rb{read_buf.data(), r.result};

        for (size_t i = 0; i < num_entries_read; ++i)
        {
            KeyValueWithLogOffset kvo;
            kvo.entry.deserialize(rb, /*version=*/1);
            kvo.offset = file_offset;

            /// Validation
            if (!entry_cache.empty() && (entry_cache.back().entry > kvo.entry))
            {
                LOG_ERROR(
                    logger_,
                    "{} log file={} has out of order entries at index={} with {} and index={} with {}. Truncating tail entries at "
                    "file_offset={} entries_to_truncate={}",
                    app_,
                    filename,
                    entry_cache.size(),
                    entry_cache.back().entry.string(),
                    entry_cache.size() + 1,
                    kvo.entry.string(),
                    file_offset,
                    num_entries - entry_cache.size());

                /// In some cases, file system exhibited very weird behavior which returns garbage data after a hard power-off
                /// In this case, we can safely truncate the tail entries
                /// ref https://github.com/timeplus-io/proton-enterprise/issues/10316
                log_segment.truncate(file_offset);
                early_exit = true;
                break;
            }

            entry_cache.push_back(std::move(kvo));
            file_offset += entry_size;
        }

        read_offset += r.result;
    }

    assert((early_exit && entry_cache.size() < num_entries) || (!early_exit && entry_cache.size() == num_entries));

    LOG_INFO(
        logger_,
        "Loaded {} log file={}, total_entries_expected={} total_entries_loaded={}",
        app_,
        filename,
        num_entries,
        entry_cache.size());

    return {bytes, DB::ErrorCodes::OK};
}

template <typename KeyValue>
int PersistentMonoMap<KeyValue>::loadRolledLogSegment(const fs::path & segment_filepath, const std::string & segment_filename)
{
    std::string_view filename{log_file->getFilename().c_str()};

    /// 1) Parse segment id, postfix is .<segment-id>
    uint64_t segment_id = 0;
    auto [p, ec] = std::from_chars(&segment_filename[filename.size() + 1], &segment_filename[segment_filename.size()], segment_id);
    if (ec != std::errc() || p != &segment_filename[segment_filename.size()] || segment_id == 0)
    {
        LOG_ERROR(logger, "{} log filename={} is invalid", app, segment_filename);
        return DB::ErrorCodes::INCORRECT_FILE_NAME;
    }

    /// 2) Read the first entry
    AppendOnlyFile segment_file(
        segment_filepath, /*file_already_exists=*/true, /*read_only_=*/true, /*initial_file_size=*/0, /*preallocate=*/false);

    auto segment_file_size = segment_file.size();
    if (segment_file_size.hasError())
    {
        LOG_ERROR(
            logger,
            "Can't get the log size for {} log file={}, error={}",
            app,
            segment_filename,
            errnoToString(segment_file_size.error_code));
        return DB::ErrorCodes::CANNOT_FSTAT;
    }

    if (segment_file_size.result < KeyValue::exactSerializedSize())
    {
        LOG_ERROR(logger, "{} log file={} is invalid, too small size {}", app, segment_filename, segment_file_size.result);
        return DB::ErrorCodes::FILE_CORRUPTION;
    }

    std::string entry_data;
    entry_data.resize(KeyValue::exactSerializedSize());
    auto res = segment_file.read(entry_data.data(), entry_data.size(), /*offset=*/0);
    if (res.hasError())
    {
        LOG_ERROR(logger, "Can't read first entry in {} log file={} , error={}", app, segment_filename, errnoToString(res.error_code));
        return DB::ErrorCodes::CANNOT_READ_FILE;
    }

    auto entry = cluster::parse<KeyValue, false>(entry_data, /*version=*/1);
    seg_id_to_entry_offset_map.emplace(segment_id, std::deque<KeyValueWithLogOffset>{1, KeyValueWithLogOffset{entry, /*offset_=*/0}});

    LOG_INFO(logger, "Loaded {} log file={} with first entry: {}", app, segment_filename, entry.string());

    return DB::ErrorCodes::OK;
}

template <typename KeyValue>
int PersistentMonoMap<KeyValue>::roll(const KeyValue & entry, bool sync)
{
    /// .log.<segment-id>
    /// roll the log file
    fs::path filename = log_file->getFilename();
    auto seg_id = nextSegmentID();
    auto new_file = segmentLogPath(filename, seg_id);

    std::error_code ec;
    log_file->renameTo(std::move(new_file), ec);

    if (ec)
    {
        LOG_ERROR(logger, "Failed to roll {} log file to '{}', error={}", app, log_file->getFilename().c_str(), ec.message());
        return DB::ErrorCodes::ATOMIC_RENAME_FAIL;
    }

    if (sync)
    {
        if (auto err = log_file->sync(/*include_metadata=*/true); err != 0)
        {
            LOG_ERROR(logger, "Failed to flush {} log file '{}', error={}", app, log_file->getFilename().c_str(), ec.message());
            return DB::ErrorCodes::CANNOT_FLUSH_FILE;
        }
    }

    LOG_INFO(
        logger,
        "Rolled {} log file to '{}' with file_size={} next log file starts with {}",
        app,
        log_file->getFilename().c_str(),
        log_size,
        entry.string());

    /// Add this rolled log to map
    seg_id_to_entry_offset_map.emplace(seg_id, std::move(cache));

    /// Reset log end offset
    log_size = 0;

    /// Create a new current log file
    log_file = std::make_shared<nlog::AppendOnlyFile>(filename, max_size, preallocate);

    return DB::ErrorCodes::OK;
}

template class PersistentMonoMap<EpochSequence>;
template class PersistentMonoMap<SequencePosition>;
template class PersistentMonoMap<TimestampSequence>;
}
