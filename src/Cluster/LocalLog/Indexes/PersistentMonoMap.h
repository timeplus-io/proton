#pragma once

#include <Cluster/LocalLog/Base/AppendOnlyFile.h>

#include <absl/container/btree_map.h>

#include <deque>

namespace Poco
{
class Logger;
}
using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace cluster::nlog
{
/// PersistentMonoMap persists the mapping of monotonically increased (Key => Value) pair in a sorted way.
/// The key and value both have fixed size length when serialization and both key and value are mono increasing.
/// PersistentMonoMap uses a log based implementation.
/// For example, Raft epoch to sequence number, and sequence to file offset mapping etc scenarios.
/// It is way faster and more memory efficient than RocksDB based implementation because its use
/// cases is way narrowed and its implementation way simpler.
/// The mono map is divided into segment logs and the log entries in each segment log are monotonically
/// increased and the segment logs as a whole are globally monotonically increased.
/// The implementation assumes Key and Value are small and can be efficiently copied around. This is
/// the reason the function signatures accepts by value instead of const-ref.
///
///                                                                   current log segment
/// Tail log segment       seg.1            seg.2            seg   ┌──────────────────────
///          │                                                     │
///          │             ┌────────┐       ┌────────┐      ┌──────▼─┐
///          │             │        │       │        │      │        │
///          └────────────►│        ├──────►│        ├─────►│        │
///                        │        │       │        │      │        │
///                        │        │       │        │      └────────┘
///                        │        │       │        │
///                        └────────┘       └────────┘
///
/// Some key design highlights
/// 1. Each ckpt log segment has bounded size (configurable), 64MB for example. Once a ckpt log segment is rolled, it is immutable.
///    with one exception which is the last several ckpt log segments since we may have tail truncation on them.
/// 2. Old ckpt log segment can be garbage collected at appropriate time. Old ckpt log segment are not kept in memory only loaded when necessary.
/// 3. Usually the last 2 ckpt log segment will be loaded in memory but in a lazy way. First load the last segment and the second last if necessary
///
///  PersistentMonoMap is not thread-safe

/// Template KeyValue is required to be StrictSizeSerializable and has KeyType and ValueType defined.
/// An example is like
/// struct MonoKeyValue
/// {
///     using KeyType = uint64_t;
///     using ValueType = int64_t;
///
///     const KeyType & key() const noexcept;
///     const ValueType & value() const noexcept;
///     size_t exactSerializedSize();
///     void serialize(DB::WriteBuffer & wb, uint16_t version) const;
///     void deserialize(DB::ReadBuffer & rb, uint16_t version);
///     std::string string() const;
///
///     bool operator==(const MonoKeyValue & rhs) const noexcept;
///     bool operator>(const MonoKeyValue & rhs) const noexcept;
///     bool operator<(const MonoKeyValue & rhs) const noexcept;
///     bool operator>=(const MonoKeyValue & rhs) const noexcept;
///     bool operator<=(const MonoKeyValue & rhs) const noexcept;
/// };

/// We introduce "left lower bound" convention for communicating
/// the meaning of function name.
/// If the `key or value` to find sits in the middle, they split the search space
/// into 2 halves: left and right.
/// `left lower bound` is referring a key or value which is the first one and less or equal
/// to the key or value to be searched.
/// In C++, std::lower_bound returns the first element which is ">= target_key"
///         std::upper_bound returns the first element which is "> target_key"
/// leftLowerBound instead returns the first element which is "<= target_key"

template <typename KeyValue>
class PersistentMonoMap final
{
    using KeyType = KeyValue::KeyType;
    using ValueType = KeyValue::ValueType;

    /// Epoch, sequence number and log file offset
    struct KeyValueWithLogOffset
    {
        KeyValueWithLogOffset() : offset(0) { }
        KeyValueWithLogOffset(const KeyValue & entry_, uint64_t offset_) : entry(entry_), offset(offset_) { }

        KeyValue entry;
        uint64_t offset;
    };

    using EntryContainer = std::deque<KeyValueWithLogOffset>;

    struct FoundKeyValue
    {
        FoundKeyValue() = default;
        FoundKeyValue(
            KeyValueWithLogOffset entry_offset_,
            uint64_t segment_id_,
            EntryContainer * cache_to_truncate_,
            EntryContainer::iterator truncation_point_)
            : entry_offset(entry_offset_)
            , segment_id(segment_id_)
            , cache_to_truncate(cache_to_truncate_)
            , truncation_point(truncation_point_)
        {
        }

        KeyValueWithLogOffset entry_offset;
        uint64_t segment_id = 0;

        /// Truncation point is very subtle, need carefully maintain
        EntryContainer * cache_to_truncate = nullptr; /// Help sanity check
        EntryContainer::iterator truncation_point;
    };

public:
    /// \param filename name of log file underlying to persist the entry data
    /// \param file_max_size_ when log file reached this setting, it will get rolled automatically
    PersistentMonoMap(const fs::path & filename, uint64_t file_max_size_, bool preallocate_, std::string app_, LoggerPtr logger_);
    ~PersistentMonoMap() = default;

    /// return true if updated / inserted otherwise false, error code if something wrong happened
    CallResult<bool> maybeIndex(KeyValue entry, bool sync = true);

    /// lowerBound searches the map to find the first key value entry whose key
    /// is equal or great (>=) to the key passed in.
    /// \return the key value if found or an empty result or error code
    CallResult<std::optional<KeyValue>> lowerBound(KeyType key) { return lowerBound<true>(key); }

    /// Find the largest key which is less than or equal to the given key
    CallResult<std::optional<KeyValue>> leftLowerBound(KeyType key) { return leftLowerBound<true>(key); }

    /// lowerBoundByValue searches the map to find the first key value entry whose value
    /// is equal or great (>=) to the value passed in.
    /// \return the key value if found or an empty result or error code
    CallResult<std::optional<KeyValue>> lowerBoundByValue(ValueType value) { return lowerBound<false>(value); }

    /// prevLowerBoundByValue searches the map to find the first key value entry whose value
    /// is less or equal to the value passed in.
    /// \return the key value if found or an empty result or error code
    CallResult<std::optional<KeyValue>> leftLowerBoundByValue(ValueType value) { return leftLowerBound<false>(value); }

    /// \return the last key value if the map is not empty, otherwise an empty result is returned
    CallResult<std::optional<KeyValue>> last();
    /// \return the second latest key value if the map is not empty, otherwise an empty result is returned
    CallResult<std::optional<KeyValue>> prevLast();

    /// Convenient functions
    /// \return the last key if the map is not empty, otherwise an empty result is returned
    CallResult<std::optional<KeyType>> lastKey();
    /// \return the second latest key if the map is not empty, otherwise an empty result is returned
    CallResult<std::optional<KeyType>> prevLastKey();

    /// For testing
    /// \return the first key value if the map is not empty, otherwise an empty result is returned
    CallResult<std::optional<KeyValue>> first();

    /// Truncate tail entries which has key in the entries in range [start_key, ...)
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trim(ValueType start_key);

    /// Truncate head entries which has key in the entries in range [..., target_key]
    /// Best effort, may be trim a prefix sub-range of [..., target_value].
    /// Please note, trimHead never trim the current active log segment,
    /// since trimHead for now it is used for garbage collection
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trimHead(KeyType target_key);

    /// Truncate tail entries which has value in the entries in range [start_value, ...)
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trimByValue(ValueType start_value);

    /// Truncate head entries which has value in the entries in range [..., target_value]
    /// Best effort, may be trim a prefix sub-range of [..., target_value].
    /// Please note, trimHead never trim the current active log segment,
    /// since trimHead for now it is used for garbage collection
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int trimHeadByValue(ValueType target_value);

    /// Delete all all entries
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int remove() { return remove({}); }

    int close() { return log_file->close(); }

private:
    /// \param segment_id if it has value, remove all segment log files which has segment id in [segment_id, current log] ranges
    /// otherwise delete all of the log segments
    /// \return DB::ErrorCodes::OK if success, otherwise other error code
    int remove(std::optional<uint64_t> segment_id);

    int recover();
    /// \return log segment size if success
    static CallResult<uint64_t>
    loadLogSegment(const AppendOnlyFile & log_segment, EntryContainer & entry_cache, const std::string & app_, LoggerPtr logger_);
    int loadRolledLogSegment(const fs::path & segment_filepath, const std::string & segment_filename);
    int loadRolledLogSegmentFully(uint64_t segment_id, EntryContainer & entry_cache);
    int roll(const KeyValue & entry, bool sync);

    using SegmentIDEntryOffsetMap = absl::btree_map<uint64_t, EntryContainer>;
    int removeRange(SegmentIDEntryOffsetMap::iterator iter_start, SegmentIDEntryOffsetMap::iterator iter_end);

    /// \return the first entry whose key or value is >= key or value passed in
    template <bool by_key, typename T>
    CallResult<std::optional<KeyValue>> lowerBound(T t)
    {
        if (!current_log_seg_loaded)
            if (auto err = recover(); err != DB::ErrorCodes::OK)
                return CallResult<std::optional<KeyValue>>{err};

        CallResult<std::optional<FoundKeyValue>> res;

        if constexpr (by_key)
            res = findFirstEntryByKey_EqualToOrGreaterThan(t);
        else
            res = findFirstEntryByValue_EqualToOrGreaterThan(t);

        if (!res.hasError())
        {
            if (res.result.has_value())
                return CallResult{std::optional{res.result->entry_offset.entry}};
            return CallResult{std::optional<KeyValue>{}};
        }
        return CallResult<std::optional<KeyValue>>{res.error_code};
    }

    /// \return the first target key or value which is <= key or value passed in
    template <bool by_key, typename T>
    CallResult<std::optional<KeyValue>> leftLowerBound(T t)
    {
        if (!current_log_seg_loaded)
            if (auto err = recover(); err != DB::ErrorCodes::OK)
                return CallResult<std::optional<KeyValue>>{err};

        CallResult<std::optional<FoundKeyValue>> res;
        if constexpr (by_key)
            res = findFirstEntryByKey_LessThanOrEqualTo(t);
        else
            res = findFirstEntryByValue_LessThanOrEqualTo(t);

        if (!res.hasError())
        {
            if (res.result.has_value())
                return CallResult{std::optional{res.result->entry_offset.entry}};
            return CallResult{std::optional<KeyValue>{}};
        }
        return CallResult<std::optional<KeyValue>>{res.error_code};
    }

    CallResult<std::optional<FoundKeyValue>> findFirstEntryByKey_LessThanOrEqualTo(KeyType key);
    CallResult<std::optional<FoundKeyValue>> findFirstEntryByKey_EqualToOrGreaterThan(KeyType key);

    CallResult<std::optional<FoundKeyValue>> findFirstEntryByValue_LessThanOrEqualTo(ValueType value);
    CallResult<std::optional<FoundKeyValue>> findFirstEntryByValue_EqualToOrGreaterThan(ValueType value);

    uint64_t nextSegmentID() const noexcept
    {
        if (seg_id_to_entry_offset_map.empty())
            /// segment id starts with 1
            return 1;

        return seg_id_to_entry_offset_map.rbegin()->first + 1;
    }

private:
    /// Used by or initialized by which application component. Used in logging
    /// to better differentiate different instantiations
    std::string app;
    uint64_t max_size;
    bool preallocate;

    /// log segment id -> entry cache and offset mapping
    SegmentIDEntryOffsetMap seg_id_to_entry_offset_map;

    /// Point to the latest log file
    AppendOnlyFilePtr log_file;
    /// Cached, then we don't need a sys call to evaluate the log size
    uint64_t log_size = 0;

    bool current_log_seg_loaded = false;

    EntryContainer cache;

    LoggerPtr logger;
};

template <typename KeyValue>
using PersistentMonoMapPtr = std::unique_ptr<PersistentMonoMap<KeyValue>>;
}
