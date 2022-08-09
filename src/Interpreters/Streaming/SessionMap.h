#pragma once

#include "Interpreters/Aggregator.h"

#include <functional>
#include <memory>
#include <mutex>

#include <base/logger_useful.h>

#include <base/StringRef.h>
#include <Common/Arena.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/HashTable/TwoLevelHashMap.h>
#include <Common/HashTable/TwoLevelStringHashMap.h>
#include <Common/IntervalKind.h>

#include <Common/ColumnsHashing.h>
#include <Common/ThreadPool.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>

#include <QueryPipeline/SizeLimits.h>

#include <Disks/SingleDiskVolume.h>

#include <Interpreters/AggregateDescription.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/JIT/compileFunction.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>

/// proton: starts
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/StreamingTwoLevelHashMap.h>

#include <numeric>
/// proton: ends

namespace DB
{
enum SessionStatus
{
    END_EXTENDED,
    START_EXTENDED,
    EMIT,
    EMIT_INCLUDED,
    KEEP,
    IGNORE,
};

struct SessionInfo
{
    size_t id = 0;
    Int64 win_start = 0;
    Int64 win_end = 0;
    Int64 scale = 0;
    Int64 interval = 0;
    UInt64 cur_session_id = 0;

    /// When some event meet the start condition, session window is active.
    /// And an event which meet the end condition will make session window inactive.
    /// Session window only accept event when it is active.
    bool active = false;

    String toString() const
    {
        return "( " + std::to_string(cur_session_id) + "," + std::to_string(win_start) + "," + std::to_string(win_end) + ")";
    }
};

struct SessionBlockQueue
{
    SessionInfo session_info;
    std::map<DateTime64, std::vector<size_t>> block_queue;

    SessionInfo & getCurrentSessionInfo() { return session_info; }

    void appendBlock(DateTime64 timestamp, size_t offset)
    {
        auto & cache = block_queue[timestamp];
        cache.emplace_back(offset);
    }

    void removeCurrentAndLateSession(DateTime64 /*session_watermark*/)
    {
        size_t removed = 0;
        for (auto it = block_queue.begin(); it != block_queue.end();)
        {
            if (it->first < session_info.win_start)
            {
                it = block_queue.erase(it);
                removed++;
            }
            else
                break;
        }

        std::cout << "remove rows before " << session_info.win_start << ": " << removed << " rows been removed, " << block_queue.size()
                  << " remains." << std::endl;
    }

    std::vector<size_t> emitCurrentSession(DateTime64 session_watermark)
    {
        std::vector<size_t> blocks;

        for (auto it = block_queue.begin(); it != block_queue.end();)
        {
            if (it->first >= session_info.win_start && it->first <= session_info.win_end)
            {
                blocks.insert(blocks.end(), it->second.begin(), it->second.end());
                it = block_queue.erase(it);
                continue;
            }

            ++it;
        }

        /// start new session
        session_info.win_start = !block_queue.empty() ? block_queue.begin()->first : session_watermark;
        session_info.win_end = session_info.win_start + session_info.interval;
        session_info.cur_session_id++;
        return blocks;
    }
};

using SessionBlockQueuePtr = SessionBlockQueue *;
using SessionInfoPtr = std::shared_ptr<SessionInfo>;

using SessionInfoPtrWithNullStringKeyHash64 = AggregationDataWithNullKey<HashMapWithSavedHash<StringRef, SessionInfoPtr, StringRefHash64>>;
using SessionInfoPtrWithNullFixedStringKeyHash64
    = AggregationDataWithNullKey<HashMapWithSavedHash<StringRef, SessionBlockQueuePtr, StringRefHash64>>;

struct SessionHashMap : private boost::noncopyable
{
    size_t keys_size{}; /// Number of keys. NOTE do we need this field?
    Sizes key_sizes; /// Dimensions of keys, if keys of fixed length

    std::unique_ptr<AggregationMethodOneNumber<UInt8, FixedHashMap<UInt8, SessionBlockQueuePtr>, false>> key8;
    std::unique_ptr<AggregationMethodOneNumber<UInt16, FixedHashMap<UInt16, SessionBlockQueuePtr>, false>> key16;
    std::unique_ptr<AggregationMethodOneNumber<UInt32, HashMap<UInt32, SessionBlockQueuePtr, HashCRC32<UInt32>>, false>> key32;
    std::unique_ptr<AggregationMethodOneNumber<UInt64, HashMap<UInt64, SessionBlockQueuePtr, HashCRC32<UInt64>>, false>> key64;

    std::unique_ptr<AggregationMethodStringNoCache<StringHashMap<SessionBlockQueuePtr>>> key_string;
    std::unique_ptr<AggregationMethodFixedStringNoCache<StringHashMap<SessionBlockQueuePtr>>> key_fixed_string;

    std::unique_ptr<AggregationMethodKeysFixed<FixedImplicitZeroHashMap<UInt16, SessionBlockQueuePtr>, false, false, false>> keys16;
    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt32, SessionBlockQueuePtr, HashCRC32<UInt32>>>> keys32;
    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt64, SessionBlockQueuePtr, HashCRC32<UInt64>>>> keys64;
    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt128, SessionBlockQueuePtr, UInt128HashCRC32>>> keys128;
    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt256, SessionBlockQueuePtr, UInt256HashCRC32>>> keys256;
    std::unique_ptr<AggregationMethodSerialized<HashMapWithSavedHash<StringRef, SessionBlockQueuePtr>>> serialized;

    std::unique_ptr<AggregationMethodOneNumber<UInt64, HashMap<UInt64, SessionBlockQueuePtr, DefaultHash<UInt64>>>> key64_hash64;
    std::unique_ptr<AggregationMethodString<HashMapWithSavedHash<StringRef, SessionBlockQueuePtr, StringRefHash64>>> key_string_hash64;
    std::unique_ptr<AggregationMethodFixedString<HashMapWithSavedHash<StringRef, SessionBlockQueuePtr, StringRefHash64>>>
        key_fixed_string_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt128, SessionBlockQueuePtr, UInt128Hash>>> keys128_hash64;
    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt256, SessionBlockQueuePtr, UInt256Hash>>> keys256_hash64;
    std::unique_ptr<AggregationMethodSerialized<HashMapWithSavedHash<StringRef, SessionBlockQueuePtr, StringRefHash64>>> serialized_hash64;

    /// Support for nullable keys.
    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt128, SessionBlockQueuePtr, UInt128HashCRC32>, true>> nullable_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt256, SessionBlockQueuePtr, UInt256HashCRC32>, true>> nullable_keys256;

    /// Support for low cardinality.
    std::unique_ptr<
        AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false>>>
        low_cardinality_key8;
    std::unique_ptr<
        AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key, false>>>
        low_cardinality_key16;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt64Key>>>
        low_cardinality_key32;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>>>
        low_cardinality_key64;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<SessionInfoPtrWithNullStringKeyHash64>>>
        low_cardinality_key_string_hash64;
    std::unique_ptr<AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<SessionInfoPtrWithNullFixedStringKeyHash64>>>
        low_cardinality_key_fixed_string_hash64;

    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt128, SessionBlockQueuePtr, UInt128HashCRC32>, false, true>>
        low_cardinality_keys128;
    std::unique_ptr<AggregationMethodKeysFixed<HashMap<UInt256, SessionBlockQueuePtr, UInt256HashCRC32>, false, true>>
        low_cardinality_keys256;

    /// no HashTable
    std::map<UInt64, SessionInfoPtr> map64;


/// TODO: Support for nullable keys

/// TODO: Support for low cardinality

/// Different types of keys for caches.
#define APPLY_FOR_CACHE_VARIANTS(M) \
    M(key8) \
    M(key16) \
    M(key32) \
    M(key64) \
    M(keys16) \
    M(keys32) \
    M(keys64) \
    M(keys128) \
    M(keys256) \
    M(serialized) \
    M(key64_hash64) \
    M(key_string_hash64) \
    M(key_fixed_string_hash64) \
    M(keys128_hash64) \
    M(keys256_hash64) \
    M(serialized_hash64) \
    M(nullable_keys128) \
    M(nullable_keys256) \
    M(low_cardinality_key8) \
    M(low_cardinality_key16) \
    M(low_cardinality_key32) \
    M(low_cardinality_key64) \
    M(low_cardinality_key_string_hash64) \
    M(low_cardinality_key_fixed_string_hash64) \
    M(low_cardinality_keys128) \
    M(low_cardinality_keys256)

    enum class Type
    {
        EMPTY = 0,
        without_key,
        map64,
#define M(NAME) NAME,
        APPLY_FOR_CACHE_VARIANTS(M)
#undef M
    };
    Type type = Type::EMPTY;

    void init(Type type_)
    {
        switch (type_)
        {
            case Type::EMPTY:
                break;
            case Type::without_key:
                break;
            case Type::map64:
                break;

#define M(NAME) \
    case Type::NAME: \
        (NAME) = std::make_unique<decltype(NAME)::element_type>(); \
        break;
                APPLY_FOR_CACHE_VARIANTS(M)
#undef M
        }

        type = type_;
        HashMethodContext::Settings cache_settings;
        cache_settings.max_threads = 1;
        method_ctx_ptr = createCache(type_, cache_settings);
    }

    SessionInfoPtr getSessionInfo(UInt64 session_id)
    {
        auto it = map64.find(session_id);
        if (it != map64.end())
            return it->second;

        map64[session_id] = std::make_shared<SessionInfo>();
        return map64[session_id];
    }

    void removeSessionInfo(const std::vector<size_t> & sessions)
    {
        for (const auto & id : sessions)
            map64.erase(id);
    }

    template <typename Method>
    void insertSessionIdIntoColumn(Method & method, ColumnRawPtrs & key_columns, MutableColumnPtr & session_id_column, size_t num_rows)
    {
        typename Method::State state(key_columns, key_sizes, method_ctx_ptr);
        for (size_t i = 0; i < num_rows; i++)
        {
            size_t hash = state.getHash(method.data, i, arena);
            session_id_column->insert(hash);
        }
    }

    HashMethodContextPtr method_ctx_ptr;
    Arena arena;

    static HashMethodContextPtr createCache(Type type, const HashMethodContext::Settings & settings)
    {
        switch (type)
        {
            case Type::without_key:
                return nullptr;
            case Type::map64:
                return nullptr;

#define M(NAME) \
    case Type::NAME: { \
        using TPtr##NAME = decltype(SessionHashMap::NAME); \
        using T##NAME = typename TPtr##NAME ::element_type; \
        return T##NAME ::State::createContext(settings); \
    }
                APPLY_FOR_CACHE_VARIANTS(M)
#undef M

            default:
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }
    }

    size_t size() const
    {
        switch (type)
        {
            case Type::EMPTY:
                return 0;
            case Type::without_key:
                return 1;
            case Type::map64:
                return map64.size();

#define M(NAME) \
    case Type::NAME: \
        return (NAME)->data.size();
                APPLY_FOR_CACHE_VARIANTS(M)
#undef M
        }

        __builtin_unreachable();
    }
};

SessionStatus
handleSession(const DateTime64 & tp_time, SessionInfo & info, IntervalKind::Kind kind, Int64 session_size, Int64 window_interval);
void updateSessionInfo(DateTime64 /*timestamp*/, SessionBlockQueue & queue, size_t session_size, UInt64 window_interval);

}
