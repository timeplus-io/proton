#pragma once

#include <cinttypes>

namespace nlog
{
struct IndexEntry
{
    IndexEntry(int64_t key_, int64_t value_) : key(key_), value(value_) { }

    IndexEntry(const IndexEntry & rhs)
    {
        key = rhs.key;
        value = rhs.value;
    }

    IndexEntry & operator=(const IndexEntry & rhs)
    {
        key = rhs.key;
        value = rhs.value;
        return *this;
    }

    bool isValid() const { return key != -1 && value != -1; }

    int64_t key;
    int64_t value;
};

using SequencePosition = IndexEntry;
using PositionSequence = IndexEntry;
using TimestampSequence = IndexEntry;

/// static const IndexEntry UNKNOWN_SN_POSITION{-1, -1};
/// static const IndexEntry UNKNOWN_TIMESTAMP_SN{-1, -1};

inline bool operator==(const IndexEntry & lhs, const IndexEntry & rhs)
{
    return lhs.key == rhs.key && lhs.value == rhs.value;
}

inline bool operator>(const IndexEntry & lhs, const IndexEntry & rhs)
{
    return lhs.key > rhs.key;
}
}
