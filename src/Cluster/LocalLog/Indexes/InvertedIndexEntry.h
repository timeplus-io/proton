#pragma once

#include <cinttypes>

namespace cluster::nlog
{
struct InvertedIndexEntry
{
    InvertedIndexEntry() = default;

    InvertedIndexEntry(int64_t key_, int64_t value_) : key(key_), value(value_) { }

    InvertedIndexEntry(const InvertedIndexEntry & rhs)
    {
        key = rhs.key;
        value = rhs.value;
    }

    InvertedIndexEntry & operator=(const InvertedIndexEntry & rhs)
    {
        key = rhs.key;
        value = rhs.value;
        return *this;
    }

    /// bool isValid() const { return key != -1 && value != -1; }

    int64_t key = 0;
    int64_t value = 0;
};

inline bool operator==(const InvertedIndexEntry & lhs, const InvertedIndexEntry & rhs)
{
    return lhs.key == rhs.key && lhs.value == rhs.value;
}

inline bool operator>(const InvertedIndexEntry & lhs, const InvertedIndexEntry & rhs)
{
    return lhs.key > rhs.key;
}
}
