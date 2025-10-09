#pragma once

#include <cstddef>
#include <vector>


namespace DB
{
template <typename Integral>
int64_t decodeTimeBucket(const Integral & packed_key, size_t bucket_bytes, size_t bucket_offset)
{
    //// 0x ... bucket-byte1 bucket-byte2 ...
    return (packed_key >> (8 * bucket_offset)) & ((0xFFull << ((bucket_bytes - 1) << 3)) + ((1ull << ((bucket_bytes - 1) << 3)) - 1));
}

template <typename Integral>
Integral zeroOutTimeBucket(const Integral & packed_key, size_t bucket_bytes, size_t bucket_offset)
{
    Integral mask = ((Integral{1} << (bucket_bytes * 8)) - 1) << (bucket_offset << 3);
    return packed_key & ~mask;
}

size_t timeBucketPackedOffset(const std::vector<size_t> & key_sizes, size_t key_bytes);
}
