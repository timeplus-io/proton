#pragma once

namespace DB
{

#define APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_COMMON_HYBRID(M) \
    M(key8, false) \
    M(key16, false) \
    M(key32, false) \
    M(key64, false) \
    M(key_fixed_string, false) \
    M(key_string, false) \
    M(keys128, false) \
    M(keys256, false)

#define APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_JOIN_HYBRID(M) \
    APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_COMMON_HYBRID(M) \
    M(key_hashed, false)

#define APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M) \
    APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_COMMON_HYBRID(M) \
    M(keys16, false) \
    M(keys32, false) \
    M(keys64, false) \
    M(key_serialized, false)

#define APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M) \
    M(time_bucket_key16_two_level, true) \
    M(time_bucket_key32_two_level, true) \
    M(time_bucket_key64_two_level, true) \
    M(time_bucket_keys32_two_level, true) \
    M(time_bucket_keys64_two_level, true) \
    M(time_bucket_keys128_two_level, true) \
    M(time_bucket_keys256_two_level, true) \
    M(time_bucket_key_serialized_two_level, true)

#define APPLY_FOR_HASH_KEY_VARIANTS_HYBRID(M) \
    APPLY_FOR_HASH_KEY_VARIANTS_SINGLE_LEVEL_HYBRID(M) \
    APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)

#define APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M) \
    APPLY_FOR_HASH_KEY_VARIANTS_HYBRID(M) \
    M(key_hashed, false)

enum class HybridHashType : uint8_t
{
    Empty = 0,
    WithoutKey,

#define M(NAME, IS_TWO_LEVEL) NAME,
    APPLY_FOR_HASH_KEY_VARIANTS_ALL_HYBRID(M)
#undef M
};

}
