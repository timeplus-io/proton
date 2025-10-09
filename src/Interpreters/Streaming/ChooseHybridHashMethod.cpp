#include <Interpreters/AggregationCommon.h>
#include <Interpreters/Streaming/ChooseHybridHashMethod.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/HybridHashTable/HybridHashTableTemplate.h>
#include <Common/PackedTimeBucket.h>

namespace DB::Streaming
{

namespace
{
std::pair<HybridHashType, size_t> chooseHybridMethodTimeBucket(
    const DataTypes & types_removed_nullable,
    bool has_nullable_key,
    bool has_low_cardinality,
    size_t num_fixed_contiguous_keys,
    const std::vector<size_t> & key_sizes,
    size_t keys_bytes)
{
    /// By default, time bucket is placed at the beginning of the group by columns
    size_t bucket_offset = 0;

    if (has_low_cardinality)
        /// For all low cardinality key column, use serialized
        return {HybridHashType::time_bucket_key_serialized_two_level, bucket_offset};

    /// Fixed key packing ref AggregationCommon.h
    if (has_nullable_key)
    {
        /// Pack if possible all the keys along with information about which key values are nulls
        /// into a fixed 16- or 32-byte blob.
        if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
        {
            bucket_offset = getBitmapSize<UInt128>();
            return {HybridHashType::time_bucket_keys128_two_level, bucket_offset};
        }

        if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
        {
            bucket_offset = getBitmapSize<UInt256>();
            return {HybridHashType::time_bucket_keys256_two_level, bucket_offset};
        }

        /// Fallback case.
        return {HybridHashType::time_bucket_key_serialized_two_level, bucket_offset};
    }

    auto num_keys = types_removed_nullable.size();
    if (num_keys == 1)
    {
        /// Single numeric key, group by window_start or window_end
        assert(!has_nullable_key);
        assert(!has_low_cardinality);

        assert(types_removed_nullable[0]->isValueRepresentedByNumber());

        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (size_of_field == 2)
            return {HybridHashType::time_bucket_key16_two_level, bucket_offset};
        if (size_of_field == 4)
            return {HybridHashType::time_bucket_key32_two_level, bucket_offset};
        if (size_of_field == 8)
            return {HybridHashType::time_bucket_key64_two_level, bucket_offset};

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: the first streaming aggregation column has sizeOfField not in 2, 4, 8.");
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (num_keys == num_fixed_contiguous_keys)
    {
        assert(keys_bytes > 2);

        bucket_offset = timeBucketPackedOffset(key_sizes, keys_bytes);

        if (keys_bytes <= 4)
            return {HybridHashType::time_bucket_keys32_two_level, bucket_offset};
        if (keys_bytes <= 8)
            return {HybridHashType::time_bucket_keys64_two_level, bucket_offset};
        if (keys_bytes <= 16)
            return {HybridHashType::time_bucket_keys128_two_level, bucket_offset};
        if (keys_bytes <= 32)
            return {HybridHashType::time_bucket_keys256_two_level, bucket_offset};
    }

    return {HybridHashType::time_bucket_key_serialized_two_level, bucket_offset};
}

}

HybridHashMethod chooseHybridHashMethod(DataTypes key_column_types, bool needs_time_bucket)
{
    if (key_column_types.empty())
        return {HybridHashType::WithoutKey, {}};

    DataTypes types_removed_nullable;
    types_removed_nullable.reserve(key_column_types.size());
    bool has_nullable_key = false;
    bool has_low_cardinality = false;

    for (auto & type : key_column_types)
    {
        if (type->lowCardinality())
        {
            has_low_cardinality = true;
            type = removeLowCardinality(type);
        }

        if (type->isNullable())
        {
            has_nullable_key = true;
            type = removeNullable(type);
        }

        types_removed_nullable.push_back(type);
    }

    std::vector<size_t> key_sizes;
    key_sizes.resize(key_column_types.size(), 0);

    size_t keys_bytes = 0;
    size_t num_fixed_contiguous_keys = 0;

    for (size_t j = 0; j < key_column_types.size(); ++j)
    {
        if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            {
                ++num_fixed_contiguous_keys;
                key_sizes[j] = types_removed_nullable[j]->getSizeOfValueInMemory();
                keys_bytes += key_sizes[j];
            }
        }
    }

    if (needs_time_bucket)
        return {
            chooseHybridMethodTimeBucket(
                types_removed_nullable, has_nullable_key, has_low_cardinality, num_fixed_contiguous_keys, key_sizes, keys_bytes),
            std::move(key_sizes),
            has_nullable_key,
            has_low_cardinality};

    if (has_low_cardinality)
        /// For all low cardinality key column, use serialized
        return {HybridHashType::key_serialized, std::move(key_sizes), has_nullable_key, has_low_cardinality};

    if (has_nullable_key)
    {
        if (key_column_types.size() == 1)
        {
            if (isFixedString(types_removed_nullable[0]))
                return {HybridHashType::key_fixed_string, std::move(key_sizes), has_nullable_key, has_low_cardinality};

            if (isString(types_removed_nullable[0]))
                return {HybridHashType::key_string, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        }

        return {HybridHashType::key_serialized, std::move(key_sizes), has_nullable_key, has_low_cardinality};
    }

    /// Single numeric key.
    if (key_column_types.size() == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
    {
        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (size_of_field == 1)
            return {HybridHashType::key8, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        if (size_of_field == 2)
            return {HybridHashType::key16, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        if (size_of_field == 4)
            return {HybridHashType::key32, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        if (size_of_field == 8)
            return {HybridHashType::key64, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        if (size_of_field == 16)
            return {HybridHashType::keys128, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        if (size_of_field == 32)
            return {HybridHashType::keys256, std::move(key_sizes), has_nullable_key, has_low_cardinality};

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.");
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (key_sizes.size() == num_fixed_contiguous_keys)
    {
        if (keys_bytes <= 2)
            return {HybridHashType::keys16, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        if (keys_bytes <= 4)
            return {HybridHashType::keys32, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        if (keys_bytes <= 8)
            return {HybridHashType::keys64, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        if (keys_bytes <= 16)
            return {HybridHashType::keys128, std::move(key_sizes), has_nullable_key, has_low_cardinality};
        if (keys_bytes <= 32)
            return {HybridHashType::keys256, std::move(key_sizes), has_nullable_key, has_low_cardinality};
    }

    /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
    if (key_sizes.size() == 1)
    {
        if (isString(types_removed_nullable[0]))
            return {HybridHashType::key_string, std::move(key_sizes), has_nullable_key, has_low_cardinality};

        if (isFixedString(types_removed_nullable[0]))
            return {HybridHashType::key_fixed_string, std::move(key_sizes), has_nullable_key, has_low_cardinality};
    }

    return {HybridHashType::key_serialized, std::move(key_sizes), has_nullable_key, has_low_cardinality};
}

HybridHashMethod chooseHybridHashMethodForJoin(const ColumnRawPtrs & key_columns)
{
    auto keys_size = key_columns.size();
    if (keys_size == 0)
        return HybridHashMethod{HybridHashType::Empty};

    bool all_fixed = true;
    size_t keys_bytes = 0;

    std::vector<size_t> key_sizes;
    key_sizes.resize(keys_size);

    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    /// If there is one numeric key that fits in 64 bits
    if (keys_size == 1 && key_columns[0]->isNumeric())
    {
        size_t size_of_field = key_columns[0]->sizeOfValueIfFixed();
        if (size_of_field == 1)
            return {HybridHashType::key8, std::move(key_sizes)};
        if (size_of_field == 2)
            return {HybridHashType::key16, std::move(key_sizes)};
        if (size_of_field == 4)
            return {HybridHashType::key32, std::move(key_sizes)};
        if (size_of_field == 8)
            return {HybridHashType::key64, std::move(key_sizes)};
        if (size_of_field == 16)
            return {HybridHashType::keys128, std::move(key_sizes)};
        if (size_of_field == 32)
            return {HybridHashType::keys256, std::move(key_sizes)};

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.");
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return {HybridHashType::keys128, std::move(key_sizes)};
    if (all_fixed && keys_bytes <= 32)
        return {HybridHashType::keys256, std::move(key_sizes)};

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1)
    {
        auto is_string_column = [](const IColumn * column_ptr) -> bool {
            if (const auto * lc_column_ptr = typeid_cast<const ColumnLowCardinality *>(column_ptr))
                return typeid_cast<const ColumnString *>(lc_column_ptr->getDictionary().getNestedColumn().get());
            return typeid_cast<const ColumnString *>(column_ptr);
        };

        const auto * key_column = key_columns[0];
        if (is_string_column(key_column)
            || (isColumnConst(*key_column) && is_string_column(assert_cast<const ColumnConst *>(key_column)->getDataColumnPtr().get())))
            return {HybridHashType::key_string, std::move(key_sizes)};
    }

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return {HybridHashType::key_fixed_string, std::move(key_sizes)};

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return {HybridHashType::key_hashed, std::move(key_sizes)};
}

}
