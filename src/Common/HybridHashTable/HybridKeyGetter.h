#pragma once

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/ColumnsHashingImpl.h>
#include <Common/HybridHashTable/HybridHashTableTemplate.h>

namespace DB
{

/// The implementation of this file largely borrowed from ColumnsHashing and ColumnsHashingImpl
/// We will need port more or just refine them to support LC, nullable etc
template <HybridHashType type, bool nullable>
struct HybridKeyGetter
{
};

template <typename FieldType, bool nullable>
struct HybridKeyGetterOneNumber
{
    explicit HybridKeyGetterOneNumber(ColumnRawPtrsSpan key_columns_)
    {
        if constexpr (nullable)
        {
            const auto * null_column = checkAndGetColumn<ColumnNullable>(key_columns_[0]);
            vec = null_column->getNestedColumnPtr()->getRawData().data();
        }
        else
        {
            vec = key_columns_[0]->getRawData().data();
        }
        vec = key_columns_[0]->getRawData().data();
    }

    ALWAYS_INLINE FieldType getKeyHolder(size_t row) const { return unalignedLoad<FieldType>(vec + row * sizeof(FieldType)); }

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    // Insert the key from the hash table into columns.
    static void insertKeyIntoColumns(FieldType key, std::vector<IColumn *> & key_columns, const Sizes & /*key_sizes*/)
    {
        ColumnFixedSizeHelper * column;
        if constexpr (nullable)
        {
            ColumnNullable & nullable_col = assert_cast<ColumnNullable &>(*key_columns[0]);
            ColumnUInt8 * null_map = assert_cast<ColumnUInt8 *>(&nullable_col.getNullMapColumn());
            null_map->insertDefault();
            column = static_cast<ColumnFixedSizeHelper *>(&nullable_col.getNestedColumn());
        }
        else
        {
            column = static_cast<ColumnFixedSizeHelper *>(key_columns[0]);
        }

        const auto * key_holder = reinterpret_cast<const char *>(&key);
        column->insertRawData<sizeof(FieldType)>(key_holder);
    }

private:
    const FieldType * getKeyData() const { return reinterpret_cast<const FieldType *>(vec); }

    const char * vec = nullptr;
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::key8, nullable> : public HybridKeyGetterOneNumber<uint8_t, nullable>
{
    using KeyType = uint8_t;
    using Base = HybridKeyGetterOneNumber<uint8_t, nullable>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
        : HybridKeyGetterOneNumber<uint8_t, nullable>(key_columns_)
    {
    }
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::key16, nullable> : public HybridKeyGetterOneNumber<uint16_t, nullable>
{
    using KeyType = uint16_t;
    using Base = HybridKeyGetterOneNumber<uint16_t, nullable>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
        : HybridKeyGetterOneNumber<uint16_t, nullable>(key_columns_)
    {
    }
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::key32, nullable> : public HybridKeyGetterOneNumber<uint32_t, nullable>
{
    using KeyType = uint32_t;
    using Base = HybridKeyGetterOneNumber<uint32_t, nullable>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
        : HybridKeyGetterOneNumber<uint32_t, nullable>(key_columns_)
    {
    }
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::key64, nullable> : public HybridKeyGetterOneNumber<uint64_t, nullable>
{
    using KeyType = uint64_t;
    using Base = HybridKeyGetterOneNumber<uint64_t, nullable>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
        : HybridKeyGetterOneNumber<uint64_t, nullable>(key_columns_)
    {
    }
};

template <typename KeyType, bool has_nullable_keys>
struct HybridKeyGetterFixedKeys : private ColumnsHashing::columns_hashing_impl::BaseStateKeysFixed<KeyType, has_nullable_keys>
{
    using Base = ColumnsHashing::columns_hashing_impl::BaseStateKeysFixed<KeyType, has_nullable_keys>;
    HybridKeyGetterFixedKeys(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : Base(key_columns_), key_columns(key_columns_), key_sizes(key_sizes_)
    {
        if (usePreparedKeys(key_sizes_))
        {
            packFixedBatch(key_columns.size(), Base::getActualColumns(), key_sizes, prepared_keys);
        }
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
        else if constexpr (/*!has_low_cardinality && */ !has_nullable_keys && sizeof(KeyType) <= 16)
        {
            /** The task is to "pack" multiple fixed-size fields into single larger Key.
              * Example: pack UInt8, UInt32, UInt16, UInt64 into UInt128 key:
              * [- ---- -- -------- -] - the resulting uint128 key
              *  ^  ^   ^   ^       ^
              *  u8 u32 u16 u64    zero
              *
              * We can do it with the help of SSSE3 shuffle instruction.
              *
              * There will be a mask for every GROUP BY element (keys_size masks in total).
              * Every mask has 16 bytes but only sizeof(Key) bytes are used (other we don't care).
              *
              * Every byte in the mask has the following meaning:
              * - if it is 0..15, take the element at this index from source register and place here in the result;
              * - if it is 0xFF - set the elemend in the result to zero.
              *
              * Example:
              * We want to copy UInt32 to offset 1 in the destination and set other bytes in the destination as zero.
              * The corresponding mask will be: FF, 0, 1, 2, 3, FF, FF, FF, FF, FF, FF, FF, FF, FF, FF, FF
              *
              * The max size of destination is 16 bytes, because we cannot process more with SSSE3.
              *
              * The method is disabled under MSan, because it's allowed
              * to load into SSE register and process up to 15 bytes of uninitialized memory in columns padding.
              * We don't use this uninitialized memory but MSan cannot look "into" the shuffle instruction.
              *
              * 16-bytes masks can be placed overlapping, only first sizeof(Key) bytes are relevant in each mask.
              * We initialize them to 0xFF and then set the needed elements.
              */
            auto keys_size = key_columns.size();
            size_t total_masks_size = sizeof(KeyType) * keys_size + (16 - sizeof(KeyType));
            masks.reset(new uint8_t[total_masks_size]);
            memset(masks.get(), 0xFF, total_masks_size);

            size_t offset = 0;
            for (size_t i = 0; i < keys_size; ++i)
            {
                for (size_t j = 0; j < key_sizes[i]; ++j)
                {
                    masks[i * sizeof(KeyType) + offset] = j;
                    ++offset;
                }
            }

            columns_data.reset(new const char *[keys_size]);

            for (size_t i = 0; i < keys_size; ++i)
                columns_data[i] = Base::getActualColumns()[i]->getRawData().data();
        }
#endif
    }

    ALWAYS_INLINE KeyType getKeyHolder(size_t row) const
    {
        if constexpr (has_nullable_keys)
        {
            auto bitmap = Base::createBitmap(row);
            return packFixed<KeyType>(row, key_columns.size(), Base::getActualColumns(), key_sizes, bitmap);
        }
        else
        {
            if (!prepared_keys.empty())
                return prepared_keys[row];

#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
            if constexpr (sizeof(KeyType) <= 16)
            {
                chassert(/*!has_low_cardinality && */ !has_nullable_keys);
                return packFixedShuffle<KeyType>(columns_data.get(), key_columns.size(), key_sizes.data(), row, masks.get());
            }
#endif
            return packFixed<KeyType>(row, key_columns.size(), Base::getActualColumns(), key_sizes);
        }
    }

    static void insertKeyIntoColumns(const KeyType & key, std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        size_t keys_size = key_columns.size();

        static constexpr auto bitmap_size = has_nullable_keys ? std::tuple_size<KeysNullMap<KeyType>>::value : 0;
        /// In any hash key value, column values to be read start just after the bitmap, if it exists.
        size_t pos = bitmap_size;

        for (size_t i = 0; i < keys_size; ++i)
        {
            IColumn * observed_column;
            ColumnUInt8 * null_map;

            bool column_nullable = false;
            if constexpr (has_nullable_keys)
                column_nullable = isColumnNullable(*key_columns[i]);

            /// If we have a nullable column, get its nested column and its null map.
            if (column_nullable)
            {
                ColumnNullable & nullable_col = assert_cast<ColumnNullable &>(*key_columns[i]);
                observed_column = &nullable_col.getNestedColumn();
                null_map = assert_cast<ColumnUInt8 *>(&nullable_col.getNullMapColumn());
            }
            else
            {
                observed_column = key_columns[i];
                null_map = nullptr;
            }

            bool is_null = false;
            if (column_nullable)
            {
                /// The current column is nullable. Check if the value of the
                /// corresponding key is nullable. Update the null map accordingly.
                size_t bucket = i / 8;
                size_t offset = i % 8;
                UInt8 val = (reinterpret_cast<const UInt8 *>(&key)[bucket] >> offset) & 1;
                null_map->insertValue(val);
                is_null = val == 1;
            }

            if (has_nullable_keys && is_null)
                observed_column->insertDefault();
            else
            {
                size_t size = key_sizes[i];
                observed_column->insertData(reinterpret_cast<const char *>(&key) + pos, size);
                pos += size;
            }
        }
    }

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        if (!usePreparedKeys(key_sizes))
            return {};

        std::vector<IColumn *> new_columns;
        new_columns.reserve(key_columns.size());

        Sizes new_sizes;
        auto fill_size = [&](size_t size) {
            for (size_t i = 0; i < key_sizes.size(); ++i)
            {
                if (key_sizes[i] == size)
                {
                    new_columns.push_back(key_columns[i]);
                    new_sizes.push_back(size);
                }
            }
        };

        fill_size(16);
        fill_size(8);
        fill_size(4);
        fill_size(2);
        fill_size(1);

        key_columns.swap(new_columns);
        return new_sizes;
    }

private:
    static bool usePreparedKeys(SizesSpan key_sizes_)
    {
        if (/*has_low_cardinality ||*/ has_nullable_keys || sizeof(KeyType) > 16)
            return false;

        for (auto size : key_sizes_)
            if (size != 1 && size != 2 && size != 4 && size != 8 && size != 16)
                return false;

        return true;
    }

    ColumnRawPtrsSpan key_columns;
    SizesSpan key_sizes;

    /// SSSE3 shuffle method can be used. Shuffle masks will be calculated and stored here.
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
    std::unique_ptr<uint8_t[]> masks;
    std::unique_ptr<const char *[]> columns_data;
#endif

    PaddedPODArray<KeyType> prepared_keys;
};

template <bool has_nullable_keys>
struct HybridKeyGetter<HybridHashType::keys16, has_nullable_keys> : public HybridKeyGetterFixedKeys<uint16_t, has_nullable_keys>
{
    using KeyType = uint16_t;

    using Base = HybridKeyGetterFixedKeys<uint16_t, has_nullable_keys>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : HybridKeyGetterFixedKeys<uint16_t, has_nullable_keys>(key_columns_, key_sizes_)
    {
    }
};

template <bool has_nullable_keys>
struct HybridKeyGetter<HybridHashType::keys32, has_nullable_keys> : public HybridKeyGetterFixedKeys<uint32_t, has_nullable_keys>
{
    using KeyType = uint32_t;

    using Base = HybridKeyGetterFixedKeys<uint32_t, has_nullable_keys>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : HybridKeyGetterFixedKeys<uint32_t, has_nullable_keys>(key_columns_, key_sizes_)
    {
    }
};

template <bool has_nullable_keys>
struct HybridKeyGetter<HybridHashType::keys64, has_nullable_keys> : public HybridKeyGetterFixedKeys<uint64_t, has_nullable_keys>
{
    using KeyType = uint64_t;

    using Base = HybridKeyGetterFixedKeys<uint64_t, has_nullable_keys>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : HybridKeyGetterFixedKeys<uint64_t, has_nullable_keys>(key_columns_, key_sizes_)
    {
    }
};

template <bool has_nullable_keys>
struct HybridKeyGetter<HybridHashType::keys128, has_nullable_keys> : public HybridKeyGetterFixedKeys<UInt128, has_nullable_keys>
{
    using KeyType = UInt128;

    using Base = HybridKeyGetterFixedKeys<UInt128, has_nullable_keys>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : HybridKeyGetterFixedKeys<UInt128, has_nullable_keys>(key_columns_, key_sizes_)
    {
    }
};

template <bool has_nullable_keys>
struct HybridKeyGetter<HybridHashType::keys256, has_nullable_keys> : public HybridKeyGetterFixedKeys<UInt256, has_nullable_keys>
{
    using KeyType = UInt256;

    using Base = HybridKeyGetterFixedKeys<UInt256, has_nullable_keys>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : HybridKeyGetterFixedKeys<UInt256, has_nullable_keys>(key_columns_, key_sizes_)
    {
    }
};

/// One fixed string key
template <bool nullable>
struct HybridKeyGetter<HybridHashType::key_fixed_string, nullable>
{
    using KeyType = std::string;

    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
    {
        const IColumn * column;
        if constexpr (nullable)
        {
            column = checkAndGetColumn<ColumnNullable>(key_columns_[0])->getNestedColumnPtr().get();
        }
        else
        {
            column = key_columns_[0];
        }
        const ColumnFixedString & column_string = assert_cast<const ColumnFixedString &>(*column);
        n = column_string.getN();
        chars = &column_string.getChars();
    }

    ALWAYS_INLINE std::string getKeyHolder(size_t row) const { return std::string{reinterpret_cast<const char *>(&(*chars)[row * n]), n}; }

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        static_cast<ColumnFixedString *>(key_columns[0])->insertData(key.data(), key.size());
    }

private:
    size_t n;
    const ColumnFixedString::Chars * chars;
};

/// One string key
template <bool nullable>
struct HybridKeyGetter<HybridHashType::key_string, nullable>
{
    using KeyType = std::string;

    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
    {
        const IColumn * column;
        if constexpr (nullable)
            column = checkAndGetColumn<ColumnNullable>(key_columns_[0])->getNestedColumnPtr().get();
        else
            column = key_columns_[0];

        const ColumnString & column_string = assert_cast<const ColumnString &>(*column);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();
    }

    ALWAYS_INLINE std::string getKeyHolder(ssize_t row) const
    {
        return std::string{reinterpret_cast<const char *>(chars + offsets[row - 1]), offsets[row] - offsets[row - 1] - 1};
    }

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        static_cast<ColumnString *>(key_columns[0])->insertData(key.data(), key.size());
    }

private:
    const IColumn::Offset * offsets;
    const UInt8 * chars;
};

template <bool nullable>
struct HybridKeyGetterSerialized
{
    using KeyType = std::string;

    explicit HybridKeyGetterSerialized(ColumnRawPtrsSpan key_columns_) : key_columns(key_columns_), keys_size(key_columns.size())
    {
        row_sizes.reserve(keys_size);

        if constexpr (nullable)
        {
            nested_key_columns.resize(keys_size);
            null_maps.resize(keys_size);
            for (size_t i = 0; i < keys_size; ++i)
            {
                if (const auto * nullable_column = dynamic_cast<const ColumnNullable *>(key_columns[i]))
                {
                    null_maps[i] = nullable_column->getNullMapData().data();
                    nested_key_columns[i] = nullable_column->getNestedColumnPtr().get();
                }
                else
                {
                    nested_key_columns[i] = key_columns[i];
                }

                nested_key_columns[i]->collectSerializedValueSizes(row_sizes, null_maps[i]);
            }
        }
        else
        {
            for (size_t i = 0; i < keys_size; ++i)
                key_columns[i]->collectSerializedValueSizes(row_sizes, nullptr);
        }
    }

    ALWAYS_INLINE std::string getKeyHolder(size_t row) const
    {
        std::string serialized;
        serialized.resize(row_sizes[row]);
        char * memory = serialized.data();

        for (size_t i = 0; i < keys_size; ++i)
        {
            if constexpr (nullable)
                memory = nested_key_columns[i]->serializeValueIntoMemoryWithNull(row, memory, null_maps[i]);
            else
                memory = key_columns[i]->serializeValueIntoMemory(row, memory);
        }

        return serialized;
    }

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        const auto * pos = key.data();
        for (auto & column : key_columns)
            pos = column->deserializeAndInsertFromArena(pos);
    }

protected:
    ColumnRawPtrsSpan key_columns;
    ColumnRawPtrs nested_key_columns;
    std::vector<const UInt8 *> null_maps;
    PaddedPODArray<UInt64> row_sizes;
    size_t keys_size;
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::key_serialized, nullable> : public HybridKeyGetterSerialized<nullable>
{
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
        : HybridKeyGetterSerialized<nullable>(key_columns_)
    {
    }
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::time_bucket_key16_two_level, nullable> : public HybridKeyGetterOneNumber<uint16_t, nullable>
{
    using KeyType = uint16_t;
    using Base = HybridKeyGetterOneNumber<uint16_t, nullable>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
        : HybridKeyGetterOneNumber<uint16_t, nullable>(key_columns_)
    {
    }
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::time_bucket_key32_two_level, nullable> : public HybridKeyGetterOneNumber<uint32_t, nullable>
{
    using KeyType = uint32_t;
    using Base = HybridKeyGetterOneNumber<uint32_t, nullable>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
        : HybridKeyGetterOneNumber<uint32_t, nullable>(key_columns_)
    {
    }
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::time_bucket_key64_two_level, nullable> : public HybridKeyGetterOneNumber<uint64_t, nullable>
{
    using KeyType = uint64_t;
    using Base = HybridKeyGetterOneNumber<uint64_t, nullable>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
        : HybridKeyGetterOneNumber<uint64_t, nullable>(key_columns_)
    {
    }
};

template <bool has_nullable_keys>
struct HybridKeyGetter<HybridHashType::time_bucket_keys32_two_level, has_nullable_keys>
    : public HybridKeyGetterFixedKeys<uint32_t, has_nullable_keys>
{
    using KeyType = uint32_t;

    using Base = HybridKeyGetterFixedKeys<uint32_t, has_nullable_keys>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : HybridKeyGetterFixedKeys<uint32_t, has_nullable_keys>(key_columns_, key_sizes_)
    {
    }
};

template <bool has_nullable_keys>
struct HybridKeyGetter<HybridHashType::time_bucket_keys64_two_level, has_nullable_keys>
    : public HybridKeyGetterFixedKeys<uint64_t, has_nullable_keys>
{
    using KeyType = uint64_t;

    using Base = HybridKeyGetterFixedKeys<uint64_t, has_nullable_keys>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : HybridKeyGetterFixedKeys<uint64_t, has_nullable_keys>(key_columns_, key_sizes_)
    {
    }
};

template <bool has_nullable_keys>
struct HybridKeyGetter<HybridHashType::time_bucket_keys128_two_level, has_nullable_keys>
    : public HybridKeyGetterFixedKeys<UInt128, has_nullable_keys>
{
    using KeyType = UInt128;

    using Base = HybridKeyGetterFixedKeys<UInt128, has_nullable_keys>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : HybridKeyGetterFixedKeys<UInt128, has_nullable_keys>(key_columns_, key_sizes_)
    {
    }
};

template <bool has_nullable_keys>
struct HybridKeyGetter<HybridHashType::time_bucket_keys256_two_level, has_nullable_keys>
    : public HybridKeyGetterFixedKeys<UInt256, has_nullable_keys>
{
    using KeyType = UInt256;

    using Base = HybridKeyGetterFixedKeys<UInt256, has_nullable_keys>;
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan key_sizes_)
        : HybridKeyGetterFixedKeys<UInt256, has_nullable_keys>(key_columns_, key_sizes_)
    {
    }
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::time_bucket_key_serialized_two_level, nullable> : public HybridKeyGetterSerialized<nullable>
{
    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/)
        : HybridKeyGetterSerialized<nullable>(key_columns_)
    {
    }
};

template <bool nullable>
struct HybridKeyGetter<HybridHashType::key_hashed, nullable>
{
    using KeyType = UInt128;

    HybridKeyGetter(ColumnRawPtrsSpan key_columns_, SizesSpan /*key_sizes_*/) : key_columns(key_columns_) { }

    ALWAYS_INLINE KeyType getKeyHolder(size_t row) const { return hash128(row, key_columns.size(), key_columns); }

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(const KeyType & /*key*/, std::vector<IColumn *> & /*key_columns*/, const Sizes & /*key_sizes*/)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "hashed key can't be inserted to columns");
    }

    ColumnRawPtrsSpan key_columns;
};

}
