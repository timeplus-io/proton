#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <vector>

namespace DB
{
enum class HybridHashType : uint8_t;

namespace Streaming
{

/// Choose best hash method for key columns
/// \return hash type and key sizes pair
struct HybridHashMethod
{
    explicit HybridHashMethod(HybridHashType type_) : type(type_) { }

    HybridHashMethod(HybridHashType type_, std::vector<size_t> key_sizes_) : type(type_), key_sizes(std::move(key_sizes_)) { }

    HybridHashMethod(HybridHashType type_, std::vector<size_t> key_sizes_, bool has_nullable_key_, bool has_low_cardinality_key_)
        : type(type_)
        , key_sizes(std::move(key_sizes_))
        , has_nullable_key(has_nullable_key_)
        , has_low_cardinality_key(has_low_cardinality_key_)
    {
    }

    HybridHashMethod(
        std::pair<HybridHashType, size_t> type_and_offset,
        std::vector<size_t> key_sizes_,
        bool has_nullable_key_,
        bool has_low_cardinality_key_)
        : type(type_and_offset.first)
        , key_sizes(std::move(key_sizes_))
        , bucket_key_offset(type_and_offset.second)
        , has_nullable_key(has_nullable_key_)
        , has_low_cardinality_key(has_low_cardinality_key_)
    {
    }

    HybridHashType type;
    std::vector<size_t> key_sizes;
    std::optional<size_t> bucket_key_offset = std::nullopt;
    bool has_nullable_key = false;
    bool has_low_cardinality_key = false;
};

HybridHashMethod chooseHybridHashMethod(DataTypes key_column_types, bool need_time_bucket);

HybridHashMethod chooseHybridHashMethodForJoin(const ColumnRawPtrs & key_columns);
}

}
