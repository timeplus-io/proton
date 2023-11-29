#include <Interpreters/Streaming/RowRefs.h>

#include <Interpreters/Streaming/joinSerder.h>
#include <base/types.h>
#include <Common/ColumnsHashing.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_TYPE_OF_FIELD;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
namespace
{
/// maps enum values to types
template <typename F>
void callWithType(TypeIndex which, F && f)
{
    switch (which)
    {
        case TypeIndex::UInt8:
            return f(UInt8());
        case TypeIndex::UInt16:
            return f(UInt16());
        case TypeIndex::UInt32:
            return f(UInt32());
        case TypeIndex::UInt64:
            return f(UInt64());
        case TypeIndex::Int8:
            return f(Int8());
        case TypeIndex::Int16:
            return f(Int16());
        case TypeIndex::Int32:
            return f(Int32());
        case TypeIndex::Int64:
            return f(Int64());
        case TypeIndex::Float32:
            return f(Float32());
        case TypeIndex::Float64:
            return f(Float64());
        case TypeIndex::Decimal32:
            return f(Decimal32());
        case TypeIndex::Decimal64:
            return f(Decimal64());
        case TypeIndex::Decimal128:
            return f(Decimal128());
        case TypeIndex::DateTime64:
            return f(DateTime64());
        default:
            break;
    }

    UNREACHABLE();
}
}

template <typename DataBlock>
void RowRef<DataBlock>::serialize(const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const
{
    DB::writeIntBinary<UInt32>(serialized_blocks_to_indices.at(reinterpret_cast<std::uintptr_t>(block)), wb);
    DB::writeBinary(row_num, wb);
}

template <typename DataBlock>
void RowRef<DataBlock>::deserialize(const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks, ReadBuffer & rb)
{
    UInt32 block_index;
    DB::readIntBinary<UInt32>(block_index, rb);
    block = &(deserialized_indices_to_blocks.at(block_index)->block);
    DB::readBinary(row_num, rb);
}

template <typename DataBlock>
void RowRefList<DataBlock>::serialize(const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const
{
    /// Row list with same key.
    UInt32 size = 0;
    for (auto it = begin(); it.ok(); ++it)
        ++size;

    /// At least has current one, first one always is itself
    assert(size > 0);

    DB::writeIntBinary<UInt32>(size, wb);

    for (auto it = begin(); it.ok(); ++it)
        (*it)->serialize(serialized_blocks_to_indices, wb);
}

template <typename DataBlock>
void RowRefList<DataBlock>::deserialize(
    Arena & pool, const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks, ReadBuffer & rb)
{
    UInt32 size;
    DB::readIntBinary<UInt32>(size, rb);
    if (size == 0)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Failed to recover hash join checkpoint. Got an invalid count of row ref list");

    /// First one always is itself.
    RowRefDataBlock::deserialize(deserialized_indices_to_blocks, rb);

    /// Other row list with same key.
    RowRefDataBlock other_row_ref;
    for (UInt32 i = 1; i < size; ++i)
    {
        other_row_ref.deserialize(deserialized_indices_to_blocks, rb);
        insert(std::move(other_row_ref), pool);
    }
}

template <typename DataBlock>
void RowRefWithRefCount<DataBlock>::serialize(const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const
{
    DB::writeIntBinary<UInt32>(serialized_blocks_to_indices.at(reinterpret_cast<std::uintptr_t>(&(block_iter->block))), wb);
    DB::writeBinary(row_num, wb);
}

template <typename DataBlock>
void RowRefWithRefCount<DataBlock>::deserialize(
    RefCountDataBlockList<DataBlock> * block_list,
    const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks,
    ReadBuffer & rb)
{
    blocks = block_list;
    UInt32 block_index;
    DB::readIntBinary<UInt32>(block_index, rb);
    block_iter = deserialized_indices_to_blocks.at(block_index);
    DB::readBinary(row_num, rb);
}

template <typename DataBlock>
AsofRowRefs<DataBlock>::AsofRowRefs(TypeIndex type)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using LookupType = typename Entry<T>::LookupType;
        lookups = std::make_unique<LookupType>();
    };

    callWithType(type, call);
}

template <typename DataBlock>
void AsofRowRefs<DataBlock>::insert(
    TypeIndex type,
    const IColumn & asof_column,
    RefCountDataBlockList<DataBlock> * blocks,
    size_t original_row_num,
    size_t row_num,
    ASOFJoinInequality inequality,
    size_t keep_versions)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using LookupPtr = typename Entry<T>::LookupPtr;

        auto & container = std::get<LookupPtr>(lookups);

        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(original_row_num);
        bool ascending = (inequality == ASOFJoinInequality::Less) || (inequality == ASOFJoinInequality::LessOrEquals);
        container->insert(Entry<T>(key, RowRefDataBlock(blocks, row_num)), ascending, keep_versions);
    };

    callWithType(type, call);
}

template <typename DataBlock>
const typename AsofRowRefs<DataBlock>::RowRefDataBlock *
AsofRowRefs<DataBlock>::findAsof(TypeIndex type, ASOFJoinInequality inequality, const IColumn & asof_column, size_t row_num) const
{
    const RowRefDataBlock * out = nullptr;

    bool ascending = (inequality == ASOFJoinInequality::Less) || (inequality == ASOFJoinInequality::LessOrEquals);
    bool is_strict = (inequality == ASOFJoinInequality::Less) || (inequality == ASOFJoinInequality::Greater);

    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using EntryType = Entry<T>;
        using LookupPtr = typename EntryType::LookupPtr;

        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);
        T key = column.getElement(row_num);
        auto & typed_lookup = std::get<LookupPtr>(lookups);

        if (is_strict)
            out = typed_lookup->upperBound(EntryType(key), ascending);
        else
            out = typed_lookup->lowerBound(EntryType(key), ascending);
    };

    callWithType(type, call);
    return out;
}

std::optional<TypeIndex> getAsofTypeSize(const IColumn & asof_column, size_t & size)
{
    TypeIndex idx = asof_column.getDataType();

    switch (idx)
    {
        case TypeIndex::UInt8:
            size = sizeof(UInt8);
            return idx;
        case TypeIndex::UInt16:
            size = sizeof(UInt16);
            return idx;
        case TypeIndex::UInt32:
            size = sizeof(UInt32);
            return idx;
        case TypeIndex::UInt64:
            size = sizeof(UInt64);
            return idx;
        case TypeIndex::Int8:
            size = sizeof(Int8);
            return idx;
        case TypeIndex::Int16:
            size = sizeof(Int16);
            return idx;
        case TypeIndex::Int32:
            size = sizeof(Int32);
            return idx;
        case TypeIndex::Int64:
            size = sizeof(Int64);
            return idx;
        //case TypeIndex::Int128:
        case TypeIndex::Float32:
            size = sizeof(Float32);
            return idx;
        case TypeIndex::Float64:
            size = sizeof(Float64);
            return idx;
        case TypeIndex::Decimal32:
            size = sizeof(Decimal32);
            return idx;
        case TypeIndex::Decimal64:
            size = sizeof(Decimal64);
            return idx;
        case TypeIndex::Decimal128:
            size = sizeof(Decimal128);
            return idx;
        case TypeIndex::DateTime64:
            size = sizeof(DateTime64);
            return idx;
        default:
            break;
    }

    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "ASOF join not supported for type: {}", asof_column.getFamilyName());
}

template <typename DataBlock>
void AsofRowRefs<DataBlock>::serialize(
    TypeIndex type, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        const auto & sorted_lookup_vec = *std::get<typename Entry<T>::LookupPtr>(lookups);
        DB::writeIntBinary<UInt32>(static_cast<UInt32>(sorted_lookup_vec.size()), wb);
        for (const auto & [asof_value, row_ref] : sorted_lookup_vec)
        {
            /// Key
            DB::writeBinary(asof_value, wb);
            /// Mapped: RowRefWithRefCount
            row_ref.serialize(serialized_blocks_to_indices, wb);
        }
    };

    callWithType(type, call);
}

template <typename DataBlock>
void AsofRowRefs<DataBlock>::deserialize(
    TypeIndex type,
    RefCountDataBlockList<DataBlock> * block_list,
    const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks,
    ReadBuffer & rb)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        lookups = std::make_unique<typename Entry<T>::LookupType>();
        auto & sorted_lookup_vec = *std::get<typename Entry<T>::LookupPtr>(lookups);

        UInt32 vec_size;
        DB::readIntBinary<UInt32>(vec_size, rb);
        sorted_lookup_vec.resize(vec_size);
        for (auto & [asof_value, row_ref] : sorted_lookup_vec)
        {
            /// Key
            DB::readBinary(asof_value, rb);
            /// Mapped: RowRefWithRefCount<DataBlock>
            row_ref.deserialize(block_list, deserialized_indices_to_blocks, rb);
        }
    };

    callWithType(type, call);
}

template <typename DataBlock>
RangeAsofRowRefs<DataBlock>::RangeAsofRowRefs(TypeIndex type)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        lookups = std::make_unique<LookupType<T>>();
    };

    callWithType(type, call);
}

template <typename DataBlock>
void RangeAsofRowRefs<DataBlock>::insert(
    TypeIndex type, const IColumn & asof_column, const DataBlock * block, size_t original_row_num, size_t row_num)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(original_row_num);
        std::get<LookupPtr<T>>(lookups)->emplace(key, RowRefDataBlock(block, row_num));
    };

    callWithType(type, call);
}

template <typename DataBlock>
std::vector<typename RangeAsofRowRefs<DataBlock>::RowRefDataBlock> RangeAsofRowRefs<DataBlock>::findRange(
    TypeIndex type, const RangeAsofJoinContext & range_join_ctx, const IColumn & asof_column, size_t row_num, bool is_left_block) const
{
    std::vector<RowRefDataBlock> results;

    auto call_for_left_block = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(row_num);

        auto & m = std::get<LookupPtr<T>>(lookups);

        /// lower_bound [left_inequality] key - right_key [right_inequality] upper_bound
        /// Example: lower_bound < key - right_key <= upper_bound
        /// => -upper_bound <= right_key - key < -lower_bound
        /// => key - upper_bound <= right_key < key - lower_bound
        /// Find key range : [key - upper_bound, key - lower_bound)

        bool is_right_strict = range_join_ctx.right_inequality == ASOFJoinInequality::Less;

        if constexpr (is_decimal<T>)
            key -= static_cast<typename T::NativeType>(range_join_ctx.upper_bound);
        else
            key -= static_cast<T>(range_join_ctx.upper_bound);

        decltype(m->begin()) lower_iter;
        if (is_right_strict)
            lower_iter = m->upper_bound(key);
        else
            lower_iter = m->lower_bound(key);

        if constexpr (is_decimal<T>)
        {
            key += static_cast<typename T::NativeType>(range_join_ctx.upper_bound); /// restore
            key -= static_cast<typename T::NativeType>(range_join_ctx.lower_bound); /// upper bound
        }
        else
        {
            key += static_cast<T>(range_join_ctx.upper_bound); /// restore
            key -= static_cast<T>(range_join_ctx.lower_bound); /// upper bound
        }

        if (lower_iter == m->end() || lower_iter->first > key)
            /// all keys in the map < key - upper_bound or
            /// all keys in the map > key - lower_bound
            return;

        bool is_left_strict = range_join_ctx.left_inequality == ASOFJoinInequality::Greater;

        /// >= key
        auto upper_iter = m->lower_bound(key);

        if (is_left_strict && upper_iter == m->begin())
            return;

        if (upper_iter == m->end() || is_left_strict || upper_iter->first > key)
            /// We need back one step in these cases
            --upper_iter;

        assert(upper_iter->first >= lower_iter->first);

        do
        {
            results.push_back(lower_iter->second);
        } while (lower_iter++ != upper_iter); /// We need include value at upper_iter, so postfix lower_iter++
    };

    auto call_for_right_block = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(row_num);

        auto & m = std::get<LookupPtr<T>>(lookups);

        /// lower_bound [left_inequality] left_key - key [right_inequality] upper_bound
        /// Example: lower_bound < left_key - key <= upper_bound
        /// key + lower_bound < left_key <= key + upper_bound
        /// Find key range : [key + lower_bound, key + upper_bound)

        bool is_left_strict = range_join_ctx.left_inequality == ASOFJoinInequality::Greater;

        if constexpr (is_decimal<T>)
            key += static_cast<typename T::NativeType>(range_join_ctx.lower_bound);
        else
            key += static_cast<T>(range_join_ctx.lower_bound);

        decltype(m->begin()) lower_iter;
        if (is_left_strict)
            lower_iter = m->upper_bound(key);
        else
            lower_iter = m->lower_bound(key);

        if constexpr (is_decimal<T>)
        {
            key -= static_cast<typename T::NativeType>(range_join_ctx.lower_bound); /// restore
            key += static_cast<typename T::NativeType>(range_join_ctx.upper_bound); /// upper bound
        }
        else
        {
            key -= static_cast<T>(range_join_ctx.lower_bound); /// restore
            key += static_cast<T>(range_join_ctx.upper_bound); /// upper bound
        }

        if (lower_iter == m->end() || lower_iter->first > key)
            /// all keys in the map < key + lower_bound or
            /// all keys in the map > key + upper_bound
            return;

        bool is_right_strict = range_join_ctx.right_inequality == ASOFJoinInequality::Less;

        /// >= key
        auto upper_iter = m->lower_bound(key);

        if (is_right_strict && upper_iter == m->begin())
            return;

        if (upper_iter == m->end() || is_right_strict || upper_iter->first > key)
            /// We need back one step in these cases
            --upper_iter;

        assert(upper_iter->first >= lower_iter->first);

        do
        {
            results.push_back(lower_iter->second);
        } while (lower_iter++ != upper_iter); /// We need include value at upper_iter, so postfix lower_iter++
    };

    if (is_left_block)
        callWithType(type, call_for_left_block);
    else
        callWithType(type, call_for_right_block);
    return results;
}

template <typename DataBlock>
const typename RangeAsofRowRefs<DataBlock>::RowRefDataBlock * RangeAsofRowRefs<DataBlock>::findAsof(
    TypeIndex type, const RangeAsofJoinContext & range_join_ctx, const IColumn & asof_column, size_t row_num) const
{
    RowRefDataBlock * result = nullptr;

    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        using ColumnType = ColumnVectorOrDecimal<T>;
        const auto & column = typeid_cast<const ColumnType &>(asof_column);

        T key = column.getElement(row_num);

        auto & m = std::get<LookupPtr<T>>(lookups);

        /// lower_bound [left_inequality] key - right_key [right_inequality] upper_bound
        /// Example: lower_bound < key - right_key <= upper_bound
        /// => -upper_bound <= right_key - key < -lower_bound
        /// => key - upper_bound <= right_key < key - lower_bound
        /// Find key range : [key - upper_bound, key - lower_bound)

        bool is_right_strict = range_join_ctx.right_inequality == ASOFJoinInequality::Less;

        if constexpr (is_decimal<T>)
            key -= static_cast<typename T::NativeType>(range_join_ctx.upper_bound);
        else
            key -= static_cast<T>(range_join_ctx.upper_bound);

        decltype(m->begin()) lower_iter;
        if (is_right_strict)
            lower_iter = m->upper_bound(key);
        else
            lower_iter = m->lower_bound(key);

        if (lower_iter == m->end())
            /// all keys in the map < key - upper_bound
            return;

        assert(lower_iter->first >= key);

        if constexpr (is_decimal<T>)
        {
            key += static_cast<typename T::NativeType>(range_join_ctx.upper_bound); // restore
            key -= static_cast<typename T::NativeType>(range_join_ctx.lower_bound);
        }
        else
        {
            key += static_cast<T>(range_join_ctx.upper_bound); // restore
            key -= static_cast<T>(range_join_ctx.lower_bound);
        }

        /// >= key
        auto upper_iter = m->lower_bound(key);

        bool is_left_strict = range_join_ctx.left_inequality == ASOFJoinInequality::Greater;
        if (is_left_strict && upper_iter == m->begin())
            return;

        if (upper_iter == m->end() || is_left_strict)
            --upper_iter;

        assert(upper_iter->first <= key);
        assert(upper_iter->first >= lower_iter->first);

        result = &upper_iter->second;
    };

    callWithType(type, call);
    return result;
}

template <typename DataBlock>
void RangeAsofRowRefs<DataBlock>::serialize(
    TypeIndex type, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb) const
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        const auto & map = *std::get<LookupPtr<T>>(lookups);
        DB::writeIntBinary<UInt32>(static_cast<UInt32>(map.size()), wb);
        for (const auto & [key, mapped] : map)
        {
            /// Key
            DB::writeBinary(key, wb);
            /// Mapped: RowRef<DataBlock>
            mapped.serialize(serialized_blocks_to_indices, wb);
        }
    };

    callWithType(type, call);
}

template <typename DataBlock>
void RangeAsofRowRefs<DataBlock>::deserialize(
    TypeIndex type, const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks, ReadBuffer & rb)
{
    auto call = [&](const auto & t) {
        using T = std::decay_t<decltype(t)>;
        static_assert(!std::is_same_v<T, StringRef>);
        lookups = std::make_unique<LookupType<T>>();
        auto & map = *std::get<LookupPtr<T>>(lookups);

        T key;
        UInt32 map_size;
        DB::readIntBinary<UInt32>(map_size, rb);
        for (size_t i = 0; i < map_size; ++i)
        {
            /// Key
            DB::readBinary(key, rb);
            // assert(!map.contains(key)); // multimap allows multiple same keys
            auto iter = map.emplace(key, RowRefDataBlock{});

            /// Mapped: RowRef<DataBlock>
            iter->second.deserialize(deserialized_indices_to_blocks, rb);
        }
    };

    callWithType(type, call);
}

template <typename DataBlock>
void RowRefListMultiple<DataBlock>::serialize(
    const SerializedBlocksToIndices & serialized_blocks_to_indices,
    WriteBuffer & wb,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices) const
{
    writeIntBinary<UInt32>(static_cast<UInt32>(rows.size()), wb);
    for (const auto & row_ref : rows)
    {
        row_ref.serialize(serialized_blocks_to_indices, wb);

        if (serialized_row_ref_list_multiple_to_indices)
        {
            [[maybe_unused]] auto [_, inserted] = serialized_row_ref_list_multiple_to_indices->emplace(
                reinterpret_cast<std::uintptr_t>(&row_ref), serialized_row_ref_list_multiple_to_indices->size());
            assert(inserted);
        }
    }
}

template <typename DataBlock>
void RowRefListMultiple<DataBlock>::deserialize(
    RefCountDataBlockList<DataBlock> * block_list,
    const DeserializedIndicesToBlocks<DataBlock> & deserialized_indices_to_blocks,
    ReadBuffer & rb,
    DeserializedIndicesToRowRefListMultiple<DataBlock> * deserialized_indices_to_row_ref_list_multiple)
{
    UInt32 rows_size;
    readIntBinary<UInt32>(rows_size, rb);
    rows.resize(rows_size);
    for (auto iter = rows.begin(); iter != rows.end(); ++iter)
    {
        iter->deserialize(block_list, deserialized_indices_to_blocks, rb);

        if (deserialized_indices_to_row_ref_list_multiple)
        {
            [[maybe_unused]] auto [_, inserted] = deserialized_indices_to_row_ref_list_multiple->emplace(
                deserialized_indices_to_row_ref_list_multiple->size(), RowRefListMultipleRef<DataBlock>{this, iter});
            assert(inserted);
        }
    }
}

template <typename DataBlock>
void RowRefListMultipleRef<DataBlock>::serialize(
    const SerializedRowRefListMultipleToIndices & serialized_row_ref_list_multiple_to_indices, WriteBuffer & wb) const
{
    writeIntBinary<UInt32>(serialized_row_ref_list_multiple_to_indices.at(reinterpret_cast<std::uintptr_t>(&*iterator)), wb);
}

template <typename DataBlock>
void RowRefListMultipleRef<DataBlock>::deserialize(
    const DeserializedIndicesToRowRefListMultiple<DataBlock> & deserialized_indices_to_row_ref_list_multiple, ReadBuffer & rb)
{
    UInt32 ref_index;
    readIntBinary<UInt32>(ref_index, rb);
    *this = deserialized_indices_to_row_ref_list_multiple.at(ref_index);
}

/// For HashJoin
template struct RowRef<LightChunkWithTimestamp>;
template struct RowRefList<LightChunkWithTimestamp>;
template struct RowRefWithRefCount<LightChunkWithTimestamp>;
template struct RowRefListMultiple<LightChunkWithTimestamp>;
template struct RowRefListMultipleRef<LightChunkWithTimestamp>;
template class AsofRowRefs<LightChunkWithTimestamp>;
template class RangeAsofRowRefs<LightChunkWithTimestamp>;

/// For ChangelogCovertTransform
template struct RowRefWithRefCount<LightChunk>;

/// For gtests
template class AsofRowRefs<Block>;
}
}
