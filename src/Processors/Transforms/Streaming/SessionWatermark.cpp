#include "SessionWatermark.h"

#include <Interpreters/Streaming/SessionMap.h>
#include <Common/ProtonCommon.h>
#include "Watermark.h"

#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <base/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{
namespace Streaming
{
SessionWatermark::SessionWatermark(
    WatermarkSettings && watermark_settings_,
    bool proc_time_,
    ExpressionActionsPtr start_actions_,
    ExpressionActionsPtr end_actions_,
    Poco::Logger * log_)
    : HopTumbleBaseWatermark(std::move(watermark_settings_), proc_time_, log_), start_actions(start_actions_), end_actions(end_actions_)
{
    method_chosen = chooseBlockCacheMethod();
    session_map.keys_size = watermark_settings.window_desc->keys.size();
    session_map.key_sizes = key_sizes;
    session_map.init(method_chosen);
}

void SessionWatermark::doProcess(Block & block)
{
    size_t keys_size = watermark_settings.window_desc->keys.size();
    ColumnRawPtrs key_columns(keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
        key_columns[i]
            = block.getByName(watermark_settings.window_desc->argument_names[watermark_settings.window_desc->keys[i]]).column.get();

    /// create '__tp_session_id' column
    MutableColumnPtr col;
    auto rows = block.rows();
    auto data_type = std::make_shared<DataTypeUInt64>();
    col = data_type->createColumn();
    col->reserve(rows);

#define M(NAME) \
    else if (method_chosen == SessionHashMap::Type::NAME) session_map.insertSessionIdIntoColumn(*session_map.NAME, key_columns, col, rows);

    if (false)
    {
    } // NOLINT
    APPLY_FOR_CACHE_VARIANTS(M)
#undef M

    size_t insert_pos = 0;
    block.insert(insert_pos++, {std::move(col), data_type, ProtonConsts::STREAMING_SESSION_ID});

    assert((start_actions && end_actions) || (!start_actions && !end_actions));

    if (start_actions && end_actions)
    {
        Block start_block;
        auto start_required_columns = start_actions->getRequiredColumns();

        if (start_required_columns.empty())
        {
            /// const expression
            start_block.insert(block.getByPosition(0));
        }
        else
        {
            start_block.reserve(start_required_columns.size());

            if (start_required_pos.empty())
            {
                for (auto & name : start_required_columns)
                    start_required_pos.push_back(block.getPositionByName(name));
            }

            for (auto pos : start_required_pos)
                start_block.insert(block.getByPosition(pos));
        }

        start_actions->execute(start_block);
        block.insert(insert_pos++, {start_block.getColumns()[0], std::make_shared<DataTypeBool>(), ProtonConsts::STREAMING_SESSION_START});

        Block end_block;
        auto end_required_columns = end_actions->getRequiredColumns();

        if (end_required_columns.empty())
        {
            /// const expression
            end_block.insert(block.getByPosition(0));
        }
        else
        {
            end_block.reserve(end_required_columns.size());

            if (end_required_pos.empty())
            {
                for (auto & name : end_required_columns)
                    end_required_pos.push_back(block.getPositionByName(name));
            }

            for (auto pos : end_required_pos)
                end_block.insert(block.getByPosition(pos));
        }
        end_actions->execute(end_block);
        block.insert(insert_pos++, {end_block.getColumns()[0], std::make_shared<DataTypeBool>(), ProtonConsts::STREAMING_SESSION_END});
    }
    else
    {
        auto start_data_type = std::make_shared<DataTypeBool>();
        MutableColumnPtr col_start = start_data_type->createColumn();
        col_start->reserve(rows);

        for (size_t i = 0; i < rows; i++)
            col_start->insert(true);

        block.insert(insert_pos++, {std::move(col_start), std::make_shared<DataTypeBool>(), ProtonConsts::STREAMING_SESSION_START});

        auto end_data_type = std::make_shared<DataTypeBool>();
        MutableColumnPtr col_end = end_data_type->createColumn();
        col_end->reserve(rows);

        for (size_t i = 0; i < rows; i++)
            col_end->insert(true);

        block.insert(insert_pos++, {std::move(col_end), std::make_shared<DataTypeBool>(), ProtonConsts::STREAMING_SESSION_END});
    }
}

void SessionWatermark::handleIdlenessWatermark(Block & block)
{
    /// insert '__tp_session_id' column
    auto rows = block.rows();
    {
        size_t insert_pos = 0;
        auto id_data_type = std::make_shared<DataTypeUInt64>();
        MutableColumnPtr col_id = id_data_type->createColumn();
        col_id->reserve(rows);
        block.insert(insert_pos++, {id_data_type, ProtonConsts::STREAMING_SESSION_ID});

        auto begin_data_type = std::make_shared<DataTypeBool>();
        MutableColumnPtr col_begin = begin_data_type->createColumn();
        col_begin->reserve(rows);
        block.insert(insert_pos++, {begin_data_type, ProtonConsts::STREAMING_SESSION_START});

        auto end_data_type = std::make_shared<DataTypeBool>();
        MutableColumnPtr col_end = end_data_type->createColumn();
        col_end->reserve(rows);
        block.insert(insert_pos++, {end_data_type, ProtonConsts::STREAMING_SESSION_END});
    }
    /// TODO: add watermark
}

SessionHashMap::Type SessionWatermark::chooseBlockCacheMethod()
{
    auto keys_size = watermark_settings.window_desc->keys.size();
    /// If no keys. All aggregating to single row.
    if (keys_size == 0)
        return SessionHashMap::Type::without_key;

    /// Check if at least one of the specified keys is nullable.
    DataTypes types_removed_nullable;
    types_removed_nullable.reserve(keys_size);
    bool has_nullable_key = false;
    bool has_low_cardinality = false;

    for (const auto & pos : watermark_settings.window_desc->keys)
    {
        DataTypePtr type = watermark_settings.window_desc->argument_types[pos];

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

    /** Returns ordinary (not two-level) methods, because we start from them.
      * Later, during aggregation process, data may be converted (partitioned) to two-level structure, if cardinality is high.
      */

    size_t keys_bytes = 0;
    size_t num_fixed_contiguous_keys = 0;

    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
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

    if (has_nullable_key)
    {
        if (keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                return SessionHashMap::Type::nullable_keys128;
            if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
                return SessionHashMap::Type::nullable_keys256;
        }

        if (has_low_cardinality && keys_size == 1)
        {
            if (types_removed_nullable[0]->isValueRepresentedByNumber())
            {
                size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

                if (size_of_field == 1)
                    return SessionHashMap::Type::low_cardinality_key8;
                if (size_of_field == 2)
                    return SessionHashMap::Type::low_cardinality_key16;
                if (size_of_field == 4)
                    return SessionHashMap::Type::low_cardinality_key32;
                if (size_of_field == 8)
                    return SessionHashMap::Type::low_cardinality_key64;
            }
            else if (isString(types_removed_nullable[0]))
                return SessionHashMap::Type::low_cardinality_key_string_hash64;
            else if (isFixedString(types_removed_nullable[0]))
                return SessionHashMap::Type::low_cardinality_key_fixed_string_hash64;
        }

        /// Fallback case.
        return SessionHashMap::Type::serialized;
    }

    /// No key has been found to be nullable.

    /// Single numeric key.
    if (keys_size == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
    {
        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (has_low_cardinality)
        {
            if (size_of_field == 1)
                return SessionHashMap::Type::low_cardinality_key8;
            if (size_of_field == 2)
                return SessionHashMap::Type::low_cardinality_key16;
            if (size_of_field == 4)
                return SessionHashMap::Type::low_cardinality_key32;
            if (size_of_field == 8)
                return SessionHashMap::Type::low_cardinality_key64;
        }

        if (size_of_field == 1)
            return SessionHashMap::Type::key8;
        if (size_of_field == 2)
            return SessionHashMap::Type::key16;
        if (size_of_field == 4)
            return SessionHashMap::Type::key32;
        if (size_of_field == 8)
            return SessionHashMap::Type::key64;
        if (size_of_field == 16)
            return SessionHashMap::Type::keys128;
        if (size_of_field == 32)
            return SessionHashMap::Type::keys256;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
    }

    if (keys_size == 1 && isFixedString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return SessionHashMap::Type::low_cardinality_key_fixed_string_hash64;
        else
            return SessionHashMap::Type::key_fixed_string_hash64;
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (keys_size == num_fixed_contiguous_keys)
    {
        if (has_low_cardinality)
        {
            if (keys_bytes <= 16)
                return SessionHashMap::Type::low_cardinality_keys128;
            if (keys_bytes <= 32)
                return SessionHashMap::Type::low_cardinality_keys256;
        }

        if (keys_bytes <= 2)
            return SessionHashMap::Type::key16;
        if (keys_bytes <= 4)
            return SessionHashMap::Type::key32;
        if (keys_bytes <= 8)
            return SessionHashMap::Type::key64;
        if (keys_bytes <= 16)
            return SessionHashMap::Type::keys128;
        if (keys_bytes <= 32)
            return SessionHashMap::Type::keys256;
    }

    /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
    if (keys_size == 1 && isString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return SessionHashMap::Type::low_cardinality_key_string_hash64;
        else
            return SessionHashMap::Type::key_string_hash64;
    }

    return SessionHashMap::Type::serialized;
}

}
}
