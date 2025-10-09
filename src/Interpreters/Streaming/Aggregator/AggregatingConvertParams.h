#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/Streaming/Aggregator/AggregatingConvertType.h>

#include <list>
#include <vector>

namespace DB::Streaming
{

using KeyColumns = std::list<Columns>;
using ManyKeyColumns = std::vector<KeyColumns>;

/// ConvertParams is used to as parameter to convert in-memory state in HashTable and aggregate operator to blocks
struct AggregatingConvertParams
{
    const AggregatingConvertType type = AggregatingConvertType::Normal;
    const bool keys_already_sharded = false;
    const bool clear_state = false;

    /// If a data variant has updated data, the corresponding updated_keys at the variant index
    /// will cached the keys
    /// updated_keys.size() == many_data_variants.size()
    /// update_keys[i] is caching the latest keys for many_data_variants[i]
    ManyKeyColumns many_updated_keys;

    /// Cached
    size_t total_keys;

    explicit AggregatingConvertParams(AggregatingConvertType type_) : type(type_), total_keys(0) { }

    AggregatingConvertParams(AggregatingConvertType type_, bool keys_already_sharded_)
        : type(type_), keys_already_sharded(keys_already_sharded_), total_keys(0)
    {
    }

    AggregatingConvertParams(AggregatingConvertType type_, bool keys_already_sharded_, bool clear_state_)
        : type(type_), keys_already_sharded(keys_already_sharded_), clear_state(clear_state_), total_keys(0)
    {
    }

    AggregatingConvertParams(AggregatingConvertType type_, KeyColumns key_columns, bool keys_already_sharded_)
        : type(type_), keys_already_sharded(keys_already_sharded_), many_updated_keys(1, std::move(key_columns)), total_keys(totalKeys())
    {
    }

    AggregatingConvertParams(AggregatingConvertType type_, ManyKeyColumns many_key_columns, bool keys_already_sharded_)
        : type(type_), keys_already_sharded(keys_already_sharded_), many_updated_keys(std::move(many_key_columns)), total_keys(totalKeys())
    {
    }

    bool hasKeyUpdated() const noexcept { return total_keys > 0; }

    void reset()
    {
        total_keys = 0;
        many_updated_keys = {};
    }

private:
    size_t totalKeys() const noexcept
    {
        size_t key_count = 0;
        for (const auto & list_key_columns : many_updated_keys)
            for (const auto & key_cols : list_key_columns)
                key_count += key_cols.back()->size();

        return key_count;
    }
};

}
