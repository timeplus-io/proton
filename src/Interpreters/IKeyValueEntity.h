#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Processors/Chunk.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

/// Interface for entities with key-value semantics.
class IKeyValueEntity
{
public:
    IKeyValueEntity() = default;
    virtual ~IKeyValueEntity() = default;

    /// Get primary key name that supports key-value requests.
    /// Primary key can consist of multiple columns.
    virtual Names getPrimaryKey() const = 0;

    /// proton: starts
    virtual Names getIndexKey([[maybe_unused]] size_t index_id) const { return getPrimaryKey(); }

    virtual bool supportMultipleValuesPerKey() const { return false; }
    /// proton: ends

    /*
     * Get data from storage directly by keys.
     *
     * \param keys - keys to get data for. Key can be compound and represented by several columns.
     * \param out_null_map - output parameter indicating which keys were not found.
     * \param required_columns - if we don't need all attributes, implementation can use it to benefit from reading a subset of them.
     * \param key_index_map - join key index mapping to storage primary key index (position)
     *
     * @return - chunk of data corresponding for keys.
     *   Number of rows in chunk is equal to size of columns in keys.
     *   If the key was not found row would have a default value.
     */
    virtual Chunk getByKeys(
        const ColumnsWithTypeAndName & keys,
        PaddedPODArray<UInt8> & out_null_map,
        const Names & required_columns,
        const std::vector<size_t> & key_index_map,
        bool add_defaults_if_missing) const /// proton updates: add_defaults_if_missing
        = 0;

    /// proton: starts
    /// Different than `getByKeys(...)` which is designed for primary key lookup: one key has one matched row at most,
    /// and for getByKeys(...) if remote returns multiple rows for `getByKeys(...)`, only one row will be honored (the last row);
    /// `getAllByKeys(...) is designed for non-full-primary key range scan (primary key prefix, secondary index etc),
    /// so one key may have multiple rows matched.
    virtual Chunk getAllByKeys(
        const ColumnsWithTypeAndName & /*key_columns_with_types*/,
        PaddedPODArray<size_t> & /*out_values*/,
        const Names & /*required_columns*/,
        const std::vector<size_t> & /*key_index_map*/,
        bool /*add_defaults_if_missing*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Get multiple matched rows by key is not implemented.");
    }
    /// proton: ends

    /// Header for getByKeys result
    virtual Block getSampleBlock(const Names & required_columns) const = 0;

    virtual std::string keyValueType() const = 0;

protected:
    /// Names of result columns. If empty then all columns are required.
    Names key_value_result_names;
};

}
