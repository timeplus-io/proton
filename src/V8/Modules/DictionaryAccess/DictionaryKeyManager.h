#pragma once

#include <Dictionaries/DictionaryStructure.h>

#include <v8.h>

namespace DB::V8
{

/**
 * DictionaryKeyManager handles all key-related operations for dictionaries,
 * including detection of key type, serialization, and conversion between
 * JavaScript and proton representations.
 */
class DictionaryKeyManager
{
public:
    /**
     * Construct a key manager for a specific dictionary structure
     */
    explicit DictionaryKeyManager(const DictionaryStructure & dict_struct_);

    /**
     * Determines if the dictionary uses a complex key (multiple columns)
     */
    bool isComplexKey() const;

    /**
     * Extracts key columns from JavaScript input and returns appropriate proton columns
     * Supports both string keys (simple) and object keys (complex)
     */
    ColumnsWithTypeAndName extractKeyColumns(v8::Isolate * isolate, v8::Local<v8::Value> js_key) const;

    /**
     * Extract batch key columns from JavaScript arrays
     */
    ColumnsWithTypeAndName extractBatchKeyColumns(v8::Isolate * isolate, v8::Local<v8::Array> js_keys) const;

    /**
     * Serialize a single key in a standard format for the dictionary
     * Works with both simple and complex keys
     */
    String serializeKey(const Block & key_columns, size_t row = 0) const;

    /**
     * Serialize a batch of keys for dictionary operations
     */
    std::vector<String> serializeBatchKeys(const ColumnsWithTypeAndName & key_columns) const;

    /**
     * Build proper key index map based on dictionary structure
     */
    std::vector<size_t> buildKeyIndexMap(const ColumnsWithTypeAndName & key_columns_with_types) const;

    /**
     * Convert JavaScript object to key columns
     * Used for complex keys
     */
    ColumnsWithTypeAndName objectToKeyColumns(v8::Isolate * isolate, v8::Local<v8::Object> js_obj) const;

    /**
     * Extract key names for a complex key dictionary
     */
    Names getComplexKeyColumnNames() const;

private:
    const DictionaryStructure & dict_struct;

    /// Cached information about dictionary keys
    bool has_complex_key;
    Names key_column_names;
    DataTypes key_column_types;
};

}
