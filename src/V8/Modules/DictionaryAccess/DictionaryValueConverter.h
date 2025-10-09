#pragma once

#include <Dictionaries/DictionaryStructure.h>

#include <V8/ConvertDataTypes.h>
#include <V8/Utils.h>
#include <v8.h>

namespace Poco
{
class Logger;
}

namespace DB::V8
{

/**
 * Handles conversion between JavaScript values and proton columns
 * for dictionary attributes
 */
class DictionaryValueConverter
{
public:
    /**
     * Construct a value converter for the specified dictionary structure
     */
    explicit DictionaryValueConverter(const DictionaryStructure & dict_struct_);

    /**
     * Convert a JavaScript object to proton columns
     * @param isolate V8 isolate
     * @param value_obj JavaScript object with attribute values
     * @param sample_block Sample block with column types
     * @return Block with columns populated from the JavaScript object
     */
    Block jsObjectToBlock(v8::Isolate * isolate, v8::Local<v8::Object> value_obj, const Block & sample_block) const;

    /**
     * Convert a batch of JavaScript objects to proton columns
     * @param isolate V8 isolate
     * @param values_array Array of JavaScript objects
     * @param sample_block Sample block with column types
     * @return Block with columns populated from the JavaScript objects
     */
    Block jsBatchToBlock(v8::Isolate * isolate, v8::Local<v8::Array> values_array, const Block & sample_block) const;

    /**
     * Convert proton columns to a JavaScript object
     * @param isolate V8 isolate
     * @param columns Source columns
     * @param row Row index to convert
     * @param column_names Names of columns to include
     * @return JavaScript object with column values
     */
    v8::Local<v8::Object> columnsToJsObject(v8::Isolate * isolate, const Columns & columns, size_t row, const Names & column_names) const;

    /**
     * Convert proton columns to an array of JavaScript objects
     * @param isolate V8 isolate
     * @param columns Source columns
     * @param column_names Names of columns to include
     * @return Array of JavaScript objects
     */
    v8::Local<v8::Array> columnsToJsArray(v8::Isolate * isolate, const Columns & columns, const Names & column_names) const;

    /**
     * Get attribute names from dictionary structure
     */
    Names getAttributeNames() const;

    /**
     * Get attribute types from dictionary structure
     */
    DataTypes getAttributeTypes() const;

    /**
     * Extract column value as JavaScript value
     */
    v8::Local<v8::Value> columnValueToJS(v8::Isolate * isolate, const IColumn & column, size_t row) const;

private:
    const DictionaryStructure & dict_struct;
    Names attribute_names;
    DataTypes attribute_types;
    LoggerPtr logger;

    /**
     * Insert a value from JavaScript into a proton column
     */
    bool insertValueIntoColumn(v8::Isolate * isolate, IColumn & column, const DataTypePtr & type, v8::Local<v8::Value> value) const;
};
}
