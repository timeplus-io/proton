#include <V8/Modules/DictionaryAccess/DictionaryKeyManager.h>

#include <V8/Utils.h>

#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <IO/WriteBufferFromString.h>
#include <Core/Block.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace DB::V8
{
DictionaryKeyManager::DictionaryKeyManager(const DictionaryStructure & dict_struct_) : dict_struct(dict_struct_)
{
    has_complex_key = (dict_struct.key.has_value());

    if (has_complex_key)
    {
        /// Complex key - multiple columns form the key
        for (const auto & key_attr : *dict_struct.key)
        {
            key_column_names.push_back(key_attr.name);
            key_column_types.push_back(key_attr.type);
        }
    }
    else if (dict_struct.id)
    {
        /// Simple key - single column (id)
        key_column_names.push_back(dict_struct.id->name);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary doesn't have id or complex key defined");
    }
}

bool DictionaryKeyManager::isComplexKey() const
{
    return has_complex_key;
}

ColumnsWithTypeAndName DictionaryKeyManager::extractKeyColumns(v8::Isolate * isolate, v8::Local<v8::Value> js_key) const
{
    if (js_key->IsString())
    {
        v8::String::Utf8Value utf8_key(isolate, js_key);
        auto key_column = ColumnString::create();
        key_column->insertData(*utf8_key, utf8_key.length());

        return {{std::move(key_column), std::make_shared<DataTypeString>(), key_column_names[0]}};
    }
    else if (js_key->IsObject() && !js_key->IsArray())
    {
        if (!has_complex_key)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Object key provided but dictionary uses simple key");

        return objectToKeyColumns(isolate, js_key->ToObject(isolate->GetCurrentContext()).ToLocalChecked());
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key must be a string or object");
}

ColumnsWithTypeAndName DictionaryKeyManager::extractBatchKeyColumns(v8::Isolate * isolate, v8::Local<v8::Array> js_keys) const
{
    auto context = isolate->GetCurrentContext();
    uint32_t num_keys = js_keys->Length();

    if (num_keys == 0)
        return {};

    /// Check first key to determine strategy(TODO: find a more robust way?)
    v8::Local<v8::Value> first_key = js_keys->Get(context, 0).ToLocalChecked();
    bool using_string_keys = first_key->IsString();
    bool using_object_keys = first_key->IsObject() && !first_key->IsArray();

    if (using_string_keys)
    {
        auto key_column = ColumnString::create();

        for (uint32_t i = 0; i < num_keys; ++i)
        {
            v8::Local<v8::Value> key = js_keys->Get(context, i).ToLocalChecked();
            if (!key->IsString())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "All keys must be of the same type (string)");

            v8::String::Utf8Value utf8_key(isolate, key);
            key_column->insertData(*utf8_key, utf8_key.length());
        }

        return {{std::move(key_column), std::make_shared<DataTypeString>(), key_column_names[0]}};
    }
    else if (using_object_keys)
    {
        if (!has_complex_key)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Object keys provided but dictionary uses simple key");

        ColumnsWithTypeAndName key_columns;
        key_columns.reserve(key_column_names.size());

        for (size_t i = 0; i < key_column_names.size(); ++i)
        {
            key_columns.emplace_back(key_column_types[i]->createColumn(), key_column_types[i], key_column_names[i]);
        }

        for (uint32_t row = 0; row < num_keys; ++row)
        {
            v8::Local<v8::Value> key = js_keys->Get(context, row).ToLocalChecked();
            if (!key->IsObject() || key->IsArray())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "All keys must be of the same type (object)");

            v8::Local<v8::Object> key_obj = key->ToObject(context).ToLocalChecked();

            for (size_t i = 0; i < key_column_names.size(); ++i)
            {
                v8::Local<v8::String> prop_name = v8::String::NewFromUtf8(isolate, key_column_names[i].c_str()).ToLocalChecked();

                if (!key_obj->Has(context, prop_name).FromMaybe(false))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key object missing required property: {}", key_column_names[i]);

                v8::Local<v8::Value> prop_value = key_obj->Get(context, prop_name).ToLocalChecked();

                try
                {
                    DB::V8::insertResult(isolate, const_cast<IColumn &>(*key_columns[i].column), key_column_types[i], prop_value, false);
                }
                catch (const DB::Exception & e)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "Failed to convert key property '{}': {}", key_column_names[i], e.displayText());
                }
            }
        }

        return key_columns;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Keys must be strings or objects");
}

String DictionaryKeyManager::serializeKey(const Block & key_columns, size_t row) const
{
    String result;
    WriteBufferFromString wb(result);

    for (const auto & col : key_columns)
        col.column->serializeValueIntoBuffer(row, wb);

    return result;
}

std::vector<String> DictionaryKeyManager::serializeBatchKeys(const ColumnsWithTypeAndName & key_columns) const
{
    const size_t num_keys = key_columns[0].column->size();
    std::vector<String> result(num_keys);

    for (size_t i = 0; i < num_keys; ++i)
    {
        WriteBufferFromString wb(result[i]);
        for (const auto & col : key_columns)
            col.column->serializeValueIntoBuffer(i, wb);
    }

    return result;
}

std::vector<size_t> DictionaryKeyManager::buildKeyIndexMap(const ColumnsWithTypeAndName & key_columns_with_types) const
{
    std::vector<size_t> result;
    result.reserve(key_column_names.size());

    for (const auto & key_name : key_column_names)
    {
        bool found = false;
        for (size_t i = 0; i < key_columns_with_types.size(); ++i)
        {
            if (key_columns_with_types[i].name == key_name)
            {
                result.push_back(i);
                found = true;
                break;
            }
        }

        if (!found)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Key column '{}' not found in provided columns", key_name);
    }

    return result;
}

ColumnsWithTypeAndName DictionaryKeyManager::objectToKeyColumns(v8::Isolate * isolate, v8::Local<v8::Object> js_obj) const
{
    auto context = isolate->GetCurrentContext();
    ColumnsWithTypeAndName result;
    result.reserve(key_column_names.size());

    for (size_t i = 0; i < key_column_names.size(); ++i)
    {
        MutableColumnPtr column = key_column_types[i]->createColumn();
        v8::Local<v8::String> prop_name = v8::String::NewFromUtf8(isolate, key_column_names[i].c_str()).ToLocalChecked();

        if (!js_obj->Has(context, prop_name).FromMaybe(false))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key object missing required property: {}", key_column_names[i]);

        v8::Local<v8::Value> prop_value = js_obj->Get(context, prop_name).ToLocalChecked();

        try
        {
            DB::V8::insertResult(isolate, *column, key_column_types[i], prop_value, false);
        }
        catch (const DB::Exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to convert key property '{}': {}", key_column_names[i], e.displayText());
        }

        result.emplace_back(std::move(column), key_column_types[i], key_column_names[i]);
    }

    return result;
}

Names DictionaryKeyManager::getComplexKeyColumnNames() const
{
    return key_column_names;
}

}
