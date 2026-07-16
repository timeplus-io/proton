#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>

#include <V8/ExecutionSettings.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <optional>


namespace DB::V8
{
namespace
{
/// The chunk returned by IDictionary::getByKeys consists of the key columns
/// (in key_index_map order) followed by the attributes from required_columns
/// (key names are skipped there). Rebuild that layout so result columns can be
/// matched by name instead of assuming required_columns is position-aligned.
Names buildChunkColumnNames(
    const ColumnsWithTypeAndName & key_columns,
    const std::vector<size_t> & key_index_map,
    const Names & required_columns,
    const DictionaryStructure & dict_struct)
{
    Names chunk_names;
    chunk_names.reserve(key_index_map.size() + required_columns.size());

    for (auto idx : key_index_map)
        chunk_names.push_back(key_columns[idx].name);

    for (const auto & name : required_columns)
        if (dict_struct.hasAttribute(name))
            chunk_names.push_back(name);

    return chunk_names;
}
}
void CacheDictionaryBridge::getCardinality(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::Isolate * isolate = args.GetIsolate();
    try
    {
        if (args.Length() < 1 || !args[0]->IsString())
        {
            isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Dictionary name argument required").ToLocalChecked());
            return;
        }

        v8::String::Utf8Value dict_name(isolate, args[0]);

        CacheDictionaryBridge & bridge = instance();
        if (!bridge.ensureFreshDictionary(*dict_name))
        {
            isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Failed to load dictionary").ToLocalChecked());
            return;
        }

        size_t count = bridge.getDictionary(*dict_name)->getElementCount();
        LOG_TRACE(bridge.logger, "Dictionary '{}' cardinality: {}", *dict_name, count);
        args.GetReturnValue().Set(v8::Number::New(isolate, static_cast<double>(count)));
    }
    catch (const DB::Exception & e)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Error in getCardinality (code={}): {}", e.code(), e.displayText());
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, e.displayText().c_str()).ToLocalChecked());
    }
    catch (...)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Unknown error in getCardinality");
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Unknown error in getCardinality").ToLocalChecked());
    }
}

void CacheDictionaryBridge::getValue(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::Isolate * isolate = args.GetIsolate();
    Stopwatch total_stopwatch;

    try
    {
        if (args.Length() < 2 || !args[0]->IsString())
        {
            isolate->ThrowException(
                v8::String::NewFromUtf8(isolate, "getValue requires dictionary name and key arguments").ToLocalChecked());
            return;
        }

        v8::String::Utf8Value dict_name(isolate, args[0]);
        v8::Local<v8::Value> key = args[1];
        auto context = isolate->GetCurrentContext();

        CacheDictionaryBridge & bridge = instance();

        if (!bridge.ensureFreshDictionary(*dict_name))
        {
            isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Failed to load dictionary").ToLocalChecked());
            return;
        }

        auto snapshot = bridge.getDictionarySnapshot(*dict_name);
        const auto & key_manager = snapshot.key_manager;
        const auto & value_converter = snapshot.value_converter;
        const auto & dict_ptr = snapshot.dictionary;
        bool is_simple_key = snapshot.is_simple_key;

        DB::Names required_columns;
        if (args.Length() >= 3 && args[2]->IsArray())
        {
            std::string projection_error;
            if (!readProjectionColumns(isolate, context, v8::Local<v8::Array>::Cast(args[2]), required_columns, projection_error))
            {
                if (!projection_error.empty())
                    isolate->ThrowException(v8::String::NewFromUtf8(isolate, projection_error.c_str()).ToLocalChecked());
                return;
            }
        }
        else
        {
            required_columns = value_converter->getAttributeNames();

            if (!is_simple_key)
            {
                auto key_columns = key_manager->getComplexKeyColumnNames();
                required_columns.insert(required_columns.begin(), key_columns.begin(), key_columns.end());
            }
            else
            {
                required_columns.insert(required_columns.begin(), snapshot.sample_block.getByPosition(0).name);
            }
        }

        ColumnsWithTypeAndName key_columns;
        std::vector<size_t> key_index_map;

        if (is_simple_key)
        {
            const auto & dict_struct = dict_ptr->getStructure();
            Names id_names = dict_struct.getKeysNames();
            DataTypes id_types = dict_struct.getKeyTypes();

            if (id_names.empty() || id_types.empty())
            {
                isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Dictionary has no key columns defined").ToLocalChecked());
                return;
            }

            const auto & id_name = id_names[0];
            const auto & id_type = id_types[0];

            MutableColumnPtr id_column = id_type->createColumn();
            UInt64 key_value = 0;
            std::string key_error;

            if (!jsSimpleKeyToUInt64(isolate, key, key_value, key_error))
            {
                isolate->ThrowException(v8::String::NewFromUtf8(isolate, key_error.c_str()).ToLocalChecked());
                return;
            }

            id_column->insert(key_value);
            key_columns.emplace_back(std::move(id_column), id_type, id_name);
            key_index_map.push_back(0);
        }
        else
        {
            try
            {
                key_columns = key_manager->extractKeyColumns(isolate, key);
                key_index_map = key_manager->buildKeyIndexMap(key_columns);
            }
            catch (const DB::Exception & e)
            {
                isolate->ThrowException(
                    v8::String::NewFromUtf8(isolate, ("Error preparing key: " + e.displayText()).c_str()).ToLocalChecked());
                return;
            }
        }

        PaddedPODArray<UInt8> null_map;

        auto chunk = dict_ptr->getByKeys(key_columns, null_map, required_columns, key_index_map, true);

        /// With add_defaults_if_missing=true the chunk contains a default-filled
        /// row even for a missing key. Preserve that legacy row by default, and
        /// return null only when the UDF isolate opted into strict miss handling.
        bool found = !null_map.empty() && null_map[0];
        if (chunk.getNumRows() == 0 || (DB::V8::dictionaryGetValueNullOnMissingKey(isolate) && !found))
        {
            args.GetReturnValue().SetNull();
            LOG_TRACE(bridge.logger, "Dictionary lookup for '{}' key not found", *dict_name);
            return;
        }

        const auto & columns = chunk.getColumns();
        Names chunk_names = buildChunkColumnNames(key_columns, key_index_map, required_columns, dict_ptr->getStructure());

        v8::Local<v8::Object> result = v8::Object::New(isolate);

        for (const auto & col_name : required_columns)
        {
            auto it = std::find(chunk_names.begin(), chunk_names.end(), col_name);
            if (it == chunk_names.end())
                continue;

            size_t col_index = static_cast<size_t>(it - chunk_names.begin());
            if (col_index >= columns.size())
                continue;

            const auto & column = columns[col_index];

            if (column->size() == 0)
                continue;

            v8::Local<v8::Value> js_value = bridge.columnValueToV8(isolate, *column, 0);
            result->Set(context, v8::String::NewFromUtf8(isolate, col_name.c_str()).ToLocalChecked(), js_value).Check();
        }

        args.GetReturnValue().Set(result);

        auto total_ms = total_stopwatch.elapsedMilliseconds();
        LOG_TRACE(bridge.logger, "getValue: total={}ms for dictionary '{}'", total_ms, *dict_name);
    }
    catch (const DB::Exception & e)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Error in getValue (code={}): {}", e.code(), e.displayText());
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, e.displayText().c_str()).ToLocalChecked());
    }
    catch (...)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Unknown error in getValue");
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Unknown error in getValue").ToLocalChecked());
    }
}

void CacheDictionaryBridge::batchGetValues(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::Isolate * isolate = args.GetIsolate();
    Stopwatch total_stopwatch;

    try
    {
        if (args.Length() < 2 || !args[0]->IsString() || !args[1]->IsArray())
        {
            isolate->ThrowException(
                v8::String::NewFromUtf8(isolate, "batchGetValues requires dictionary name and keys array").ToLocalChecked());
            return;
        }

        v8::String::Utf8Value dict_name(isolate, args[0]);
        v8::Local<v8::Array> keys_array = v8::Local<v8::Array>::Cast(args[1]);
        uint32_t keys_count = keys_array->Length();
        auto context = isolate->GetCurrentContext();

        if (keys_count == 0)
        {
            args.GetReturnValue().Set(v8::Array::New(isolate, 0));
            return;
        }

        CacheDictionaryBridge & bridge = instance();

        if (!bridge.ensureFreshDictionary(*dict_name))
        {
            isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Failed to load dictionary").ToLocalChecked());
            return;
        }

        auto snapshot = bridge.getDictionarySnapshot(*dict_name);
        const auto & dict_ptr = snapshot.dictionary;
        const auto & key_manager = snapshot.key_manager;
        const auto & value_converter = snapshot.value_converter;
        bool is_simple_key = snapshot.is_simple_key;

        DB::Names required_columns;

        if (args.Length() >= 3 && args[2]->IsArray())
        {
            std::string projection_error;
            if (!readProjectionColumns(isolate, context, v8::Local<v8::Array>::Cast(args[2]), required_columns, projection_error))
            {
                if (!projection_error.empty())
                    isolate->ThrowException(v8::String::NewFromUtf8(isolate, projection_error.c_str()).ToLocalChecked());
                return;
            }
        }
        else
        {
            required_columns = value_converter->getAttributeNames();
            if (!is_simple_key)
            {
                auto key_columns = key_manager->getComplexKeyColumnNames();
                required_columns.insert(required_columns.begin(), key_columns.begin(), key_columns.end());
            }
            else
            {
                /// assuming key is at position 0
                required_columns.insert(required_columns.begin(), snapshot.sample_block.getByPosition(0).name);
            }
        }

        ColumnsWithTypeAndName key_columns;
        std::vector<size_t> key_index_map;

        if (is_simple_key)
        {
            const auto & dict_struct = dict_ptr->getStructure();
            Names id_names = dict_struct.getKeysNames();
            DataTypes id_types = dict_struct.getKeyTypes();

            if (id_names.empty() || id_types.empty())
            {
                isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Dictionary has no key columns defined").ToLocalChecked());
                return;
            }

            const auto & id_name = id_names[0];
            const auto & id_type = id_types[0];

            MutableColumnPtr id_column = id_type->createColumn();
            id_column->reserve(keys_count);

            for (uint32_t i = 0; i < keys_count; ++i)
            {
                v8::Local<v8::Value> key;
                if (!keys_array->Get(context, i).ToLocal(&key))
                    return; /// a property getter threw; the JS exception is pending

                UInt64 key_value = 0;
                std::string key_error;

                if (!jsSimpleKeyToUInt64(isolate, key, key_value, key_error))
                {
                    isolate->ThrowException(v8::String::NewFromUtf8(isolate, key_error.c_str()).ToLocalChecked());
                    return;
                }

                id_column->insert(key_value);
            }

            key_columns.emplace_back(std::move(id_column), id_type, id_name);
            key_index_map.push_back(0);
        }
        else
        {
            try
            {
                key_columns = key_manager->extractBatchKeyColumns(isolate, keys_array);
                key_index_map = key_manager->buildKeyIndexMap(key_columns);
            }
            catch (const DB::Exception & e)
            {
                isolate->ThrowException(
                    v8::String::NewFromUtf8(isolate, ("Error preparing batch keys: " + e.displayText()).c_str()).ToLocalChecked());
                return;
            }
        }

        PaddedPODArray<UInt8> null_map;
        auto chunk = dict_ptr->getByKeys(key_columns, null_map, required_columns, key_index_map, true);

        const auto & columns = chunk.getColumns();
        Names chunk_names = buildChunkColumnNames(key_columns, key_index_map, required_columns, dict_ptr->getStructure());

        /// Pre-resolve the chunk column index for each required column
        std::vector<std::optional<size_t>> column_indexes;
        column_indexes.reserve(required_columns.size());
        for (const auto & col_name : required_columns)
        {
            auto it = std::find(chunk_names.begin(), chunk_names.end(), col_name);
            if (it != chunk_names.end() && static_cast<size_t>(it - chunk_names.begin()) < columns.size())
                column_indexes.emplace_back(static_cast<size_t>(it - chunk_names.begin()));
            else
                column_indexes.emplace_back(std::nullopt);
        }

        v8::Local<v8::Array> results = v8::Array::New(isolate, keys_count);
        bool null_on_missing_key = DB::V8::dictionaryGetValueNullOnMissingKey(isolate);

        /// With add_defaults_if_missing=true the chunk contains one row per input
        /// key (default-filled for missing keys), so row i corresponds to key i.
        for (uint32_t i = 0; i < keys_count; ++i)
        {
            bool has_row = i < chunk.getNumRows();
            bool found = i < null_map.size() && null_map[i];
            if (has_row && (!null_on_missing_key || found))
            {
                v8::Local<v8::Object> row_obj = v8::Object::New(isolate);

                for (size_t j = 0; j < required_columns.size(); ++j)
                {
                    if (!column_indexes[j].has_value())
                        continue;

                    const auto & column = columns[*column_indexes[j]];
                    if (i >= column->size())
                        continue;

                    const auto & col_name = required_columns[j];
                    v8::Local<v8::Value> js_value = bridge.columnValueToV8(isolate, *column, i);
                    row_obj->Set(context, v8::String::NewFromUtf8(isolate, col_name.c_str()).ToLocalChecked(), js_value).Check();
                }

                results->Set(context, i, row_obj).Check();
            }
            else
            {
                results->Set(context, i, v8::Null(isolate)).Check();
            }
        }

        args.GetReturnValue().Set(results);

        auto total_ms = total_stopwatch.elapsedMilliseconds();
        LOG_DEBUG(bridge.logger, "batchGetValues: {} keys, total={}ms for dictionary '{}'", keys_count, total_ms, *dict_name);
    }
    catch (const DB::Exception & e)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Error in batchGetValues (code={}): {}", e.code(), e.displayText());
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, e.displayText().c_str()).ToLocalChecked());
    }
    catch (...)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Unknown error in batchGetValues");
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Unknown error in batchGetValues").ToLocalChecked());
    }
}

}
