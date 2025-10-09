#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>

namespace DB::V8
{
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

        auto key_manager = bridge.getKeyManager(*dict_name);
        auto value_converter = bridge.getValueConverter(*dict_name);
        bool is_simple_key = bridge.isDictionarySimpleKey(*dict_name);

        DB::Names required_columns;
        if (args.Length() >= 3 && args[2]->IsArray())
        {
            v8::Local<v8::Array> columns_array = v8::Local<v8::Array>::Cast(args[2]);
            uint32_t columns_length = columns_array->Length();
            required_columns.reserve(columns_length);

            for (uint32_t i = 0; i < columns_length; ++i)
            {
                v8::String::Utf8Value column_name(isolate, columns_array->Get(context, i).ToLocalChecked());
                required_columns.push_back(*column_name);
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
                const Block & sample_block = bridge.getSampleBlock(*dict_name);
                required_columns.insert(required_columns.begin(), sample_block.getByPosition(0).name);
            }
        }

        auto dict_ptr = bridge.getDictionary(*dict_name);

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

            if (key->IsNumber())
            {
                double num_value = key->NumberValue(context).FromJust();
                key_value = static_cast<UInt64>(num_value);
            }
            else if (key->IsString())
            {
                v8::String::Utf8Value str_key(isolate, key);
                key_value = std::stoull(*str_key);
            }
            else
            {
                isolate->ThrowException(
                    v8::String::NewFromUtf8(isolate, "Simple key dictionary requires numeric or string representation of numeric key")
                        .ToLocalChecked());
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

        if (chunk.getNumRows() == 0)
        {
            args.GetReturnValue().SetNull();
            LOG_TRACE(bridge.logger, "Dictionary lookup for '{}' key not found", *dict_name);
            return;
        }

        const auto & columns = chunk.getColumns();

        v8::Local<v8::Object> result = v8::Object::New(isolate);

        for (size_t i = 0; i < required_columns.size(); ++i)
        {
            const auto & col_name = required_columns[i];
            size_t col_index = i;

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

        auto dict_ptr = bridge.getDictionary(*dict_name);
        auto key_manager = bridge.getKeyManager(*dict_name);
        auto value_converter = bridge.getValueConverter(*dict_name);
        bool is_simple_key = bridge.isDictionarySimpleKey(*dict_name);

        const Block & sample_block = bridge.getSampleBlock(*dict_name);
        DB::Names required_columns;

        if (args.Length() >= 3 && args[2]->IsArray())
        {
            v8::Local<v8::Array> columns_array = v8::Local<v8::Array>::Cast(args[2]);
            uint32_t columns_length = columns_array->Length();
            required_columns.reserve(columns_length);

            for (uint32_t i = 0; i < columns_length; ++i)
            {
                v8::String::Utf8Value column_name(isolate, columns_array->Get(context, i).ToLocalChecked());
                required_columns.push_back(*column_name);
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
                required_columns.insert(required_columns.begin(), sample_block.getByPosition(0).name);
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
                v8::Local<v8::Value> key = keys_array->Get(context, i).ToLocalChecked();
                UInt64 key_value = 0;

                if (key->IsNumber())
                {
                    double num_value = key->NumberValue(context).FromJust();
                    key_value = static_cast<UInt64>(num_value);
                }
                else if (key->IsString())
                {
                    v8::String::Utf8Value str_key(isolate, key);
                    key_value = std::stoull(*str_key);
                }
                else
                {
                    isolate->ThrowException(
                        v8::String::NewFromUtf8(isolate, "Simple key dictionary requires numeric or string representation of numeric keys")
                            .ToLocalChecked());
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

        v8::Local<v8::Array> results = v8::Array::New(isolate, keys_count);

        size_t row_index = 0;
        for (uint32_t i = 0; i < keys_count; ++i)
        {
            if (i < null_map.size() && null_map[i])
            {
                v8::Local<v8::Object> row_obj = v8::Object::New(isolate);

                for (size_t col_idx = 0; col_idx < required_columns.size(); ++col_idx)
                {
                    if (col_idx >= columns.size() || row_index >= columns[col_idx]->size())
                        continue;

                    const auto & col_name = required_columns[col_idx];
                    const auto & column = columns[col_idx];

                    v8::Local<v8::Value> js_value = bridge.columnValueToV8(isolate, *column, row_index);
                    row_obj->Set(context, v8::String::NewFromUtf8(isolate, col_name.c_str()).ToLocalChecked(), js_value).Check();
                }

                results->Set(context, i, row_obj).Check();
                row_index++;
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
