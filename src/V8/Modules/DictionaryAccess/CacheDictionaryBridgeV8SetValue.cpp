#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>

#include <Common/logger_useful.h>

namespace DB::V8
{
void CacheDictionaryBridge::setValue(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::Isolate * isolate = args.GetIsolate();
    Stopwatch total_stopwatch;

    try
    {
        if (args.Length() < 3 || !args[0]->IsString() || !args[2]->IsObject())
        {
            isolate->ThrowException(
                v8::String::NewFromUtf8(isolate, "setValue requires dictionary name, key, and value object").ToLocalChecked());
            return;
        }

        v8::String::Utf8Value dict_name(isolate, args[0]);
        v8::Local<v8::Value> key = args[1];
        auto context = isolate->GetCurrentContext();
        v8::Local<v8::Object> value_obj = args[2]->ToObject(context).ToLocalChecked();

        CacheDictionaryBridge & bridge = instance();

        if (!bridge.ensureFreshDictionary(*dict_name))
        {
            isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Failed to load dictionary").ToLocalChecked());
            return;
        }

        auto key_manager = bridge.getKeyManager(*dict_name);
        auto value_converter = bridge.getValueConverter(*dict_name);
        bool is_simple_key = bridge.isDictionarySimpleKey(*dict_name);

        /// Create a copy of the value object to avoid modifying the original
        v8::Local<v8::Object> complete_obj = v8::Object::New(isolate);
        v8::Local<v8::Array> props = value_obj->GetOwnPropertyNames(context).ToLocalChecked();
        uint32_t props_length = props->Length();

        /// Copy all properties from value_obj to complete_obj
        for (uint32_t i = 0; i < props_length; ++i)
        {
            v8::Local<v8::Value> prop_name = props->Get(context, i).ToLocalChecked();
            v8::Local<v8::Value> prop_value = value_obj->Get(context, prop_name).ToLocalChecked();
            complete_obj->Set(context, prop_name, prop_value).Check();
        }

        /// Ensure key fields are added to the value object
        if (!is_simple_key)
        {
            /// For complex keys, extract key fields from the key object
            if (!key->IsObject())
            {
                isolate->ThrowException(
                    v8::String::NewFromUtf8(isolate, "Dictionary keys must be objects for complex key dictionaries").ToLocalChecked());
                return;
            }

            v8::Local<v8::Object> key_obj = key->ToObject(context).ToLocalChecked();
            const auto & key_names = key_manager->getComplexKeyColumnNames();

            for (const auto & key_name : key_names)
            {
                v8::Local<v8::String> prop_name = v8::String::NewFromUtf8(isolate, key_name.c_str()).ToLocalChecked();

                /// Only add key if it doesn't already exist in value_obj
                if (!complete_obj->Has(context, prop_name).FromMaybe(false))
                {
                    if (key_obj->Has(context, prop_name).FromMaybe(false))
                    {
                        v8::Local<v8::Value> key_value = key_obj->Get(context, prop_name).ToLocalChecked();
                        complete_obj->Set(context, prop_name, key_value).Check();
                    }
                    else
                    {
                        isolate->ThrowException(
                            v8::String::NewFromUtf8(isolate, ("Missing required key field: " + key_name).c_str()).ToLocalChecked());
                        return;
                    }
                }
            }
        }
        else
        {
            /// For simple key, set the key field in value_obj if not already present
            const auto & dict_ptr = bridge.getDictionary(*dict_name);
            const auto & dict_struct = dict_ptr->getStructure();
            Names id_names = dict_struct.getKeysNames();

            if (id_names.empty())
            {
                isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Dictionary has no key columns defined").ToLocalChecked());
                return;
            }

            const auto & id_name = id_names[0];
            v8::Local<v8::String> prop_name = v8::String::NewFromUtf8(isolate, id_name.c_str()).ToLocalChecked();

            if (!complete_obj->Has(context, prop_name).FromMaybe(false))
            {
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

                complete_obj->Set(context, prop_name, v8::Number::New(isolate, key_value)).Check();
            }
        }

        /// Convert JS object to block
        const Block & sample_block = bridge.getSampleBlock(*dict_name);
        Block block;
        try
        {
            block = value_converter->jsObjectToBlock(isolate, complete_obj, sample_block);
        }
        catch (const DB::Exception & e)
        {
            isolate->ThrowException(
                v8::String::NewFromUtf8(isolate, ("Error converting value object to block: " + e.displayText()).c_str()).ToLocalChecked());
            return;
        }

        /// Insert into backend storage - use move semantics
        DB::StorageID source_table_id = bridge.getSourceDbAndTable(*dict_name);
        bool insert_success = true;
        try
        {
            bridge.InsertToBackendStorage(std::move(block), source_table_id);
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(bridge.logger, "Error inserting to backend storage: {}", e.displayText());
            insert_success = false;
        }

        args.GetReturnValue().Set(v8::Boolean::New(isolate, insert_success));

        /// Log performance
        auto total_ms = total_stopwatch.elapsedMilliseconds();
        LOG_TRACE(bridge.logger, "setValue completed for dictionary '{}' in {}ms", *dict_name, total_ms);
    }
    catch (const DB::Exception & e)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Error in setValue (code={}): {}", e.code(), e.displayText());
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, e.displayText().c_str()).ToLocalChecked());
    }
    catch (...)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Unknown error in setValue");
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Unknown error in setValue").ToLocalChecked());
    }
}

void CacheDictionaryBridge::batchSetValues(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::Isolate * isolate = args.GetIsolate();
    Stopwatch total_stopwatch;

    try
    {
        if (args.Length() < 3 || !args[0]->IsString() || !args[1]->IsArray() || !args[2]->IsArray())
        {
            isolate->ThrowException(
                v8::String::NewFromUtf8(isolate, "batchSetValues requires dictionary name, keys array, and values array").ToLocalChecked());
            return;
        }

        v8::String::Utf8Value dict_name(isolate, args[0]);
        v8::Local<v8::Array> keys_array = v8::Local<v8::Array>::Cast(args[1]);
        v8::Local<v8::Array> values_array = v8::Local<v8::Array>::Cast(args[2]);
        uint32_t batch_size = keys_array->Length();
        auto context = isolate->GetCurrentContext();

        if (keys_array->Length() != values_array->Length())
        {
            isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Keys and values arrays must have the same length").ToLocalChecked());
            return;
        }

        if (batch_size == 0)
        {
            args.GetReturnValue().Set(v8::Boolean::New(isolate, true));
            return;
        }

        CacheDictionaryBridge & bridge = instance();

        if (!bridge.ensureFreshDictionary(*dict_name))
        {
            isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Failed to load dictionary").ToLocalChecked());
            return;
        }

        auto key_manager = bridge.getKeyManager(*dict_name);
        auto value_converter = bridge.getValueConverter(*dict_name);
        bool is_simple_key = bridge.isDictionarySimpleKey(*dict_name);

        /// Prepare a modified array of value objects with keys added
        v8::Local<v8::Array> complete_values = v8::Array::New(isolate, batch_size);

        /// Get sample block and attribute names for validation
        const Block & sample_block = bridge.getSampleBlock(*dict_name);
        const auto & attribute_names = value_converter->getAttributeNames();
        const auto & dict_ptr = bridge.getDictionary(*dict_name);
        const auto & dict_struct = dict_ptr->getStructure();

        /// Process each key/value pair
        for (uint32_t i = 0; i < batch_size; ++i)
        {
            v8::Local<v8::Value> key = keys_array->Get(context, i).ToLocalChecked();
            v8::Local<v8::Object> value_obj = values_array->Get(context, i).ToLocalChecked()->ToObject(context).ToLocalChecked();

            /// Create a copy of the value object with key fields added
            v8::Local<v8::Object> complete_obj = v8::Object::New(isolate);
            v8::Local<v8::Array> props = value_obj->GetOwnPropertyNames(context).ToLocalChecked();
            uint32_t props_length = props->Length();

            /// Copy all properties from value_obj to complete_obj
            for (uint32_t j = 0; j < props_length; ++j)
            {
                v8::Local<v8::Value> prop_name = props->Get(context, j).ToLocalChecked();
                v8::Local<v8::Value> prop_value = value_obj->Get(context, prop_name).ToLocalChecked();
                complete_obj->Set(context, prop_name, prop_value).Check();
            }

            /// Add key fields to the value object
            if (!is_simple_key)
            {
                if (!key->IsObject())
                {
                    isolate->ThrowException(
                        v8::String::NewFromUtf8(isolate, "Dictionary keys must be objects for complex key dictionaries").ToLocalChecked());
                    return;
                }

                v8::Local<v8::Object> key_obj = key->ToObject(context).ToLocalChecked();
                const auto & key_names = key_manager->getComplexKeyColumnNames();

                for (const auto & key_name : key_names)
                {
                    v8::Local<v8::String> prop_name = v8::String::NewFromUtf8(isolate, key_name.c_str()).ToLocalChecked();

                    if (!complete_obj->Has(context, prop_name).FromMaybe(false))
                    {
                        if (key_obj->Has(context, prop_name).FromMaybe(false))
                        {
                            v8::Local<v8::Value> key_value = key_obj->Get(context, prop_name).ToLocalChecked();
                            complete_obj->Set(context, prop_name, key_value).Check();
                        }
                        else
                        {
                            isolate->ThrowException(
                                v8::String::NewFromUtf8(
                                    isolate, ("Missing required key field in item " + std::to_string(i) + ": " + key_name).c_str())
                                    .ToLocalChecked());
                            return;
                        }
                    }
                }
            }
            else
            {
                /// For simple key dictionary
                Names id_names = dict_struct.getKeysNames();

                if (id_names.empty())
                {
                    isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Dictionary has no key columns defined").ToLocalChecked());
                    return;
                }

                const auto & id_name = id_names[0];
                v8::Local<v8::String> prop_name = v8::String::NewFromUtf8(isolate, id_name.c_str()).ToLocalChecked();

                if (!complete_obj->Has(context, prop_name).FromMaybe(false))
                {
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
                            v8::String::NewFromUtf8(
                                isolate, "Simple key dictionary requires numeric or string representation of numeric key")
                                .ToLocalChecked());
                        return;
                    }

                    complete_obj->Set(context, prop_name, v8::Number::New(isolate, key_value)).Check();
                }
            }

            /// Add the complete object to our array
            complete_values->Set(context, i, complete_obj).Check();
        }

        /// Convert batch to block
        Block block = value_converter->jsBatchToBlock(isolate, complete_values, sample_block);

        /// Insert to backend storage - use move semantics
        DB::StorageID source_table_id = bridge.getSourceDbAndTable(*dict_name);
        bool insert_success = true;

        try
        {
            bridge.InsertToBackendStorage(std::move(block), source_table_id);
        }
        catch (const DB::Exception & e)
        {
            LOG_ERROR(bridge.logger, "Error inserting batch to backend storage: {}", e.displayText());
            insert_success = false;
        }

        args.GetReturnValue().Set(v8::Boolean::New(isolate, insert_success));

        /// Performance stats
        auto total_ms = total_stopwatch.elapsedMilliseconds();
        LOG_DEBUG(bridge.logger, "batchSetValues: {} items, total={}ms", batch_size, total_ms);
    }
    catch (const DB::Exception & e)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Error in batchSetValues (code={}): {}", e.code(), e.displayText());
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, e.displayText().c_str()).ToLocalChecked());
    }
    catch (...)
    {
        CacheDictionaryBridge & bridge = instance();
        LOG_ERROR(bridge.logger, "Unknown error in batchSetValues");
        isolate->ThrowException(v8::String::NewFromUtf8(isolate, "Unknown error in batchSetValues").ToLocalChecked());
    }
}

}
