#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>
#include <V8/Modules/DictionaryAccess/DictionaryValueConverter.h>

#include <IO/WriteBufferFromString.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>
#include <Common/timeScale.h>

namespace DB::V8
{

DictionaryValueConverter::DictionaryValueConverter(const DictionaryStructure & dict_struct_)
    : dict_struct(dict_struct_), logger(getLogger("DictionaryValueConverter"))
{
    attribute_names.reserve(dict_struct.attributes.size());
    attribute_types.reserve(dict_struct.attributes.size());
    for (const auto & attr : dict_struct.attributes)
    {
        attribute_names.push_back(attr.name);
        attribute_types.push_back(attr.type);
    }

    LOG_DEBUG(logger, "Value converter initialized with {} attributes", attribute_names.size());
}

Block DictionaryValueConverter::jsObjectToBlock(v8::Isolate * isolate, v8::Local<v8::Object> value_obj, const Block & sample_block) const
{
    auto context = isolate->GetCurrentContext();
    Block result = sample_block.cloneEmpty();
    MutableColumns mutable_columns = result.mutateColumns();

    /// Cache field names for better performance
    std::vector<v8::Local<v8::String>> field_names;
    field_names.reserve(sample_block.columns());

    for (size_t i = 0; i < sample_block.columns(); ++i)
    {
        const auto & col_name = sample_block.getByPosition(i).name;
        field_names.push_back(v8::String::NewFromUtf8(isolate, col_name.c_str()).ToLocalChecked());
    }

    for (size_t i = 0; i < sample_block.columns(); ++i)
    {
        const auto & col_type = sample_block.getByPosition(i).type;
        const auto & col_name = sample_block.getByPosition(i).name;

        /// Handle special event time field
        if (col_name == ProtonConsts::RESERVED_EVENT_TIME)
        {
            mutable_columns[i]->insert(DB::nowSubsecond(3).get<DB::DateTime64>());
            continue;
        }

        v8::Maybe<bool> maybe_has_prop = value_obj->Has(context, field_names[i]);
        if (maybe_has_prop.IsNothing() || !maybe_has_prop.FromJust())
        {
            LOG_TRACE(logger, "Column '{}' not found in JS object, inserting default", col_name);
            mutable_columns[i]->insertDefault();
            continue;
        }

        v8::MaybeLocal<v8::Value> maybe_field_value = value_obj->Get(context, field_names[i]);
        if (maybe_field_value.IsEmpty())
        {
            LOG_TRACE(logger, "Empty value for column '{}'", col_name);
            mutable_columns[i]->insertDefault();
            continue;
        }

        v8::Local<v8::Value> field_value = maybe_field_value.ToLocalChecked();

        /// Handle null values explicitly
        if (field_value->IsNullOrUndefined())
        {
            LOG_TRACE(logger, "Null or undefined value for column '{}'", col_name);
            mutable_columns[i]->insertDefault();
            continue;
        }

        try
        {
            insertValueIntoColumn(isolate, *mutable_columns[i], col_type, field_value);
        }
        catch (const DB::Exception & e)
        {
            LOG_WARNING(logger, "Error converting field {}: {}", col_name, e.displayText());
            mutable_columns[i]->insertDefault();
        }
    }

    result.setColumns(std::move(mutable_columns));
    return result;
}

Block DictionaryValueConverter::jsBatchToBlock(v8::Isolate * isolate, v8::Local<v8::Array> values_array, const Block & sample_block) const
{
    auto context = isolate->GetCurrentContext();
    uint32_t num_rows = values_array->Length();

    if (num_rows == 0)
        return sample_block.cloneEmpty();

    Block result = sample_block.cloneEmpty();
    MutableColumns mutable_columns = result.mutateColumns();

    for (auto & column : mutable_columns)
        column->reserve(num_rows);

    std::vector<v8::Local<v8::String>> field_names;
    field_names.reserve(sample_block.columns());

    for (size_t i = 0; i < sample_block.columns(); ++i)
    {
        const auto & col_name = sample_block.getByPosition(i).name;
        field_names.push_back(v8::String::NewFromUtf8(isolate, col_name.c_str()).ToLocalChecked());
    }

    LOG_TRACE(logger, "Processing batch with {} rows", num_rows);

    for (uint32_t row = 0; row < num_rows; ++row)
    {
        v8::Local<v8::Value> row_value = values_array->Get(context, row).ToLocalChecked();
        if (!row_value->IsObject() || row_value->IsArray())
        {
            LOG_WARNING(logger, "Row {} is not a valid object, inserting defaults", row);
            for (auto & column : mutable_columns)
                column->insertDefault();
            continue;
        }

        v8::Local<v8::Object> value_obj = row_value->ToObject(context).ToLocalChecked();

        for (size_t i = 0; i < sample_block.columns(); ++i)
        {
            const auto & col_type = sample_block.getByPosition(i).type;
            const auto & col_name = sample_block.getByPosition(i).name;

            /// Handle special event time field
            if (col_name == ProtonConsts::RESERVED_EVENT_TIME)
            {
                mutable_columns[i]->insert(DB::nowSubsecond(3).get<DB::DateTime64>());
                continue;
            }

            v8::Maybe<bool> maybe_has_prop = value_obj->Has(context, field_names[i]);
            if (maybe_has_prop.IsNothing() || !maybe_has_prop.FromJust())
            {
                mutable_columns[i]->insertDefault();
                continue;
            }

            v8::MaybeLocal<v8::Value> maybe_field_value = value_obj->Get(context, field_names[i]);
            if (maybe_field_value.IsEmpty())
            {
                mutable_columns[i]->insertDefault();
                continue;
            }

            v8::Local<v8::Value> field_value = maybe_field_value.ToLocalChecked();

            /// Handle null values explicitly
            if (field_value->IsNullOrUndefined())
            {
                mutable_columns[i]->insertDefault();
                continue;
            }

            try
            {
                insertValueIntoColumn(isolate, *mutable_columns[i], col_type, field_value);
            }
            catch (const DB::Exception & e)
            {
                LOG_WARNING(logger, "Error converting field {} at row {}: {}", col_name, row, e.displayText());
                mutable_columns[i]->insertDefault();
            }
        }
    }

    result.setColumns(std::move(mutable_columns));
    LOG_TRACE(logger, "Successfully processed batch with {} rows", num_rows);
    return result;
}

v8::Local<v8::Object>
DictionaryValueConverter::columnsToJsObject(v8::Isolate * isolate, const Columns & columns, size_t row, const Names & column_names) const
{
    auto context = isolate->GetCurrentContext();
    v8::Local<v8::Object> result = v8::Object::New(isolate);

    for (size_t i = 0; i < std::min(columns.size(), column_names.size()); ++i)
    {
        try
        {
            const auto & column = columns[i];
            const auto & column_name = column_names[i];

            v8::Local<v8::Value> js_value = columnValueToJS(isolate, *column, row);

            result->Set(context, v8::String::NewFromUtf8(isolate, column_name.c_str()).ToLocalChecked(), js_value).Check();
        }
        catch (const DB::Exception & e)
        {
            LOG_WARNING(logger, "Error converting column {} to JS: {}", column_names[i], e.displayText());
        }
    }

    return result;
}

v8::Local<v8::Array>
DictionaryValueConverter::columnsToJsArray(v8::Isolate * isolate, const Columns & columns, const Names & column_names) const
{
    auto context = isolate->GetCurrentContext();
    size_t num_rows = columns.empty() ? 0 : columns[0]->size();

    LOG_TRACE(logger, "Converting {} rows to JS array", num_rows);
    v8::Local<v8::Array> result = v8::Array::New(isolate, static_cast<uint32_t>(num_rows));

    /// Object template for better performance
    v8::Local<v8::ObjectTemplate> row_template = v8::ObjectTemplate::New(isolate);

    std::vector<v8::Local<v8::String>> js_column_names;
    js_column_names.reserve(column_names.size());

    for (const auto & col_name : column_names)
    {
        auto js_name = v8::String::NewFromUtf8(isolate, col_name.c_str()).ToLocalChecked();
        js_column_names.push_back(js_name);

        row_template->Set(js_name, v8::Undefined(isolate));
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        v8::Local<v8::Object> row_obj = row_template->NewInstance(context).ToLocalChecked();

        for (size_t i = 0; i < std::min(columns.size(), column_names.size()); ++i)
        {
            try
            {
                const auto & column = columns[i];

                v8::Local<v8::Value> js_value = columnValueToJS(isolate, *column, row);

                row_obj->Set(context, js_column_names[i], js_value).Check();
            }
            catch (const DB::Exception & e)
            {
                LOG_WARNING(logger, "Error converting column {} at row {} to JS: {}", column_names[i], row, e.displayText());
            }
        }

        result->Set(context, static_cast<uint32_t>(row), row_obj).Check();
    }

    return result;
}

Names DictionaryValueConverter::getAttributeNames() const
{
    return attribute_names;
}

DataTypes DictionaryValueConverter::getAttributeTypes() const
{
    return attribute_types;
}

v8::Local<v8::Value> DictionaryValueConverter::columnValueToJS(v8::Isolate * isolate, const IColumn & column, size_t row) const
{
    try
    {
        return DB::V8::CacheDictionaryBridge::instance().columnValueToV8(isolate, column, row);
    }
    catch (const DB::Exception & e)
    {
        LOG_WARNING(logger, "Error in columnValueToJS, falling back to string serialization: {}", e.displayText());

        try
        {
            DB::WriteBufferFromOwnString wb;
            column.serializeValueIntoBuffer(row, wb);
            return DB::V8::to_v8(isolate, wb.str());
        }
        catch (...)
        {
            LOG_ERROR(logger, "Failed to serialize column value to string, returning empty string");
            return DB::V8::to_v8(isolate, "");
        }
    }
}

bool DictionaryValueConverter::insertValueIntoColumn(
    v8::Isolate * isolate, IColumn & column, const DataTypePtr & type, v8::Local<v8::Value> value) const
{
    try
    {
        DB::V8::insertResult(isolate, column, type, value, false);
        return true;
    }
    catch (const DB::Exception & e)
    {
        LOG_WARNING(logger, "Error inserting value into column: {}", e.displayText());
        column.insertDefault();
        return false;
    }
}
}
