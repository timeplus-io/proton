#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeString.h>

namespace DB::V8
{
v8::Local<v8::Value> CacheDictionaryBridge::columnValueToV8(v8::Isolate * isolate, const IColumn & column, size_t row)
{
    TypeIndex type_id = column.getDataType();

    static const auto & converters = getColumnConverters();
    auto it = converters.find(type_id);

    if (it != converters.end())
        return it->second(isolate, column, row);

    WriteBufferFromOwnString wb;
    column.serializeValueIntoBuffer(row, wb);
    return V8::to_v8(isolate, wb.str());
}

const absl::flat_hash_map<TypeIndex, CacheDictionaryBridge::ColumnConverter> & CacheDictionaryBridge::getColumnConverters()
{
    static const absl::flat_hash_map<TypeIndex, CacheDictionaryBridge::ColumnConverter> converters = []() {
        absl::flat_hash_map<TypeIndex, CacheDictionaryBridge::ColumnConverter> temp_map;

        /// String types
        temp_map[TypeIndex::String] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return V8::to_v8(isolate, static_cast<const ColumnString &>(column).getDataAt(row).data);
        };

        temp_map[TypeIndex::FixedString] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            const auto & col = static_cast<const ColumnFixedString &>(column);
            auto str = col.getDataAt(row);
            return V8::to_v8(isolate, str.data, str.size);
        };

        /// Integer types
        temp_map[TypeIndex::Int8] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return v8::Integer::New(isolate, static_cast<const ColumnInt8 &>(column).getData()[row]);
        };

        temp_map[TypeIndex::UInt8] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return v8::Integer::New(isolate, static_cast<const ColumnUInt8 &>(column).getData()[row]);
        };

        temp_map[TypeIndex::Int16] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return v8::Integer::New(isolate, static_cast<const ColumnInt16 &>(column).getData()[row]);
        };

        temp_map[TypeIndex::UInt16] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return v8::Integer::NewFromUnsigned(isolate, static_cast<const ColumnUInt16 &>(column).getData()[row]);
        };

        temp_map[TypeIndex::Int32] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return v8::Integer::New(isolate, static_cast<const ColumnInt32 &>(column).getData()[row]);
        };

        temp_map[TypeIndex::UInt32] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return v8::Integer::NewFromUnsigned(isolate, static_cast<const ColumnUInt32 &>(column).getData()[row]);
        };

        temp_map[TypeIndex::Int64] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return V8::to_v8(isolate, static_cast<const ColumnInt64 &>(column).getData()[row]);
        };

        temp_map[TypeIndex::UInt64] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return V8::to_v8(isolate, static_cast<const ColumnUInt64 &>(column).getData()[row]);
        };

        /// Float types
        temp_map[TypeIndex::Float32] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return v8::Number::New(isolate, static_cast<const ColumnFloat32 &>(column).getData()[row]);
        };

        temp_map[TypeIndex::Float64] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            return v8::Number::New(isolate, static_cast<const ColumnFloat64 &>(column).getData()[row]);
        };

        /// Date and time types - optimized to create JavaScript Date objects directly
        temp_map[TypeIndex::Date] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            time_t timestamp = static_cast<const ColumnDate &>(column).getData()[row] * 86400; // days to seconds
            return DB::V8::toV8Date(isolate, static_cast<double>(timestamp) * 1000);
        };

        temp_map[TypeIndex::DateTime] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            time_t timestamp = static_cast<const ColumnDateTime &>(column).getData()[row];
            return DB::V8::toV8Date(isolate, static_cast<double>(timestamp) * 1000);
        };

        temp_map[TypeIndex::DateTime64] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            const auto & col = static_cast<const ColumnDateTime64 &>(column);
            Int64 value = col.getData()[row];
            return DB::V8::toV8Date(isolate, static_cast<double>(value));
        };

        /// Complex types
        temp_map[TypeIndex::Array] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            const auto & col = static_cast<const ColumnArray &>(column);
            const auto & offsets = col.getOffsets();
            size_t offset = row > 0 ? offsets[row - 1] : 0;
            size_t size = offsets[row] - offset;

            v8::Local<v8::Array> result = v8::Array::New(isolate, static_cast<int>(size));
            auto context = isolate->GetCurrentContext();
            const auto & nested_column = col.getData();

            auto & bridge = CacheDictionaryBridge::instance();
            for (size_t i = 0; i < size; ++i)
            {
                auto value = bridge.columnValueToV8(isolate, nested_column, offset + i);
                result->Set(context, static_cast<uint32_t>(i), value).Check();
            }

            return result;
        };

        temp_map[TypeIndex::Nullable] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            const auto & col = static_cast<const ColumnNullable &>(column);
            if (col.isNullAt(row))
                return v8::Null(isolate);
            return CacheDictionaryBridge::instance().columnValueToV8(isolate, col.getNestedColumn(), row);
        };

        temp_map[TypeIndex::Tuple] = [](v8::Isolate * isolate, const IColumn & column, size_t row) -> v8::Local<v8::Value> {
            const auto & col = static_cast<const ColumnTuple &>(column);
            const auto & columns = col.getColumns();
            size_t num_elements = columns.size();

            v8::Local<v8::Object> result = v8::Object::New(isolate);
            auto context = isolate->GetCurrentContext();

            auto & bridge = CacheDictionaryBridge::instance();
            for (size_t i = 0; i < num_elements; ++i)
            {
                auto value = bridge.columnValueToV8(isolate, *columns[i], row);
                result->Set(context, V8::to_v8(isolate, std::to_string(i)), value).Check();
            }

            return result;
        };

        return temp_map;
    }();

    return converters;
}
}
