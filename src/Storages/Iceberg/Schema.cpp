#include <Storages/Iceberg/Schema.h>

#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_CONVERT_TYPE;
}

using namespace DB;
using JsonObjectPtr = std::shared_ptr<Poco::JSON::Object>;
using JsonArrayPtr = std::shared_ptr<Poco::JSON::Array>;

namespace
{

void setFieldType(Poco::JSON::Object & field, const DataTypePtr & proton_type, int & next_field_id, const std::string & name = "type");

void setNullableType(Poco::JSON::Object & field, const DataTypePtr & proton_type, int & next_field_id, const std::string & name)
{
    const auto nested_type = typeid_cast<const DataTypeNullable *>(proton_type.get())->getNestedType();
    setFieldType(field, nested_type, next_field_id, name);
}

void setArrayType(Poco::JSON::Object & field, const DataTypePtr & proton_type, int & next_field_id, const std::string & name)
{
    const auto element_type = typeid_cast<const DataTypeArray *>(proton_type.get())->getNestedType();

    Poco::JSON::Object list_json;
    list_json.set("type", "list");
    list_json.set("element-id", next_field_id++);
    list_json.set("element-required", !element_type->isNullable());
    setFieldType(list_json, element_type, next_field_id, "element");

    field.set(name, list_json);
}

void addField(Poco::JSON::Array & fields, const String & name, const DataTypePtr & data_type, int & next_field_id)
{
    Poco::JSON::Object field;
    field.set("id", next_field_id++);
    field.set("name", name);


    if (data_type->isNullable())
    {
        field.set("required", false);
        /// TODO
        /// Version 3: set "initial-default" and "write-default"
        setFieldType(field, typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType(), next_field_id);
    }
    else
    {
        field.set("required", true);
        setFieldType(field, data_type, next_field_id);
    }

    fields.add(field);
}

Poco::JSON::Object newStructType(const DB::NamesAndTypesList & columns, int & next_field_id)
{
    Poco::JSON::Array fields_array;
    for (const auto & [name, data_type] : columns)
        addField(fields_array, name, data_type, next_field_id);

    Poco::JSON::Object struct_json;
    struct_json.set("type", "struct");
    struct_json.set("fields", fields_array);
    return struct_json;
}

void setTupleType(Poco::JSON::Object & field, const DataTypePtr & proton_type, int & next_field_id, const std::string & name = "type")
{
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(proton_type.get());
    const auto & elements = tuple_type->getElements();
    const auto & names = tuple_type->getElementNames();

    NamesAndTypesList schema;
    for (size_t i = 0; const auto & element : elements)
        schema.emplace_back(names[i++], element);

    field.set(name, newStructType(schema, next_field_id));
}

void setMapType(Poco::JSON::Object & field, const DataTypePtr & proton_type, int & next_field_id, const std::string & name)
{
    const auto * map_type = typeid_cast<const DataTypeMap *>(proton_type.get());
    const auto key_type = map_type->getKeyType();
    const auto value_type = map_type->getValueType();

    Poco::JSON::Object map_json;
    map_json.set("type", "map");
    map_json.set("key-id", next_field_id++);
    setFieldType(map_json, key_type, next_field_id, "key");
    map_json.set("value-id", next_field_id++);
    map_json.set("value-required", !value_type->isNullable());
    setFieldType(map_json, value_type, next_field_id, "value");

    field.set(name, map_json);
}

void setFieldType(Poco::JSON::Object & field, const DataTypePtr & proton_type, int & next_field_id, const std::string & name)
{
    /// Since we also generate proton stream schemas based on Iceberg table schemas,
    /// we need to make sure that proton types and Iceberg types are 1:1 mapped.
    /// We could consider using structs for proton types which are not supported by Iceberg table format.
    /// For example, we can map `ipv4` to `struct { _tp_ipv4: string }`.
    switch (proton_type->getTypeId())
    {
        case TypeIndex::Bool:
            field.set(name, "boolean");
            break;

        /// Integer types
        case TypeIndex::UInt8:
            [[fallthrough]];
        case TypeIndex::UInt16:
            [[fallthrough]];
        case TypeIndex::UInt32:
            [[fallthrough]];
        case TypeIndex::UInt64:
            [[fallthrough]];
        case TypeIndex::UInt128:
            [[fallthrough]];
        case TypeIndex::UInt256:
            throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE, "Iceberg table format does not support unsigned integer types");

        case TypeIndex::Int8:
            [[fallthrough]];
        case TypeIndex::Int16:
            throw DB::Exception(
                DB::ErrorCodes::CANNOT_CONVERT_TYPE, "Iceberg table format does not support signed integer types less than 32-bit");
        case TypeIndex::Int32:
            field.set(name, "int");
            break;
        case TypeIndex::Int64:
            field.set(name, "long");
            break;
        case TypeIndex::Int128:
            [[fallthrough]];
        case TypeIndex::Int256:
            throw DB::Exception(
                DB::ErrorCodes::CANNOT_CONVERT_TYPE, "Iceberg table format does not support signed integer types bigger than 64-bit");

        /// Floating point
        case TypeIndex::Float32:
            field.set(name, "float");
            break;
        case TypeIndex::Float64:
            field.set(name, "double");
            break;

        /// String types
        case TypeIndex::String:
            field.set(name, "string");
            break;
        case TypeIndex::FixedString:
        {
            auto n = typeid_cast<const DataTypeFixedString *>(proton_type.get())->getN();
            field.set(name, fmt::format("fixed[{}]", n));
            break;
        }
        /// Date/time types
        /// TODO Version 3: timestamp_ns, timestamptz_ns
        case TypeIndex::Date:
            field.set(name, "date");
            break;
        case TypeIndex::Date32:
            /// ClickHouse maps Iceberg date to DataTypeDate, so we keep it the same way for now.
            throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE, "Please use date instead of date32");
        case TypeIndex::DateTime:
            throw DB::Exception(DB::ErrorCodes::CANNOT_CONVERT_TYPE, "Please use datetime64 instead of datetime");
        case TypeIndex::DateTime64:
        {
            auto p = typeid_cast<const DataTypeDateTime64 *>(proton_type.get())->getScale();
            if (p > 6)
                throw DB::Exception(
                    DB::ErrorCodes::CANNOT_CONVERT_TYPE,
                    "{} with scale greater than 6 is not supported, used scale: {}",
                    proton_type->getName(),
                    p);
            field.set(name, "timestamptz");
            break;
        }
        // Special types
        case TypeIndex::UUID:
            field.set(name, "uuid");
            break;

        // Decimal types (require special handling in conversion)
        case TypeIndex::Decimal32:
            [[fallthrough]];
        case TypeIndex::Decimal64:
            [[fallthrough]];
        case TypeIndex::Decimal128:
            [[fallthrough]];
        case TypeIndex::Decimal256:
            field.set(name, fmt::format("decimal({}, {})", getDecimalPrecision(*proton_type), getDecimalScale(*proton_type)));
            break;

        // Complex types (should be handled before this)
        case TypeIndex::Array:
            setArrayType(field, proton_type, next_field_id, name);
            break;
        case TypeIndex::Tuple:
            setTupleType(field, proton_type, next_field_id, name);
            break;
        case TypeIndex::Map:
            setMapType(field, proton_type, next_field_id, name);
            break;
        case TypeIndex::Nullable:
            setNullableType(field, proton_type, next_field_id, name);
            break;
        case TypeIndex::LowCardinality:
            throw std::runtime_error("Complex type should have been handled earlier: " + proton_type->getName());

        // Unsupported types
        case TypeIndex::Nothing: /// TODO Version 3
            [[fallthrough]];
        case TypeIndex::Enum8:
            [[fallthrough]];
        case TypeIndex::Enum16:
            [[fallthrough]];
        case TypeIndex::IPv4:
            [[fallthrough]];
        case TypeIndex::IPv6:
            [[fallthrough]];
        case TypeIndex::Object:
            [[fallthrough]];
        case TypeIndex::AggregateFunction:
            [[fallthrough]];
        case TypeIndex::Function:
            [[fallthrough]];
        case TypeIndex::Interval:
            [[fallthrough]];
        case TypeIndex::Set:
            [[fallthrough]];
        case TypeIndex::JSONPaths:
        case TypeIndex::Variant:
        case TypeIndex::Dynamic:
            throw DB::Exception(
                DB::ErrorCodes::CANNOT_CONVERT_TYPE, "Iceberg table format does not support type: {}", proton_type->getName());
    }
}

void collectFieldIds(const Poco::JSON::Object & type, const std::string & path, Apache::Iceberg::FieldIdsByPath & result);

void collectStructFieldIds(const Poco::JSON::Array & fields, const std::string & path, Apache::Iceberg::FieldIdsByPath & result)
{
    for (unsigned i = 0; i < fields.size(); ++i)
    {
        auto field = fields.getObject(i);
        auto field_path = Nested::concatenateName(path, field->getValue<std::string>("name"));
        result[field_path] = field->getValue<int64_t>("id");
        if (field->isObject("type"))
            collectFieldIds(*field->getObject("type"), field_path, result);
    }
}

/// Mirrors the path naming of the Iceberg spec: struct members by name, list elements as `element`,
/// map entries as `key` and `value`. Nullability does not appear in the path.
void collectFieldIds(const Poco::JSON::Object & type, const std::string & path, Apache::Iceberg::FieldIdsByPath & result)
{
    auto type_name = type.getValue<std::string>("type");
    if (type_name == "struct")
    {
        collectStructFieldIds(*type.getArray("fields"), path, result);
    }
    else if (type_name == "list")
    {
        auto element_path = Nested::concatenateName(path, "element");
        result[element_path] = type.getValue<int64_t>("element-id");
        if (type.isObject("element"))
            collectFieldIds(*type.getObject("element"), element_path, result);
    }
    else if (type_name == "map")
    {
        auto key_path = Nested::concatenateName(path, "key");
        result[key_path] = type.getValue<int64_t>("key-id");
        if (type.isObject("key"))
            collectFieldIds(*type.getObject("key"), key_path, result);

        auto value_path = Nested::concatenateName(path, "value");
        result[value_path] = type.getValue<int64_t>("value-id");
        if (type.isObject("value"))
            collectFieldIds(*type.getObject("value"), value_path, result);
    }
    else
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown Iceberg complex type: {}", type_name);
}

} // namespace

namespace Apache::Iceberg
{

Poco::JSON::Object generateIcebergSchema(const NamesAndTypesList & proton_schema)
{
    int next_field_id = 1;
    auto schema_json = newStructType(proton_schema, next_field_id);
    return schema_json;
}

FieldIdsByPath getFieldIdsByPath(const Poco::JSON::Object & iceberg_schema)
{
    FieldIdsByPath result;
    collectStructFieldIds(*iceberg_schema.getArray("fields"), /*path=*/"", result);
    return result;
}

}
