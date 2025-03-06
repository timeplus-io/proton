#include <Processors/Formats/Impl/AvroConfluentRowOutputFormat.h>
#if USE_AVRO

#include <IO/WriteHelpers.h>

#include <Formats/KafkaSchemaRegistryForAvro.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/NestedUtils.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

#include <Formats/Avro/Schemas.h>
#include <Formats/Avro/OutputStreamWriteBufferAdapter.h>

#include <Schema.hh>
#include <Types.hh>

namespace DB
{
namespace ErrorCodes
{
extern const int INCOMPATIBLE_SCHEMA;
extern const int MULTIPLE_COLUMNS_SERIALIZED_TO_SAME_AVRO_FIELD;
extern const int THERE_IS_NO_COLUMN;
extern const int TYPE_MISMATCH;
extern const int UNKNOWN_TYPE;
}

namespace
{

inline String concatPath(const std::string & a, const std::string & b)
{
    return a.empty() ? b : a + "." + b;
}

String nodeName(avro::NodePtr node)
{
    if (node->hasName())
        return node->name().simpleName();
    else
        return avro::toString(node->type());
}

bool typesMatched(avro::Type avro_type, const DataTypePtr & timeplusd_type)
{
    auto which = WhichDataType(timeplusd_type);

    switch (avro_type)
    {
        case avro::AVRO_STRING:
            [[fallthrough]];
        case avro::AVRO_BYTES:
            return which.isStringOrFixedString();
        case avro::AVRO_INT:
            return which.isInt8() || which.isInt16() || which.isInt32() || which.isUInt8() || which.isUInt16() || which.isUInt32();
        case avro::AVRO_LONG:
            return which.isInt64() || which.isUInt64();
        case avro::AVRO_FLOAT:
            return which.isFloat32();
        case avro::AVRO_DOUBLE:
            return which.isFloat();
        case avro::AVRO_BOOL:
            return which.isBool();
        case avro::AVRO_NULL:
            return which.isNothing();
        case avro::AVRO_RECORD:
            return which.isTuple();
        case avro::AVRO_ENUM:
            return which.isEnum();
        case avro::AVRO_ARRAY:
            return which.isArray();
        case avro::AVRO_MAP:
            return which.isMap();
        case avro::AVRO_UNION:
            break;
        case avro::AVRO_FIXED:
            return which.isFixedString();
        case avro::AVRO_NUM_TYPES:
            return which.isInt() || which.isFloat();
        case avro::AVRO_UNKNOWN:
            break;
    }

    return false;
}

/// Return the leaf node position of a union if that leaf node matches the timeplusd_type.
std::optional<int> findUnionNodeByType(const avro::NodePtr & union_node, const DataTypePtr & timeplusd_type)
{
    for (int i = 0; i < static_cast<int>(union_node->leaves()); i++)
    {
        const auto & leaf = union_node->leafAt(i);
        if (typesMatched(leaf->type(), timeplusd_type))
            return i;
    }
    return {};
}

Exception missing_column_exception(const String & avro_field_name)
{
    return Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Could not find column for field {} in the Avro schema", avro_field_name);
}

Exception multiple_columns_to_union_exception(const String & field_name)
{
    return Exception(
        ErrorCodes::MULTIPLE_COLUMNS_SERIALIZED_TO_SAME_AVRO_FIELD,
        "Multiple columns were mapped to the same Avro union field {}",
        field_name);
}

}

AvroSchemaSerializer::Action
AvroSchemaSerializer::Action::serializeAction(int target_column_idx_, SerializeFn serialize_fn_, std::optional<int> union_idx_)
{
    Action result;
    result.type = Serialize;
    result.target_column_idx = target_column_idx_;
    result.union_idx = union_idx_;
    result.serialize_fn = serialize_fn_;
    return result;
}

AvroSchemaSerializer::Action AvroSchemaSerializer::Action::groupAction(std::vector<Action> field_actions, std::optional<int> union_idx_)
{
    Action result;
    result.type = Group;
    result.union_idx = union_idx_;
    result.actions = field_actions;
    return result;
}

AvroSchemaSerializer::Action AvroSchemaSerializer::Action::arrayAction(std::vector<Action> field_actions, std::optional<int> union_idx_)
{
    Action result;
    result.type = Array;
    result.union_idx = union_idx_;
    result.actions = field_actions;
    return result;
}

AvroSchemaSerializer::Action AvroSchemaSerializer::Action::nestedAction(
    std::vector<size_t> nested_column_indexes_, std::vector<SerializeFn> nested_serializers_, std::optional<int> union_idx_)
{
    assert(nested_column_indexes_.size() == nested_column_indexes_.size());
    Action result;
    result.type = Nested;
    result.union_idx = union_idx_;
    result.nested_column_indexes = nested_column_indexes_;
    result.nested_serializers = nested_serializers_;
    return result;
}

void AvroSchemaSerializer::Action::execute(const Columns & columns, size_t row_num, avro::Encoder & enc) const
{
    if (union_idx.has_value())
        enc.encodeUnionIndex(*union_idx);

    switch (type)
    {
        case Unknown:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown avro serialization action type");
        case Serialize:
        {
            const auto & col = columns.at(target_column_idx);
            serialize_fn(*col, row_num, enc);
            break;
        }
        case Group:
        {
            for (const auto & action : actions)
                action.execute(columns, row_num, enc);
            break;
        }
        case Array:
            serializeArray(columns, row_num, enc);
            break;
        case Nested:
            serializeNested(columns, row_num, enc);
            break;
    }
}

void AvroSchemaSerializer::Action::serializeNested(const Columns & columns, size_t row_num, avro::Encoder & enc) const
{
    assert(nested_column_indexes.size() == nested_serializers.size());

    size_t row_count = 0;
    std::vector<ColumnPtr> nested_columns;
    nested_columns.reserve(nested_column_indexes.size());

    for (auto nested_index : nested_column_indexes)
    {
        const ColumnArray & column_array = assert_cast<const ColumnArray &>(*columns.at(nested_index));
        if (row_count == 0)
        {
            const ColumnArray::Offsets & offsets = column_array.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t next_offset = offsets[row_num];
            row_count = next_offset - offset;
        }
        nested_columns.push_back(column_array.getDataPtr());
    }

    enc.arrayStart();
    if (row_count > 0)
    {
        enc.setItemCount(row_count);
    }
    for (size_t row = 0; row < row_count; row++)
        for (size_t i = 0; i < nested_serializers.size(); i++)
            nested_serializers.at(i)(*nested_columns[i], row, enc);
    enc.arrayEnd();
}

void AvroSchemaSerializer::Action::serializeArray(const Columns & columns, size_t row_num, avro::Encoder & enc) const
{
    size_t row_count = 0;
    std::vector<ColumnPtr> nested_columns(columns.size());

    assert(std::all_of(actions.begin(), actions.end(), [](const auto & action) { return action.type == Serialize; }));

    for (const auto & action : actions)
    {
        const ColumnArray & column_array = assert_cast<const ColumnArray &>(*columns.at(action.target_column_idx));
        if (row_count == 0)
        {
            const ColumnArray::Offsets & offsets = column_array.getOffsets();
            size_t offset = offsets[row_num - 1];
            size_t next_offset = offsets[row_num];
            row_count = next_offset - offset;
        }
        nested_columns[action.target_column_idx] = column_array.getDataPtr();
    }

    enc.arrayStart();
    if (row_count > 0)
    {
        enc.setItemCount(row_count);
    }
    for (size_t row = 0; row < row_count; row++)
        for (const auto & action : actions)
            action.execute(nested_columns, row, enc);
    enc.arrayEnd();
}

AvroSchemaSerializer::AvroSchemaSerializer(avro::ValidSchema valid_schema, const Block & header, WriteBuffer & out)
    : ostream(std::make_unique<Avro::OutputStreamWriteBufferAdapter>(out)), encoder(avro::validatingEncoder(valid_schema, avro::binaryEncoder()))
{
    const auto & schema_root = valid_schema.root();
    if (schema_root->type() != avro::AVRO_RECORD)
    {
        throw Exception(ErrorCodes::INCOMPATIBLE_SCHEMA, "Unsupported schema, root element must be a record");
    }

    row_actions.reserve(schema_root->leaves());
    for (int i = 0; i < static_cast<int>(schema_root->leaves()); i++)
    {
        const auto & node = schema_root->leafAt(i);
        auto action = createAction(header, node, schema_root->nameAt(i));
        if (!action)
            throw missing_column_exception(schema_root->nameAt(i));
        row_actions.push_back(*action);
    }

    encoder->init(*ostream);
}

void AvroSchemaSerializer::serializeRow(const Columns & columns, size_t row_num)
{
    for (const auto & row_action : row_actions)
        row_action.execute(columns, row_num, *encoder);
}

AvroSchemaSerializer::SerializeFn AvroSchemaSerializer::createSerializeFn(const avro::NodePtr & node, const DataTypePtr & data_type) const
{
    auto avro_type = node->type();

    SerializeFn union_index_serializer;
    avro::NodePtr actual_node;
    if (data_type->getTypeId() != TypeIndex::Nullable)
    {
        if (avro_type == avro::AVRO_UNION)
        {
            auto i = findUnionNodeByType(node, data_type);
            if (i.has_value())
            {
                actual_node = node->leafAt(*i);
                avro_type = actual_node->type();
                union_index_serializer = createUnionSerializeFn(*i);
            }
        }
    }

    if (!actual_node)
        actual_node = node;

    auto type_mismatch_exception = [data_type, avro_type](const String & expected_type) {
        return Exception(
            ErrorCodes::TYPE_MISMATCH,
            "Type `{}` must map to Avro type {}, but got {}",
            data_type->getName(),
            expected_type,
            avro::toString(avro_type));
    };

    switch (data_type->getTypeId())
    {
        case TypeIndex::LowCardinality:
        {
            const auto & nested_type = removeLowCardinality(data_type);
            auto nested_ser = createSerializeFn(actual_node, nested_type);
            return [union_index_serializer, nested_ser](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);
                const auto & col = assert_cast<const ColumnLowCardinality &>(column);
                nested_ser(*col.getDictionary().getNestedColumn(), col.getIndexAt(row_num), enc);
            };
        }
        case TypeIndex::Nullable:
        {
            if (avro_type != avro::AVRO_UNION)
                throw type_mismatch_exception("union");

            if (actual_node->leaves() != 2)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type `nullable` must map to Avro type `union` which has 2 elements");

            int union_null_index = -1;
            if (actual_node->leafAt(0)->type() == avro::AVRO_NULL)
                union_null_index = 0;
            if (actual_node->leafAt(1)->type() == avro::AVRO_NULL)
                union_null_index = 1;

            if (union_null_index < 0)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type `nullable` must map to Avro type `union` which contains `null`");

            auto nested_type = removeNullable(data_type);
            auto nested_ser = createSerializeFn(actual_node->leafAt(1 - union_null_index), nested_type);
            return [union_null_index, nested_ser](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);
                if (!col.isNullAt(row_num))
                {
                    enc.encodeUnionIndex(1 - union_null_index);
                    nested_ser(col.getNestedColumn(), row_num, enc);
                }
                else
                {
                    enc.encodeUnionIndex(union_null_index);
                    enc.encodeNull();
                }
            };
        }
        case TypeIndex::UInt8:
            if (isBool(data_type))
            {
                if (avro_type != avro::AVRO_BOOL)
                    throw type_mismatch_exception("boolean");

                return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                    if (union_index_serializer)
                        union_index_serializer(column, row_num, enc);

                    enc.encodeBool(assert_cast<const ColumnUInt8 &>(column).getElement(row_num));
                };
            }

            if (avro_type != avro::AVRO_INT && avro_type != avro::AVRO_LONG)
                throw type_mismatch_exception("int");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeInt(assert_cast<const ColumnUInt8 &>(column).getElement(row_num));
            };
        case TypeIndex::Int8:
            if (avro_type != avro::AVRO_INT && avro_type != avro::AVRO_LONG)
                throw type_mismatch_exception("int");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeInt(assert_cast<const ColumnInt8 &>(column).getElement(row_num));
            };
        case TypeIndex::UInt16:
            if (avro_type != avro::AVRO_INT && avro_type != avro::AVRO_LONG)
                throw type_mismatch_exception("int");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeInt(assert_cast<const ColumnUInt16 &>(column).getElement(row_num));
            };
        case TypeIndex::Int16:
            if (avro_type != avro::AVRO_INT && avro_type != avro::AVRO_LONG)
                throw type_mismatch_exception("int");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeInt(assert_cast<const ColumnInt16 &>(column).getElement(row_num));
            };
        case TypeIndex::UInt32:
            [[fallthrough]];
        case TypeIndex::DateTime:
            if (avro_type != avro::AVRO_INT && avro_type != avro::AVRO_LONG)
                throw type_mismatch_exception("int");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeInt(assert_cast<const ColumnUInt32 &>(column).getElement(row_num));
            };
        case TypeIndex::Int32:
            if (avro_type != avro::AVRO_INT && avro_type != avro::AVRO_LONG)
                throw type_mismatch_exception("int");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeInt(assert_cast<const ColumnInt32 &>(column).getElement(row_num));
            };
        case TypeIndex::UInt64:
            if (avro_type != avro::AVRO_LONG)
                throw type_mismatch_exception("long");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeLong(assert_cast<const ColumnUInt64 &>(column).getElement(row_num));
            };
        case TypeIndex::Int64:
            if (avro_type != avro::AVRO_LONG)
                throw type_mismatch_exception("long");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeLong(assert_cast<const ColumnInt64 &>(column).getElement(row_num));
            };
        case TypeIndex::Float32:
            if (avro_type != avro::AVRO_FLOAT)
                throw type_mismatch_exception("float");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeFloat(assert_cast<const ColumnFloat32 &>(column).getElement(row_num));
            };
        case TypeIndex::Float64:
            if (avro_type != avro::AVRO_DOUBLE)
                throw type_mismatch_exception("double");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeDouble(assert_cast<const ColumnFloat64 &>(column).getElement(row_num));
            };
        case TypeIndex::Date:
        {
            if (avro_type != avro::AVRO_INT || actual_node->logicalType().type() != avro::LogicalType::DATE)
                throw type_mismatch_exception("int with logical type `date`");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                UInt16 date = assert_cast<const DataTypeDate::ColumnType &>(column).getElement(row_num);
                enc.encodeInt(date);
            };
        }
        case TypeIndex::Date32:
        {
            if (avro_type != avro::AVRO_INT || actual_node->logicalType().type() != avro::LogicalType::DATE)
                throw type_mismatch_exception("int with logical type `date`");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                UInt16 date = assert_cast<const DataTypeDate32::ColumnType &>(column).getElement(row_num);
                enc.encodeInt(date);
            };
        }
        case TypeIndex::DateTime64:
        {
            if (avro_type != avro::AVRO_LONG)
                throw type_mismatch_exception("long");

            const auto & provided_type = assert_cast<const DataTypeDateTime64 &>(*data_type);

            if (provided_type.getScale() == 3 && actual_node->logicalType().type() != avro::LogicalType::TIMESTAMP_MILLIS)
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Type `datetime64` with scale 3 requires `timestamp-millis` logical type, but got {}", actual_node->logicalType().type());

            if (provided_type.getScale() == 6 && actual_node->logicalType().type() != avro::LogicalType::TIMESTAMP_MICROS)
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Type `datetime64` with scale 6 requires `timestamp-micros` logical type, but got {}", actual_node->logicalType().type());

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                const auto & col = assert_cast<const DataTypeDateTime64::ColumnType &>(column);
                enc.encodeLong(col.getElement(row_num));
            };
        }
        case TypeIndex::String:
            if (avro_type == avro::AVRO_STRING)
                return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                    if (union_index_serializer)
                        union_index_serializer(column, row_num, enc);

                    const std::string_view & s = assert_cast<const ColumnString &>(column).getDataAt(row_num).toView();
                    enc.encodeString(std::string(s));
                };

            if (avro_type == avro::AVRO_BYTES)
                return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                    if (union_index_serializer)
                        union_index_serializer(column, row_num, enc);

                    const std::string_view & s = assert_cast<const ColumnString &>(column).getDataAt(row_num).toView();
                    enc.encodeBytes(reinterpret_cast<const uint8_t *>(s.data()), s.size());
                };

            throw type_mismatch_exception("string or bytes");
        case TypeIndex::FixedString:
        {
            if (avro_type != avro::AVRO_FIXED)
                throw type_mismatch_exception("fixed");

            /// TODO check fixed size
            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                const std::string_view & s = assert_cast<const ColumnFixedString &>(column).getDataAt(row_num).toView();
                enc.encodeFixed(reinterpret_cast<const uint8_t *>(s.data()), s.size());
            };
        }
        case TypeIndex::Enum8:
        {
            if (avro_type != avro::AVRO_ENUM)
                throw type_mismatch_exception("enum");

            const auto & enum_values = assert_cast<const DataTypeEnum8 &>(*data_type).getValues();
            if (enum_values.size() != actual_node->names())
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Expected an enum8 type with {} values, but got {}",
                    actual_node->names(),
                    enum_values.size());

            std::unordered_map<DataTypeEnum8::FieldType, size_t> enum_mapping;
            for (size_t i = 0; i < actual_node->names(); ++i)
            {
                int idx = static_cast<int>(i);
                if (actual_node->nameAt(idx) != enum_values.at(idx).first)
                    throw Exception(
                        ErrorCodes::TYPE_MISMATCH,
                        "Expected the number {} value of the enum has name {}, but got {}",
                        idx,
                        actual_node->nameAt(idx),
                        enum_values.at(idx).first);

                enum_mapping.emplace(enum_values.at(i).second, i);
            }

            return [union_index_serializer, enum_mapping](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                auto enum_value = assert_cast<const DataTypeEnum8::ColumnType &>(column).getElement(row_num);
                enc.encodeEnum(enum_mapping.at(enum_value));
            };
        }
        case TypeIndex::Enum16:
        {
            if (avro_type != avro::AVRO_ENUM)
                throw type_mismatch_exception("enum");

            const auto & enum_values = assert_cast<const DataTypeEnum16 &>(*data_type).getValues();
            if (enum_values.size() != actual_node->names())
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Expected an enum16 type with {} values, but got {}",
                    actual_node->names(),
                    enum_values.size());

            std::unordered_map<DataTypeEnum16::FieldType, size_t> enum_mapping;
            for (size_t i = 0; i < actual_node->names(); ++i)
            {
                int idx = static_cast<int>(i);
                if (actual_node->nameAt(idx) != enum_values.at(idx).first)
                    throw Exception(
                        ErrorCodes::TYPE_MISMATCH,
                        "Expected the number {} value of the enum has name {}, but got {}",
                        idx,
                        actual_node->nameAt(idx),
                        enum_values.at(idx).first);

                enum_mapping.emplace(enum_values.at(i).second, i);
            }

            return [union_index_serializer, enum_mapping](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                auto enum_value = assert_cast<const DataTypeEnum16::ColumnType &>(column).getElement(row_num);
                enc.encodeEnum(enum_mapping.at(enum_value));
            };
        }
        case TypeIndex::UUID:
        {
            if (avro_type != avro::AVRO_STRING || actual_node->logicalType().type() != avro::LogicalType::UUID)
                throw type_mismatch_exception("string with logical type `uuid`");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                const auto & uuid = assert_cast<const DataTypeUUID::ColumnType &>(column).getElement(row_num);
                std::array<UInt8, 36> s;
                formatUUID(std::reverse_iterator<const UInt8 *>(reinterpret_cast<const UInt8 *>(&uuid) + 16), s.data());
                enc.encodeString({reinterpret_cast<const char *>(s.data()), s.size()});
            };
        }
        case TypeIndex::Array:
        {
            if (avro_type != avro::AVRO_ARRAY)
                throw type_mismatch_exception("array");

            const auto & array_type = assert_cast<const DataTypeArray &>(*data_type);
            auto nested_ser = createSerializeFn(actual_node->leafAt(0), array_type.getNestedType());
            return [union_index_serializer, nested_ser](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
                const ColumnArray::Offsets & offsets = column_array.getOffsets();
                size_t offset = offsets[row_num - 1];
                size_t next_offset = offsets[row_num];
                size_t row_count = next_offset - offset;
                const IColumn & nested_column = column_array.getData();

                enc.arrayStart();
                if (row_count > 0)
                {
                    enc.setItemCount(row_count);
                }
                for (size_t i = offset; i < next_offset; ++i)
                {
                    nested_ser(nested_column, i, enc);
                }
                enc.arrayEnd();
            };
        }
        case TypeIndex::Nothing:
            if (avro_type != avro::AVRO_NULL)
                throw type_mismatch_exception("null");

            return [union_index_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                enc.encodeNull();
            };

        case TypeIndex::Tuple:
        {
            if (avro_type != avro::AVRO_RECORD)
                throw type_mismatch_exception("record");

            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
            const auto & nested_types = tuple_type.getElements();
            std::vector<SerializeFn> nested_serializers;
            nested_serializers.reserve(nested_types.size());
            for (size_t i = 0; i != nested_types.size(); ++i)
            {
                /// We don't care about tuple element names, just the positions.
                auto nested_ser = createSerializeFn(actual_node->leafAt(static_cast<int>(i)), nested_types[i]);
                nested_serializers.push_back(nested_ser);
            }

            return [union_index_serializer, nested_serializers](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                if (union_index_serializer)
                    union_index_serializer(column, row_num, enc);

                const ColumnTuple & column_tuple = assert_cast<const ColumnTuple &>(column);
                const auto & nested_columns = column_tuple.getColumns();
                for (size_t i = 0; i != nested_serializers.size(); ++i)
                    nested_serializers[i](*nested_columns[i], row_num, enc);
            };
        }
        case TypeIndex::Map:
        {
            if (avro_type != avro::AVRO_MAP)
                throw type_mismatch_exception("map");

            const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
            const auto & keys_type = map_type.getKeyType();
            if (!isStringOrFixedString(keys_type))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Avro Maps only support string keys, got {}", keys_type->getName());

            auto keys_serializer = [](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                const StringRef & s = column.getDataAt(row_num);
                enc.encodeString(s.toString());
            };

            const auto & values_type = map_type.getValueType();
            auto values_serializer = createSerializeFn(actual_node->leafAt(1), values_type);

            return
                [union_index_serializer, keys_serializer, values_serializer](const IColumn & column, size_t row_num, avro::Encoder & enc) {
                    if (union_index_serializer)
                        union_index_serializer(column, row_num, enc);

                    const ColumnMap & column_map = assert_cast<const ColumnMap &>(column);
                    const ColumnArray & column_array = column_map.getNestedColumn();
                    const ColumnArray::Offsets & offsets = column_array.getOffsets();
                    size_t offset = offsets[row_num - 1];
                    size_t next_offset = offsets[row_num];
                    size_t row_count = next_offset - offset;
                    const ColumnTuple & nested_columns = column_map.getNestedData();
                    const IColumn & keys_column = nested_columns.getColumn(0);
                    const IColumn & values_column = nested_columns.getColumn(1);

                    enc.mapStart();
                    if (row_count > 0)
                        enc.setItemCount(row_count);

                    for (size_t i = offset; i < next_offset; ++i)
                    {
                        keys_serializer(keys_column, i, enc);
                        values_serializer(values_column, i, enc);
                    }
                    enc.mapEnd();
                };
        }
        default:
            break;
    }

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Cannot encode type {} to Avro type {}", data_type->getName(), avro::toString(avro_type));
}

AvroSchemaSerializer::SerializeFn AvroSchemaSerializer::createDefaultNullUnionSerializeFn() const
{
    static auto null_serialize_fn = [](const IColumn & /*column*/, size_t /*row_num*/, avro::Encoder & enc) {
        enc.encodeUnionIndex(0);
        enc.encodeNull();
    };
    return null_serialize_fn;
}

AvroSchemaSerializer::SerializeFn AvroSchemaSerializer::createUnionSerializeFn(int index) const
{
    return [index](const IColumn & /*column*/, size_t /*row_num*/, avro::Encoder & enc) { enc.encodeUnionIndex(index); };
}

std::optional<AvroSchemaSerializer::Action>
AvroSchemaSerializer::createAction(const Block & header, const avro::NodePtr & node, const std::string & current_path, int union_index)
{
    std::optional<int> union_idx;
    if (union_index >= 0)
        union_idx = union_index;

    if (node->type() == avro::AVRO_SYMBOLIC)
    {
        /// continue traversal only if some column name starts with current_path
        auto keep_going = std::any_of(header.begin(), header.end(), [&current_path](const ColumnWithTypeAndName & col) {
            return col.name.starts_with(current_path);
        });
        auto resolved_node = avro::resolveSymbol(node);
        if (keep_going)
            return createAction(header, resolved_node, current_path, union_index);
        else
        {
            /// if the node is a union and defaults to null, then we use null as the value when the column is missing.
            if (resolved_node->type() == avro::AVRO_UNION && resolved_node->leafAt(0)->type() == avro::AVRO_NULL)
                return Action::serializeAction(0, createDefaultNullUnionSerializeFn(), union_idx);

            return {};
        }
    }

    /// Found a column that matches the field name excactly.
    if (header.has(current_path))
    {
        auto target_column_idx = header.getPositionByName(current_path);
        const auto & column = header.getByPosition(target_column_idx);

        try
        {
            auto serialize_fn = createSerializeFn(node, column.type);
            return Action::serializeAction(static_cast<int>(target_column_idx), serialize_fn, union_idx);
        }
        catch (Exception & e)
        {
            e.addMessage("column " + column.name);
            throw;
        }
    }
    else if (node->type() == avro::AVRO_RECORD)
    {
        /// Instead of using a `tuple`, users can also flatten a record, like
        /// ```sql
        /// CREATE STREAM example (
        ///   record.field_1 <field_1_type>,
        ///   record.field_2 <field_2_type>,
        ///   ...
        /// )
        /// ```
        std::vector<AvroSchemaSerializer::Action> field_actions;
        field_actions.reserve(node->leaves());
        for (int i = 0; i < static_cast<int>(node->leaves()); ++i)
        {
            const auto & field_name = node->nameAt(i);
            const auto & field_node = node->leafAt(i);
            auto action = createAction(header, field_node, concatPath(current_path, field_name));
            if (!action)
                return {};

            field_actions.push_back(*action);
        }

        return AvroSchemaSerializer::Action::groupAction(std::move(field_actions), union_idx);
    }
    else if (node->type() == avro::AVRO_UNION)
    {
        /// Allow users to flatten the avro union. For example, for an avro union of `["int", "string"]` named `"foo"`,
        /// one can create columns like this
        /// ```sql
        /// CREATE STREAM example (
        ///   foo.int integer,
        ///   foo.string string
        /// )
        /// ```
        std::optional<AvroSchemaSerializer::Action> result;
        for (int i = 0; i < static_cast<int>(node->leaves()); ++i)
        {
            const auto & branch_node = node->leafAt(i);
            const auto & branch_name = nodeName(branch_node);

            auto action = createAction(header, branch_node, concatPath(current_path, branch_name), i);

            if (action)
            {
                if (result)
                    throw multiple_columns_to_union_exception(nodeName(node));

                result.swap(action);
            }
        }

        if (!result)
        {
            if (node->leafAt(0)->type() == avro::AVRO_NULL)
                return Action::serializeAction(0, createDefaultNullUnionSerializeFn());

            return {};
        }

        /// A union canot not have another union as its direct element, thus, no need to handle union_index in this case.
        return result;
    }
    else if (node->type() == avro::AVRO_ARRAY)
    {
        const auto & nested_node = node->leafAt(0);
        if (nested_node->type() == avro::AVRO_RECORD)
        {
            /// If header doesn't have column matches the node name and node is Array(Record),
            /// check if we have a flattened Nested table with such name.

            /// Create nested deserializer for each nested column.
            std::vector<SerializeFn> nested_serializers;
            std::vector<size_t> nested_indexes;
            for (int i = 0; i != static_cast<int>(nested_node->leaves()); ++i)
            {
                const auto & field_name = nested_node->nameAt(i);
                const auto & field = nested_node->leafAt(i);

                auto nested_column_index = header.tryGetPositionByName(Nested::concatenateName(current_path, field_name));
                if (!nested_column_index)
                {
                    if (field->type() == avro::AVRO_UNION && field->leafAt(0)->type() == avro::AVRO_NULL)
                    {
                        nested_serializers.emplace_back(createDefaultNullUnionSerializeFn());
                        nested_indexes.push_back(0); /// For null, the index doesn't matter, just need a valid value
                    }
                    else
                        return {};
                }
                else
                {
                    /// Nested columns have type of `array(<field_type>)`, thus we need to get the nested type in the `array`.
                    const auto & col = header.getByPosition(*nested_column_index);
                    const auto & array_type = assert_cast<const DataTypeArray &>(*col.type);
                    auto nested_serializer = createSerializeFn(field, array_type.getNestedType());
                    nested_serializers.emplace_back(nested_serializer);
                    nested_indexes.push_back(*nested_column_index);
                }
            }

            return Action::nestedAction(std::move(nested_indexes), std::move(nested_serializers), union_idx);
        }

        if (nested_node->type() == avro::AVRO_UNION)
        {
            /// For unions, try to match a flatten union type. For example, if a union has thress sub-types, like:
            /// ```json
            /// "name": "an_array_field",
            /// "type": "array",
            /// "items": ["string", "long", {
            ///   "type": "record",
            ///   "name": "inner_record",
            ///   "fields": [...]
            /// }]
            /// ```
            /// Then we allow the header contains one of the following columns: `an_array_field.string`, `an_array_field.long`, `an_array_field.inner_record`.
            std::vector<Action> nested_actions;
            for (int i = 0; i != static_cast<int>(nested_node->leaves()); ++i)
            {
                const auto & branch_node = nested_node->leafAt(i);
                const auto & branch_name = nodeName(branch_node);

                Block new_header;
                new_header.reserve(header.columns());

                for (const auto & col : header)
                {
                    if (!col.name.starts_with(concatPath(current_path, branch_name)))
                    {
                        new_header.insert(col);
                        break;
                    }

                    auto new_col = col.cloneEmpty();
                    const auto & array_type = assert_cast<const DataTypeArray &>(*col.type);
                    new_col.type = array_type.getNestedType();
                    new_header.insert(new_col);
                }

                /// Do not pass the union index to createAction, because we will flatten the group action later, which will lose the union index information.
                /// Thus, we create a separate action for encoding the union index.
                auto action = createAction(new_header, branch_node, concatPath(current_path, branch_name));

                if (action)
                {
                    if (!nested_actions.empty())
                        throw multiple_columns_to_union_exception(nodeName(nested_node));

                    nested_actions.push_back(Action::serializeAction(0, createUnionSerializeFn(i)));
                    if (action->type == Action::Group)
                        /// flatten the group action
                        nested_actions.insert(nested_actions.end(), action->actions.cbegin(), action->actions.cend());
                    else
                        nested_actions.push_back(*action);
                }
            }

            if (nested_actions.empty())
            {
                if (nested_node->leafAt(0)->type() == avro::AVRO_NULL)
                    nested_actions.push_back(Action::serializeAction(0, createDefaultNullUnionSerializeFn()));
                else
                    return {};
            }

            return Action::arrayAction(std::move(nested_actions), union_idx);
        }
    }

    return {};
}

AvroConfluentRowOutputFormat::AvroConfluentRowOutputFormat(
    WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & settings_)
    : IRowOutputFormat(header_, out_, params_, ProcessorID::AvroRowOutputFormatID), settings(settings_)
{
    assert(!settings.kafka_schema_registry.topic_name.empty());
    auto schema = KafkaSchemaRegistryForAvro::getOrCreate(settings)->getSchemaForTopic(settings.kafka_schema_registry.topic_name, settings.kafka_schema_registry.force_refresh_schema);
    schema_id = schema.first;
    serializer = std::make_unique<AvroSchemaSerializer>(std::move(schema.second), header_, out);
}

AvroConfluentRowOutputFormat::~AvroConfluentRowOutputFormat() = default;

void AvroConfluentRowOutputFormat::consume(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    for (size_t row = 0; row < num_rows; ++row)
    {
        KafkaSchemaRegistry::writeSchemaId(out, schema_id);
        write(columns, row);
        serializer->flush();
        if (params.callback)
            params.callback(columns, row);
        out.next(); /// Always one row per message.
    }
}

void AvroConfluentRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    serializer->serializeRow(columns, row_num);
}

AvroSchemaRowOutputFormat::AvroSchemaRowOutputFormat(
    WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSchemaInfo & schema_info, const FormatSettings & format_settings)
    : IRowOutputFormat(header_, out_, params_, ProcessorID::AvroRowOutputFormatID), settings(format_settings)
{
    auto schema = Avro::compileSchemaFromSchemaInfo(schema_info);
    serializer = std::make_unique<AvroSchemaSerializer>(std::move(schema), header_, out);
}

AvroSchemaRowOutputFormat::~AvroSchemaRowOutputFormat() = default;

void AvroSchemaRowOutputFormat::consume(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    for (size_t row = 0; row < num_rows; ++row)
    {
        write(columns, row);
        serializer->flush();
        if (params.callback)
            params.callback(columns, row);
        out.next(); /// Always one row per message.
    }
}

void AvroSchemaRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    serializer->serializeRow(columns, row_num);
}

}

#endif
