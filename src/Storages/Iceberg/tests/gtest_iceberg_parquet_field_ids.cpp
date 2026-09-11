#include "config.h"

#if USE_PARQUET

#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Formats/Impl/Parquet/Write.h>
#include <Storages/Iceberg/SchemaProcessor.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <gtest/gtest.h>

#include <map>
#include <optional>

using namespace DB;

namespace
{

/// Ids are deliberately non-sequential so that a match cannot come from positional numbering.
constexpr auto ICEBERG_SCHEMA = R"({
    "type": "struct",
    "schema-id": 0,
    "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "name", "required": false, "type": "string"},
        {"id": 3, "name": "tags", "required": true,
         "type": {"type": "list", "element-id": 10, "element": "string", "element-required": true}},
        {"id": 4, "name": "attrs", "required": true,
         "type": {"type": "map", "key-id": 11, "key": "string", "value-id": 12, "value": "int", "value-required": false}},
        {"id": 5, "name": "point", "required": true,
         "type": {"type": "struct", "fields": [
            {"id": 13, "name": "x", "required": true, "type": "double"},
            {"id": 14, "name": "y", "required": true, "type": "double"}]}},
        {"id": 6, "name": "history", "required": true,
         "type": {"type": "list", "element-id": 15, "element-required": true,
                  "element": {"type": "struct", "fields": [{"id": 16, "name": "ts", "required": true, "type": "long"}]}}}
    ]
})";

std::unordered_map<String, Int64> fieldIds()
{
    Poco::JSON::Parser parser;
    auto schema = parser.parse(ICEBERG_SCHEMA).extract<Poco::JSON::Object::Ptr>();
    return IcebergSchemaProcessor::traverseSchema(schema->getArray("fields"));
}

/// The Proton counterpart of ICEBERG_SCHEMA, as the Iceberg sink receives it.
Block sampleBlock()
{
    auto int32 = std::make_shared<DataTypeInt32>();
    auto int64 = std::make_shared<DataTypeInt64>();
    auto float64 = std::make_shared<DataTypeFloat64>();
    auto string = std::make_shared<DataTypeString>();

    auto name = makeNullable(string);
    auto tags = std::make_shared<DataTypeArray>(string);
    auto attrs = std::make_shared<DataTypeMap>(string, makeNullable(int32));
    auto point = std::make_shared<DataTypeTuple>(DataTypes{float64, float64}, Strings{"x", "y"});
    auto history = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{int64}, Strings{"ts"}));

    Block block;
    block.insert({int64->createColumn(), int64, "id"});
    block.insert({name->createColumn(), name, "name"});
    block.insert({tags->createColumn(), tags, "tags"});
    block.insert({attrs->createColumn(), attrs, "attrs"});
    block.insert({point->createColumn(), point, "point"});
    block.insert({history->createColumn(), history, "history"});
    return block;
}

using SchemaIndex = std::map<String, parquet::format::SchemaElement>;

void indexSchema(const Parquet::SchemaElements & schema, size_t & pos, const String & prefix, SchemaIndex & index)
{
    const auto & element = schema.at(pos++);
    auto path = prefix.empty() ? element.name : prefix + "." + element.name;
    index[path] = element;
    if (element.__isset.num_children)
        for (Int32 i = 0; i < element.num_children; ++i)
            indexSchema(schema, pos, path, index);
}

/// Parquet schema elements keyed by their dotted Parquet path, root excluded.
SchemaIndex indexSchema(const Parquet::SchemaElements & schema)
{
    SchemaIndex index;
    size_t pos = 1;
    while (pos < schema.size())
        indexSchema(schema, pos, "", index);
    return index;
}

std::optional<Int32> fieldIdOf(const SchemaIndex & index, const String & path)
{
    const auto & element = index.at(path);
    if (!element.__isset.field_id)
        return std::nullopt;
    return element.field_id;
}

}

TEST(IcebergParquetFieldIds, SchemaJSONToPaths)
{
    std::unordered_map<String, Int64> expected{
        {"id", 1},
        {"name", 2},
        {"tags", 3},
        {"tags.element", 10},
        {"attrs", 4},
        {"attrs.key", 11},
        {"attrs.value", 12},
        {"point", 5},
        {"point.x", 13},
        {"point.y", 14},
        {"history", 6},
        {"history.element", 15},
        {"history.element.ts", 16},
    };
    EXPECT_EQ(fieldIds(), expected);
}

TEST(IcebergParquetFieldIds, ParquetSchemaCarriesIds)
{
    auto index = indexSchema(Parquet::convertSchema(sampleBlock(), Parquet::WriteOptions{}, fieldIds()));

    EXPECT_EQ(fieldIdOf(index, "id"), 1);
    EXPECT_EQ(fieldIdOf(index, "name"), 2);
    EXPECT_EQ(fieldIdOf(index, "tags"), 3);
    EXPECT_EQ(fieldIdOf(index, "tags.list.element"), 10);
    EXPECT_EQ(fieldIdOf(index, "attrs"), 4);
    EXPECT_EQ(fieldIdOf(index, "attrs.key_value.key"), 11);
    EXPECT_EQ(fieldIdOf(index, "attrs.key_value.value"), 12);
    EXPECT_EQ(fieldIdOf(index, "point"), 5);
    EXPECT_EQ(fieldIdOf(index, "point.x"), 13);
    EXPECT_EQ(fieldIdOf(index, "point.y"), 14);
    EXPECT_EQ(fieldIdOf(index, "history"), 6);
    EXPECT_EQ(fieldIdOf(index, "history.list.element"), 15);
    EXPECT_EQ(fieldIdOf(index, "history.list.element.ts"), 16);

    /// Parquet-only wrapper groups have no Iceberg id.
    EXPECT_EQ(fieldIdOf(index, "tags.list"), std::nullopt);
    EXPECT_EQ(fieldIdOf(index, "attrs.key_value"), std::nullopt);
    EXPECT_EQ(fieldIdOf(index, "history.list"), std::nullopt);

    /// The map layout must be the same as before key/value were prepared directly.
    const auto & key_value = index.at("attrs.key_value");
    EXPECT_EQ(key_value.repetition_type, parquet::format::FieldRepetitionType::REPEATED);
    EXPECT_EQ(key_value.converted_type, parquet::format::ConvertedType::MAP_KEY_VALUE);
    EXPECT_EQ(key_value.num_children, 2);
    EXPECT_EQ(index.at("attrs.key_value.key").repetition_type, parquet::format::FieldRepetitionType::REQUIRED);
    EXPECT_EQ(index.at("attrs.key_value.value").repetition_type, parquet::format::FieldRepetitionType::OPTIONAL);
}

TEST(IcebergParquetFieldIds, NoIdsWithoutIcebergSchema)
{
    for (const auto & [path, element] : indexSchema(Parquet::convertSchema(sampleBlock(), Parquet::WriteOptions{})))
        EXPECT_FALSE(element.__isset.field_id) << path;
}

TEST(IcebergParquetFieldIds, MissingIdIsAnError)
{
    auto without_top_level = fieldIds();
    without_top_level.erase("attrs");
    EXPECT_THROW(Parquet::convertSchema(sampleBlock(), Parquet::WriteOptions{}, without_top_level), Exception);

    auto without_nested = fieldIds();
    without_nested.erase("point.y");
    EXPECT_THROW(Parquet::convertSchema(sampleBlock(), Parquet::WriteOptions{}, without_nested), Exception);
}

#endif
