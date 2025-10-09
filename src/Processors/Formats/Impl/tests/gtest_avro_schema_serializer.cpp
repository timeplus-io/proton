#include <Common/tests/gtest_global_context.h>
#include <Columns/ColumnMap.h>
#include <IO/WriteBufferFromString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <Processors/Formats/Impl/AvroConfluentRowOutputFormat.h>

#include <Compiler.hh>
#include <Decoder.hh>
#include <Generic.hh>
#include <Specific.hh>
#include <Stream.hh>
#include <Types.hh>

#include <gtest/gtest.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
}

}

using namespace DB;

namespace
{

namespace Fixture
{
const Int32 i32 = 1234567890;
const Int64 i64 = 1234567890987654321;
const Float32 f32 = 123.456f;
const Float64 f64 = 12345.09876;
const String str = "a string";
const String bytes = "some bytes";
const String uuid_string = "789a352d-896a-4c02-a95b-d0c9da945f4f";
const Int32 date = 19927;

UUID uuid()
{
    UUID uuid;
    parseUUID(reinterpret_cast<const UInt8 *>(uuid_string.data()), std::reverse_iterator<UInt8 *>(reinterpret_cast<UInt8 *>(&uuid) + 16));
    return uuid;
}

}

avro::GenericRecord getSerializationResult(const String & schema, const Block & header, Chunk data)
{
    auto valid_schema = avro::compileJsonSchemaFromString(schema);

    WriteBufferFromOwnString wb{};
    AvroSchemaSerializer ser{valid_schema, header, wb};
    ser.serializeRow(data.getColumns(), 0);
    ser.flush();

    wb.finalize();
    auto bytes = wb.stringRef();

    auto dec = avro::validatingDecoder(valid_schema, avro::binaryDecoder());
    auto in = avro::memoryInputStream(reinterpret_cast<const uint8_t *>(bytes.data), bytes.size);
    dec->init(*in);

    avro::GenericDatum datum(valid_schema);
    avro::decode(*dec, datum);

    assert(datum.type() == avro::AVRO_RECORD);

    return datum.value<avro::GenericRecord>();
}

}

TEST(AvroSchemaSerializer, Int)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_int",
      "type": "int"
    }
  ]
}
    )";

    /// Types that map to Avro int
    for (const auto & data_type : std::vector<String>{"uint8", "int8", "uint16", "int16", "uint32", "int32"})
    {
        Block header{
            {DataTypeFactory::instance().get(data_type), "f_int"},
        };

        auto columns = header.mutateColumns();
        columns.at(0)->insert(123);

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        EXPECT_EQ(root_record.field("f_int").value<Int32>(), 123);
    }
}

TEST(AvroSchemaSerializer, Long)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_long",
      "type": "long"
    }
  ]
}
    )";

    /// Types that map to Avro long
    for (const auto & data_type : std::vector<String>{"uint64", "int64"})
    {
        Block header{
            {DataTypeFactory::instance().get(data_type), "f_long"},
        };

        auto columns = header.mutateColumns();
        columns.at(0)->insert(1234567890987654321);

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        EXPECT_EQ(root_record.field("f_long").value<Int64>(), 1234567890987654321);
    }
}

TEST(AvroSchemaSerializer, OtherPrimitiveTypes)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_boolean",
      "type": "boolean"
    },
    {
      "name": "f_float",
      "type": "float"
    },
    {
      "name": "f_double",
      "type": "double"
    },
    {
      "name": "f_bytes",
      "type": "bytes"
    },
    {
      "name": "f_string",
      "type": "string"
    }
  ]
}
    )";

    Block header{
        {DataTypeFactory::instance().get("bool"), "f_boolean"},
        {std::make_shared<DataTypeFloat32>(), "f_float"},
        {std::make_shared<DataTypeFloat64>(), "f_double"},
        {std::make_shared<DataTypeString>(), "f_bytes"},
        {std::make_shared<DataTypeString>(), "f_string"},
    };

    auto columns = header.mutateColumns();
    columns.at(0)->insert(true);
    columns.at(1)->insert(Fixture::f32);
    columns.at(2)->insert(Fixture::f64);
    columns.at(3)->insert(Fixture::bytes);
    columns.at(4)->insert(Fixture::str);

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});

    EXPECT_TRUE(root_record.field("f_boolean").value<bool>());
    EXPECT_EQ(root_record.field("f_float").value<Float32>(), Fixture::f32);
    EXPECT_EQ(root_record.field("f_double").value<Float64>(), Fixture::f64);
    EXPECT_EQ(root_record.field("f_string").value<String>(), Fixture::str);

    auto the_bytes = root_record.field("f_bytes").value<std::vector<uint8_t>>();
    String bytes_string{reinterpret_cast<char *>(the_bytes.data()), the_bytes.size()};
    EXPECT_EQ(bytes_string, Fixture::bytes);
}

TEST(AvroSchemaSerializer, Fixed)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_fixed",
      "type": {"name": "fixed10", "type": "fixed", "size": 10}
    }
  ]
}
    )";

    Block header{
        {DataTypeFactory::instance().get("fixed_string(10)"), "f_fixed"},
    };

    auto columns = header.mutateColumns();
    columns.at(0)->insert("0123456789");

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    auto fixed_value = root_record.field("f_fixed").value<avro::GenericFixed>().value();
    EXPECT_EQ(String(reinterpret_cast<char *>(fixed_value.data()), fixed_value.size()), "0123456789");
}

TEST(AvroSchemaSerializer, Enum)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "an_enum",
      "type": {"name": "colors", "type": "enum", "symbols": ["red", "blue", "green"]}
    }
  ]
}
    )";


    Block header{
        {DataTypeFactory::instance().get("enum('red' = 1, 'blue' = 2, 'green' = 3)"), "an_enum"},
    };

    auto columns = header.mutateColumns();
    columns.at(0)->insert(2);

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    auto enum_symbol = root_record.field("an_enum").value<avro::GenericEnum>().symbol();
    EXPECT_EQ(enum_symbol, "blue");
}

TEST(AvroSchemaSerializer, SimpleUnion)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "int_or_float_or_string",
      "type": ["int", "float", "string"]
    }
  ]
}
    )";

    /// Use int
    {
        { /// Use just the field name
            Block header{
                {std::make_shared<DataTypeInt32>(), "int_or_float_or_string"},
            };

            auto columns = header.mutateColumns();
            columns.at(0)->insert(Fixture::i32);

            auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
            EXPECT_EQ(root_record.field("int_or_float_or_string").value<Int32>(), Fixture::i32);
        }
        { /// Use expanded name
            Block header{
                {std::make_shared<DataTypeInt32>(), "int_or_float_or_string.int"},
            };

            auto columns = header.mutateColumns();
            columns.at(0)->insert(Fixture::i32);

            auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
            EXPECT_EQ(root_record.field("int_or_float_or_string").value<Int32>(), Fixture::i32);
        }
    }

    /// Use float
    {
        {  /// Use just the field name
            Block header{
                {std::make_shared<DataTypeFloat32>(), "int_or_float_or_string"},
            };

            auto columns = header.mutateColumns();
            columns.at(0)->insert(Fixture::f32);

            auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
            EXPECT_EQ(root_record.field("int_or_float_or_string").value<Float32>(), Fixture::f32);
        }
        {  /// Use expanded name
            Block header{
                {std::make_shared<DataTypeFloat32>(), "int_or_float_or_string.float"},
            };

            auto columns = header.mutateColumns();
            columns.at(0)->insert(Fixture::f32);

            auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
            EXPECT_EQ(root_record.field("int_or_float_or_string").value<Float32>(), Fixture::f32);
        }
    }

    /// Use string
    {
        { /// Use just the field name
            Block header{
                {std::make_shared<DataTypeString>(), "int_or_float_or_string"},
            };

            auto columns = header.mutateColumns();
            columns.at(0)->insert(Fixture::str);

            auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
            EXPECT_EQ(root_record.field("int_or_float_or_string").value<String>(), Fixture::str);
        }
        { /// Use expanded name
            Block header{
                {std::make_shared<DataTypeString>(), "int_or_float_or_string.string"},
            };

            auto columns = header.mutateColumns();
            columns.at(0)->insert(Fixture::str);

            auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
            EXPECT_EQ(root_record.field("int_or_float_or_string").value<String>(), Fixture::str);
        }
    }
}

TEST(AvroSchemaSerializer, DeepNestedUnion)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "level_1",
      "type": ["int", {
        "name": "lv1_record",
        "type": "record",
        "fields": [{
          "name": "level_2",
          "type": ["long", {
            "name": "lv2_record",
            "type": "record",
            "fields": [{
              "name": "level_3",
              "type": ["float", {
                "name": "lv3_record",
                "type": "record",
                "fields": [{"name": "level_4", "type": ["null", "string"]}]
              }]
            }]
          }]
        }]
      }]
    }
  ]
}
    )";

    { /// Use just the field name
        Block header{
            {DataTypeFactory::instance().get("tuple(level_2 tuple(level_3 tuple(level_4 nullable(string))))"), "level_1"},
        };

        auto columns = header.mutateColumns();
        Tuple lv3{Fixture::str};
        Tuple lv2{};
        lv2.push_back(lv3);
        Tuple lv1{};
        lv1.push_back(lv2);
        columns.at(0)->insert(lv1);

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        EXPECT_EQ(root_record.field("level_1").value<avro::GenericRecord>()
                  .field("level_2").value<avro::GenericRecord>()
                  .field("level_3").value<avro::GenericRecord>()
                  .field("level_4").value<String>(), Fixture::str);
    }

    { /// Expend all the names
        Block header{
            {DataTypeFactory::instance().get("string"), "level_1.lv1_record.level_2.lv2_record.level_3.lv3_record.level_4.string"},
        };

        auto columns = header.mutateColumns();
        columns.at(0)->insert(Fixture::str);

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        EXPECT_EQ(root_record.field("level_1").value<avro::GenericRecord>()
                  .field("level_2").value<avro::GenericRecord>()
                  .field("level_3").value<avro::GenericRecord>()
                  .field("level_4").value<String>(), Fixture::str);
    }
}

TEST(AvroSchemaSerializer, UnionNullable)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_string",
      "type": "string"
    },
    {
      "name": "f_nullable_int",
      "type": ["null", "int"]
    }
  ]
}
    )";

    { /// Header is presented for the union field
        Block header{
            {std::make_shared<DataTypeString>(), "f_string"},
            {DataTypeFactory::instance().get("nullable(int32)"), "f_nullable_int"},
        };

        { /// Non-null value
            auto columns = header.mutateColumns();
            columns.at(0)->insert(Fixture::str);
            columns.at(1)->insert(Fixture::i32);

            auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
            EXPECT_EQ(root_record.field("f_string").value<String>(), Fixture::str);
            EXPECT_EQ(root_record.field("f_nullable_int").value<Int32>(), Fixture::i32);
        }

        { /// null
            auto columns = header.mutateColumns();
            columns.at(0)->insert(Fixture::str);
            columns.at(1)->insert(Null{});

            auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
            EXPECT_EQ(root_record.field("f_string").value<String>(), Fixture::str);
            EXPECT_EQ(root_record.field("f_nullable_int").type(), avro::AVRO_NULL);
        }
    }

    { /// Header is missing for the union field
        Block header{
            {std::make_shared<DataTypeString>(), "f_string"},
        };

        auto columns = header.mutateColumns();
        columns.at(0)->insert(Fixture::str);

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        EXPECT_EQ(root_record.field("f_string").value<String>(), Fixture::str);
        EXPECT_EQ(root_record.field("f_nullable_int").type(), avro::AVRO_NULL);
    }
}

TEST(AvroSchemaSerializer, Record)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_record",
      "type": {
        "name": "inner_record",
        "type": "record",
        "fields": [
          {"name": "f_int", "type": "int"},
          {"name": "f_long", "type": "long"}
        ]
      }
    },
    {
      "name": "another_record",
      "type": {
        "name": "another_inner_record",
        "type": "record",
        "fields": [
          {"name": "f_float", "type": "float"},
          {"name": "f_string", "type": "string"}
        ]
      }
    }
  ]
}
    )";

    /// Two flavors:
    Block header{
        /// * flatterned
        {std::make_shared<DataTypeInt32>(), "f_record.f_int"},
        {std::make_shared<DataTypeInt64>(), "f_record.f_long"},
        /// * tuple
        {DataTypeFactory::instance().get("tuple(f_float float32, f_string string)"), "another_record"},
    };

    auto columns = header.mutateColumns();
    columns.at(0)->insert(Fixture::i32);
    columns.at(1)->insert(Fixture::i64);
    columns.at(2)->insert(Tuple{ Fixture::f32, Fixture::str });

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});

    {
        auto inner_record = root_record.field("f_record").value<avro::GenericRecord>();
        EXPECT_EQ(inner_record.field("f_int").value<Int32>(), Fixture::i32);
        EXPECT_EQ(inner_record.field("f_long").value<Int64>(), Fixture::i64);
    }

    {
        auto inner_record = root_record.field("another_record").value<avro::GenericRecord>();
        EXPECT_EQ(inner_record.field("f_float").value<Float32>(), Fixture::f32);
        EXPECT_EQ(inner_record.field("f_string").value<String>(), Fixture::str);
    }
}

TEST(AvroSchemaSerializer, LogicalTypes)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_uuid",
      "type": {"type": "string", "logicalType": "uuid"}
    },
    {
      "name": "f_date",
      "type": {"type": "int", "logicalType": "date"}
    },
    {
      "name": "f_date32",
      "type": {"type": "int", "logicalType": "date"}
    },
    {
      "name": "f_datetime64_millis",
      "type": {"type": "long", "logicalType": "timestamp-millis"}
    },
    {
      "name": "f_datetime64_micros",
      "type": {"type": "long", "logicalType": "timestamp-micros"}
    }

  ]
}
    )";

    Block header{
        {std::make_shared<DataTypeUUID>(), "f_uuid"},
        {std::make_shared<DataTypeDate>(), "f_date"},
        {std::make_shared<DataTypeDate32>(), "f_date32"},
        {std::make_shared<DataTypeDateTime64>(3), "f_datetime64_millis"},
        {std::make_shared<DataTypeDateTime64>(6), "f_datetime64_micros"},
    };

    auto columns = header.mutateColumns();
    columns.at(0)->insert(Fixture::uuid());
    columns.at(1)->insert(Fixture::date);
    columns.at(2)->insert(19928);
    assert_cast<ColumnDecimal<DateTime64> &>(*columns.at(3)).insertValue(31884399415359);
    assert_cast<ColumnDecimal<DateTime64> &>(*columns.at(4)).insertValue(31884399415360);

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    EXPECT_EQ(root_record.field("f_uuid").value<String>(), Fixture::uuid_string);
    EXPECT_EQ(root_record.field("f_date").value<Int32>(), Fixture::date);
    EXPECT_EQ(root_record.field("f_date32").value<Int32>(), 19928);
    EXPECT_EQ(root_record.field("f_datetime64_millis").value<Int64>(), 31884399415359);
    EXPECT_EQ(root_record.field("f_datetime64_micros").value<Int64>(), 31884399415360);
}

TEST(AvroSchemaSerializer, LogicalTypesUUIDNegative)
{
    std::vector<String> schemas = {
        /// missing logicalType
        R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_uuid",
      "type": "string"
    }

  ]
}
        )",
        /// wrong type
        R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_uuid",
      "type": {"type": "bytes", "logicalType": "uuid"}
    }

  ]
}
        )",
    };

    for (const auto & schema : schemas)
    {
        Block header{
            {std::make_shared<DataTypeUUID>(), "f_uuid"},
        };

        int ex_code{0};
        String ex_msg;
        try
        {
            /// It should fail before even reading data, thus do not need to populate any data here.
            getSerializationResult(schema, header, {});
        }
        catch (const Exception & ex)
        {
            ex_code = ex.code();
            ex_msg = ex.message();
        }
        EXPECT_EQ(ex_code, ErrorCodes::TYPE_MISMATCH);
        EXPECT_TRUE(ex_msg.find("string with logical type `uuid`") != std::string::npos);
    }
}

TEST(AvroSchemaSerializer, LogicalTypesDateNegative)
{
    std::vector<String> schemas = {
        /// missing logicalType
        R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_date",
      "type": "int"
    }

  ]
}
        )",
        /// wrong type
        R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_date",
      "type": {"type": "long", "logicalType": "date"}
    }

  ]
}
        )"
    };

    for (const auto & schema : schemas)
        for (const auto & data_type : std::vector<String>{"date", "date32"})
        {
            Block header{
                {DataTypeFactory::instance().get(data_type), "f_date"},
            };

            int ex_code{0};
            String ex_msg;
            try
            {
                /// It should fail before even reading data, thus do not need to populate any data here.
                getSerializationResult(schema, header, {});
            }
            catch (const Exception & ex)
            {
                ex_code = ex.code();
                ex_msg = ex.message();
            }
            EXPECT_EQ(ex_code, ErrorCodes::TYPE_MISMATCH);
            EXPECT_TRUE(ex_msg.find("int with logical type `date`") != std::string::npos);
        }
}

TEST(AvroSchemaSerializer, LogicalTypesDateTime64Scale3Negative)
{
    std::vector<String> schemas = {
        /// missing logicalType
        R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_datetime64_millis",
      "type": "long"
    }

  ]
}
        )",
        /// wrong type
        R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_datetime64_millis",
      "type": {"type": "int", "logicalType": "timestamp-millis"}
    }

  ]
}
        )"
    };

    std::vector expected_error_messages{
        "Type `datetime64` with scale 3 requires `timestamp-millis` logical type",
        "Type `datetime64(3)` must map to Avro type long, but got int"
    };

    for (size_t i = 0; const auto & schema : schemas)
    {
        Block header{
            {std::make_shared<DataTypeDateTime64>(3), "f_datetime64_millis"},
        };

        int ex_code{0};
        String ex_msg;
        try
        {
            /// It should fail before even reading data, thus do not need to populate any data here.
            getSerializationResult(schema, header, {});
        }
        catch (const Exception & ex)
        {
            ex_code = ex.code();
            ex_msg = ex.message();
        }
        EXPECT_EQ(ex_code, ErrorCodes::TYPE_MISMATCH);
        EXPECT_TRUE(ex_msg.find(expected_error_messages[i++]) != std::string::npos);
    }
}

TEST(AvroSchemaSerializer, LogicalTypesDateTime64Scale6Negative)
{
    std::vector<String> schemas = {
        /// missing logicalType
        R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_datetime64_micros",
      "type": "long"
    }

  ]
}
        )",
        /// wrong type
        R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "f_datetime64_micros",
      "type": {"type": "int", "logicalType": "timestamp-micros"}
    }

  ]
}
        )"
    };

    std::vector expected_error_messages{
        "Type `datetime64` with scale 6 requires `timestamp-micros` logical type",
        "Type `datetime64(6)` must map to Avro type long, but got int"
    };

    for (size_t i = 0; const auto & schema : schemas)
    {
        Block header{
            {std::make_shared<DataTypeDateTime64>(6), "f_datetime64_micros"},
        };

        int ex_code{0};
        String ex_msg;
        try
        {
            /// It should fail before even reading data, thus do not need to populate any data here.
            getSerializationResult(schema, header, {});
        }
        catch (const Exception & ex)
        {
            ex_code = ex.code();
            ex_msg = ex.message();
        }
        EXPECT_EQ(ex_code, ErrorCodes::TYPE_MISMATCH);
        EXPECT_TRUE(ex_msg.find(expected_error_messages[i++]) != std::string::npos);
    }
}

TEST(AvroSchemaSerializer, Map)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "int_map",
      "type": {
        "type": "map",
        "values": "int"
      }
    }
  ]
}
    )";

    Block header{
        {DataTypeFactory::instance().get("map(string, int32)"), "int_map"},
    };

    auto columns = header.mutateColumns();
    {
        ColumnMap & column_map = assert_cast<ColumnMap &>(*columns.at(0));
        ColumnArray & column_array = column_map.getNestedColumn();
        ColumnArray::Offsets & offsets = column_array.getOffsets();
        ColumnTuple & nested_columns = column_map.getNestedData();
        IColumn & keys_column = nested_columns.getColumn(0);
        IColumn & values_column = nested_columns.getColumn(1);

        keys_column.insert("key1");
        values_column.insert(1);
        keys_column.insert("key2");
        values_column.insert(2);
        offsets.push_back(offsets.back() + 2);
    }

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    auto int_map = root_record.field("int_map").value<avro::GenericMap>();
    const auto & kv_pairs = int_map.value();

    EXPECT_EQ(kv_pairs.size(), 2);
    for (const auto & kv : kv_pairs)
        if (kv.first == "key1")
            EXPECT_EQ(kv.second.value<Int32>(), 1);
        else if (kv.first == "key2")
            EXPECT_EQ(kv.second.value<Int32>(), 2);
        else
            FAIL() << "Unexpected map key " << kv.first;
}

TEST(AvroSchemaSerializer, Array)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "int_array",
      "type": {
        "type": "array",
        "items": "int"
      }
    }
  ]
}
    )";

    Block header{
        {DataTypeFactory::instance().get("array(int32)"), "int_array"},
    };

    auto columns = header.mutateColumns();
    {
        ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
        ColumnArray::Offsets & offsets = column_array.getOffsets();
        IColumn & nested_column = column_array.getData();

        nested_column.insert(1);
        nested_column.insert(2);
        offsets.push_back(offsets.back() + 2);
    }

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    auto int_array = root_record.field("int_array").value<avro::GenericArray>();
    const auto & values = int_array.value();

    EXPECT_EQ(values.size(), 2);
    for (Int32 i = 1; const auto & v : values)
    {
        EXPECT_EQ(v.type(), avro::AVRO_INT);
        EXPECT_EQ(v.value<Int32>(), i++);
    }
}

TEST(AvroSchemaSerializer, ArrayOfNullable)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "int_array",
      "type": {
        "type": "array",
        "items": ["null", "int"]
      }
    }
  ]
}
    )";

    Block header{
        {DataTypeFactory::instance().get("array(nullable(int32))"), "int_array"},
    };

    auto columns = header.mutateColumns();
    {
        ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
        ColumnArray::Offsets & offsets = column_array.getOffsets();
        IColumn & nested_column = column_array.getData();

        nested_column.insert(1);
        nested_column.insert(Null{});
        nested_column.insert(2);
        offsets.push_back(offsets.back() + 3);
    }

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    auto int_array = root_record.field("int_array").value<avro::GenericArray>();
    const auto & values = int_array.value();

    EXPECT_EQ(values.size(), 3);

    EXPECT_EQ(values.at(0).type(), avro::AVRO_INT);
    EXPECT_EQ(values.at(0).value<Int32>(), 1);

    EXPECT_EQ(values.at(1).type(), avro::AVRO_NULL);

    EXPECT_EQ(values.at(2).type(), avro::AVRO_INT);
    EXPECT_EQ(values.at(2).value<Int32>(), 2);
}

TEST(AvroSchemaSerializer, ArrayOfUnion)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "int_or_string_array",
      "type": {
        "type": "array",
        "items": ["int", "string"]
      }
    }
  ]
}
    )";

    /// "int" branch
    { /// * just use the field name
        Block header{
            {DataTypeFactory::instance().get("array(int32)"), "int_or_string_array"},
        };

        auto columns = header.mutateColumns();
        {
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(1);
            nested_column.insert(2);
            offsets.push_back(offsets.back() + 2);
        }

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        auto int_array = root_record.field("int_or_string_array").value<avro::GenericArray>();
        const auto & values = int_array.value();

        EXPECT_EQ(values.size(), 2);
        for (Int32 i = 1; const auto & v : values)
        {
            EXPECT_EQ(v.type(), avro::AVRO_INT);
            EXPECT_EQ(v.value<Int32>(), i++);
        }
    }

    { /// * use expanded name
        Block header{
            {DataTypeFactory::instance().get("array(int32)"), "int_or_string_array.int"},
        };

        auto columns = header.mutateColumns();
        {
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(1);
            nested_column.insert(2);
            offsets.push_back(offsets.back() + 2);
        }

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        auto int_array = root_record.field("int_or_string_array").value<avro::GenericArray>();
        const auto & values = int_array.value();

        EXPECT_EQ(values.size(), 2);
        for (Int32 i = 1; const auto & v : values)
        {
            EXPECT_EQ(v.type(), avro::AVRO_INT);
            EXPECT_EQ(v.value<Int32>(), i++);
        }
    }

    /// "string" branch
    { /// * just use the field name
        Block header{
            {DataTypeFactory::instance().get("array(string)"), "int_or_string_array"},
        };

        auto columns = header.mutateColumns();
        {
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert("one");
            nested_column.insert("two");
            offsets.push_back(offsets.back() + 2);
        }

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        auto string_array = root_record.field("int_or_string_array").value<avro::GenericArray>();
        const auto & values = string_array.value();

        EXPECT_EQ(values.size(), 2);

        EXPECT_EQ(values.at(0).type(), avro::AVRO_STRING);
        EXPECT_EQ(values.at(0).value<String>(), "one");

        EXPECT_EQ(values.at(1).type(), avro::AVRO_STRING);
        EXPECT_EQ(values.at(1).value<String>(), "two");
    }

    { /// * use expanded name
        Block header{
            {DataTypeFactory::instance().get("array(string)"), "int_or_string_array.string"},
        };

        auto columns = header.mutateColumns();
        {
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert("one");
            nested_column.insert("two");
            offsets.push_back(offsets.back() + 2);
        }

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        auto string_array = root_record.field("int_or_string_array").value<avro::GenericArray>();
        const auto & values = string_array.value();

        EXPECT_EQ(values.size(), 2);

        EXPECT_EQ(values.at(0).type(), avro::AVRO_STRING);
        EXPECT_EQ(values.at(0).value<String>(), "one");

        EXPECT_EQ(values.at(1).type(), avro::AVRO_STRING);
        EXPECT_EQ(values.at(1).value<String>(), "two");
    }
}

TEST(AvroSchemaSerializer, ArrayOfRecord)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "record_array",
      "type": {
        "type": "array",
        "items": {
          "name": "inner_record",
          "type": "record",
          "fields": [
            {"name": "f_int", "type": "int"},
            {"name": "f_string", "type": "string"},
            {"name": "f_uuid", "type": {"type": "string", "logicalType": "uuid"}}
          ]
        }
      }
    }
  ]
}
    )";

    { /// A single tuple
        Block header{
            {DataTypeFactory::instance().get("array(tuple(f_int int32, f_string string, f_uuid uuid))"), "record_array"},
        };

        auto columns = header.mutateColumns();
        {
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(Tuple{ Fixture::i32, Fixture::str, Fixture::uuid() });
            nested_column.insert(Tuple{ Fixture::i32, Fixture::str, Fixture::uuid() });
            offsets.push_back(offsets.back() + 2);
        }

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        auto record_array = root_record.field("record_array").value<avro::GenericArray>();
        const auto & values = record_array.value();

        EXPECT_EQ(values.size(), 2);

        for (size_t i = 0; i < 2; i++)
        {
            EXPECT_EQ(values.at(i).type(), avro::AVRO_RECORD);
            const auto & record = values.at(i).value<avro::GenericRecord>();

            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32);
            EXPECT_EQ(record.field("f_string").value<String>(), Fixture::str);
            EXPECT_EQ(record.field("f_uuid").value<String>(), Fixture::uuid_string);
        }
    }

    { /// Flattened names
        Block header{
            {DataTypeFactory::instance().get("array(int32)"), "record_array.f_int"},
            {DataTypeFactory::instance().get("array(string)"), "record_array.f_string"},
            {DataTypeFactory::instance().get("array(uuid)"), "record_array.f_uuid"},
        };

        auto columns = header.mutateColumns();
        { /// f_int
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(Fixture::i32);
            nested_column.insert(Fixture::i32);
            offsets.push_back(offsets.back() + 2);
        }
        { /// f_string
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(1));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(Fixture::str);
            nested_column.insert(Fixture::str);
            offsets.push_back(offsets.back() + 2);
        }
        { /// f_uuid
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(2));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(Fixture::uuid());
            nested_column.insert(Fixture::uuid());
            offsets.push_back(offsets.back() + 2);
        }

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        auto record_array = root_record.field("record_array").value<avro::GenericArray>();
        const auto & values = record_array.value();

        EXPECT_EQ(values.size(), 2);

        for (size_t i = 0; i < 2; i++)
        {
            EXPECT_EQ(values.at(i).type(), avro::AVRO_RECORD);
            const auto & record = values.at(i).value<avro::GenericRecord>();

            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32);
            EXPECT_EQ(record.field("f_string").value<String>(), Fixture::str);
            EXPECT_EQ(record.field("f_uuid").value<String>(), Fixture::uuid_string);
        }
    }
}

TEST(AvroSchemaSerializer, ArrayOfUnionRecord)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "record_array",
      "type": {
        "type": "array",
        "items": ["null", {
          "name": "inner_record",
          "type": "record",
          "fields": [
            {"name": "f_int", "type": "int"},
            {"name": "f_string", "type": "string"},
            {"name": "f_uuid", "type": {"type": "string", "logicalType": "uuid"}}
          ]
        }]
      }
    }
  ]
}
    )";

    { /// A single tuple
        Block header{
            {DataTypeFactory::instance().get("array(tuple(f_int int32, f_string string, f_uuid uuid))"), "record_array"},
        };

        auto columns = header.mutateColumns();
        {
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(Tuple{ Fixture::i32, Fixture::str, Fixture::uuid() });
            nested_column.insert(Tuple{ Fixture::i32, Fixture::str, Fixture::uuid() });
            offsets.push_back(offsets.back() + 2);
        }

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        auto record_array = root_record.field("record_array").value<avro::GenericArray>();
        const auto & values = record_array.value();

        EXPECT_EQ(values.size(), 2);

        for (size_t i = 0; i < 2; i++)
        {
            EXPECT_EQ(values.at(i).type(), avro::AVRO_RECORD);
            const auto & record = values.at(i).value<avro::GenericRecord>();

            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32);
            EXPECT_EQ(record.field("f_string").value<String>(), Fixture::str);
            EXPECT_EQ(record.field("f_uuid").value<String>(), Fixture::uuid_string);
        }
    }

    { /// A single tuple, but expanded with union name
        Block header{
            {DataTypeFactory::instance().get("array(tuple(f_int int32, f_string string, f_uuid uuid))"), "record_array.inner_record"},
        };

        auto columns = header.mutateColumns();
        {
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(Tuple{ Fixture::i32, Fixture::str, Fixture::uuid() });
            nested_column.insert(Tuple{ Fixture::i32, Fixture::str, Fixture::uuid() });
            offsets.push_back(offsets.back() + 2);
        }

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        auto record_array = root_record.field("record_array").value<avro::GenericArray>();
        const auto & values = record_array.value();

        EXPECT_EQ(values.size(), 2);

        for (size_t i = 0; i < 2; i++)
        {
            EXPECT_EQ(values.at(i).type(), avro::AVRO_RECORD);
            const auto & record = values.at(i).value<avro::GenericRecord>();

            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32);
            EXPECT_EQ(record.field("f_string").value<String>(), Fixture::str);
            EXPECT_EQ(record.field("f_uuid").value<String>(), Fixture::uuid_string);
        }
    }

    { /// Flattened names
        Block header{
            {DataTypeFactory::instance().get("array(int32)"), "record_array.inner_record.f_int"},
            {DataTypeFactory::instance().get("array(string)"), "record_array.inner_record.f_string"},
            {DataTypeFactory::instance().get("array(uuid)"), "record_array.inner_record.f_uuid"},
        };

        auto columns = header.mutateColumns();
        { /// f_int
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(Fixture::i32);
            nested_column.insert(Fixture::i32);
            offsets.push_back(offsets.back() + 2);
        }
        { /// f_string
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(1));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(Fixture::str);
            nested_column.insert(Fixture::str);
            offsets.push_back(offsets.back() + 2);
        }
        { /// f_uuid
            ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(2));
            ColumnArray::Offsets & offsets = column_array.getOffsets();
            IColumn & nested_column = column_array.getData();

            nested_column.insert(Fixture::uuid());
            nested_column.insert(Fixture::uuid());
            offsets.push_back(offsets.back() + 2);
        }

        auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
        auto record_array = root_record.field("record_array").value<avro::GenericArray>();
        const auto & values = record_array.value();

        EXPECT_EQ(values.size(), 2);

        for (size_t i = 0; i < 2; i++)
        {
            EXPECT_EQ(values.at(i).type(), avro::AVRO_RECORD);
            const auto & record = values.at(i).value<avro::GenericRecord>();

            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32);
            EXPECT_EQ(record.field("f_string").value<String>(), Fixture::str);
            EXPECT_EQ(record.field("f_uuid").value<String>(), Fixture::uuid_string);
        }
    }
}

TEST(AvroSchemaSerializer, ArrayOfMapOfUnionRecord)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "record_array",
      "type": {
        "type": "array",
        "items": {
          "type": "map",
          "values": ["null", {
            "name": "inner_record",
            "type": "record",
            "fields": [
              {"name": "f_int", "type": "int"},
              {"name": "f_string", "type": "string"},
              {"name": "f_uuid", "type": {"type": "string", "logicalType": "uuid"}}
            ]
          }]
        }
      }
    }
  ]
}
    )";

    Block header{
        {DataTypeFactory::instance().get("array(map(string, tuple(f_int int32, f_string string, f_uuid uuid)))"), "record_array"},
    };

    auto columns = header.mutateColumns();
    {
        ColumnArray & column_array = assert_cast<ColumnArray &>(*columns.at(0));
        ColumnArray::Offsets & offsets = column_array.getOffsets();
        IColumn & nested_column = column_array.getData();

        for (size_t i = 0; i < 2; i++)
            nested_column.insert(Map{
                Tuple{fmt::format("key_{}_0", i), Tuple{ Fixture::i32, Fixture::str, Fixture::uuid() }},
                Tuple{fmt::format("key_{}_1", i), Tuple{ Fixture::i32, Fixture::str, Fixture::uuid() }}
            });

        offsets.push_back(offsets.back() + 2);
    }

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    auto record_array = root_record.field("record_array").value<avro::GenericArray>();
    const auto & values = record_array.value();

    EXPECT_EQ(values.size(), 2);

    for (size_t i = 0; i < 2; i++)
    {
        EXPECT_EQ(values.at(i).type(), avro::AVRO_MAP);
        const auto & map = values.at(i).value<avro::GenericMap>();

        EXPECT_EQ(map.value().size(), 2);

        for (size_t j = 0; j < 2; j++)
        {
            const auto & value = map.value().at(j);
            const auto & key = value.first;
            EXPECT_TRUE(key == fmt::format("key_{}_0", i) || key == fmt::format("key_{}_1", i));
            EXPECT_EQ(value.second.type(), avro::AVRO_RECORD);

            const auto & record = value.second.value<avro::GenericRecord>();
            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32);
            EXPECT_EQ(record.field("f_string").value<String>(), Fixture::str);
            EXPECT_EQ(record.field("f_uuid").value<String>(), Fixture::uuid_string);
        }
    }
}

TEST(AvroSchemaSerializer, MapOfRecord)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "record_map",
      "type": {
        "type": "map",
        "values": {
          "name": "inner_record",
          "type": "record",
          "fields": [
            {"name": "f_int", "type": "int"},
            {"name": "f_string", "type": "string"},
            {"name": "f_nullable_date", "type": ["null", {"type": "int", "logicalType": "date"}]}
          ]
        }
      }
    }
  ]
}
    )";

    Block header{
        {DataTypeFactory::instance().get("map(string, tuple(f_int int32, f_string string, f_nullable_date nullable(date)))"), "record_map"},
    };

    auto columns = header.mutateColumns();
    {
        ColumnMap & column_map = assert_cast<ColumnMap &>(*columns.at(0));
        ColumnArray & column_array = column_map.getNestedColumn();
        ColumnArray::Offsets & offsets = column_array.getOffsets();
        ColumnTuple & nested_columns = column_map.getNestedData();
        IColumn & keys_column = nested_columns.getColumn(0);
        IColumn & values_column = nested_columns.getColumn(1);

        for (size_t i = 0; i < 2; i++)
        {
            keys_column.insert(fmt::format("key_{}", i));
            values_column.insert(Tuple{Fixture::i32 + i, fmt::format("{}_{}", Fixture::str, i), Fixture::date + i});
        }
        offsets.push_back(offsets.back() + 2);
    }

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    auto record_map = root_record.field("record_map").value<avro::GenericMap>();
    const auto & kv_pairs = record_map.value();

    EXPECT_EQ(kv_pairs.size(), 2);

    for (const auto & kv : kv_pairs)
    {
        EXPECT_EQ(kv.second.type(), avro::AVRO_RECORD);
        const auto & record = kv.second.value<avro::GenericRecord>();
        if (kv.first == "key_0")
        {
            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32);
            EXPECT_EQ(record.field("f_string").value<String>(), fmt::format("{}_0", Fixture::str));
            EXPECT_EQ(record.field("f_nullable_date").value<Int32>(), Fixture::date);
        }
        else if (kv.first == "key_1")
        {
            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32 + 1);
            EXPECT_EQ(record.field("f_string").value<String>(), fmt::format("{}_1", Fixture::str));
            EXPECT_EQ(record.field("f_nullable_date").value<Int32>(), Fixture::date + 1);
        }
        else
            FAIL() << "Unexpected map key " << kv.first;
    }
}

TEST(AvroSchemaSerializer, MapOfUnionRecord)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "record_map",
      "type": {
        "type": "map",
        "values": ["null", {
          "name": "inner_record",
          "type": "record",
          "fields": [
            {"name": "f_int", "type": "int"},
            {"name": "f_string", "type": "string"},
            {"name": "f_nullable_date", "type": ["null", {"type": "int", "logicalType": "date"}]}
          ]
        }]
      }
    }
  ]
}
    )";

    Block header{
        {DataTypeFactory::instance().get("map(string, tuple(f_int int32, f_string string, f_nullable_date nullable(date)))"), "record_map"},
    };

    auto columns = header.mutateColumns();
    {
        ColumnMap & column_map = assert_cast<ColumnMap &>(*columns.at(0));
        ColumnArray & column_array = column_map.getNestedColumn();
        ColumnArray::Offsets & offsets = column_array.getOffsets();
        ColumnTuple & nested_columns = column_map.getNestedData();
        IColumn & keys_column = nested_columns.getColumn(0);
        IColumn & values_column = nested_columns.getColumn(1);

        for (size_t i = 0; i < 2; i++)
        {
            keys_column.insert(fmt::format("key_{}", i));
            values_column.insert(Tuple{Fixture::i32 + i, fmt::format("{}_{}", Fixture::str, i), Fixture::date + i});
        }
        offsets.push_back(offsets.back() + 2);
    }

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    auto record_map = root_record.field("record_map").value<avro::GenericMap>();
    const auto & kv_pairs = record_map.value();

    EXPECT_EQ(kv_pairs.size(), 2);

    for (const auto & kv : kv_pairs)
    {
        EXPECT_EQ(kv.second.type(), avro::AVRO_RECORD);
        const auto & record = kv.second.value<avro::GenericRecord>();
        if (kv.first == "key_0")
        {
            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32);
            EXPECT_EQ(record.field("f_string").value<String>(), fmt::format("{}_0", Fixture::str));
            EXPECT_EQ(record.field("f_nullable_date").value<Int32>(), Fixture::date);
        }
        else if (kv.first == "key_1")
        {
            EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32 + 1);
            EXPECT_EQ(record.field("f_string").value<String>(), fmt::format("{}_1", Fixture::str));
            EXPECT_EQ(record.field("f_nullable_date").value<Int32>(), Fixture::date + 1);
        }
        else
            FAIL() << "Unexpected map key " << kv.first;
    }
}

TEST(AvroSchemaSerializer, MapOfArrayOfUnionRecord)
{
    String schema = R"(
{
  "name": "Envelope",
  "type": "record",
  "fields": [
    {
      "name": "record_map",
      "type": {
        "type": "map",
        "values": {
          "type": "array",
          "items": ["null", {
            "name": "inner_record",
            "type": "record",
            "fields": [
              {"name": "f_int", "type": "int"},
              {"name": "f_string", "type": "string"},
              {"name": "f_nullable_date", "type": ["null", {"type": "int", "logicalType": "date"}]}
            ]
          }]
        }
      }
    }
  ]
}
    )";

    Block header{
        {DataTypeFactory::instance().get("map(string, array(tuple(f_int int32, f_string string, f_nullable_date nullable(date))))"), "record_map"},
    };

    auto columns = header.mutateColumns();
    {
        columns.at(0)->insert(Map{
            Tuple{"key_0", Array{
                Tuple{Fixture::i32, fmt::format("{}_0_0", Fixture::str), Fixture::date},
                Tuple{Fixture::i32 + 1, fmt::format("{}_0_1", Fixture::str), Fixture::date + 1},
            }},
            Tuple{"key_1", Array{
                Tuple{Fixture::i32 + 1, fmt::format("{}_1_0", Fixture::str), Fixture::date + 1},
                Tuple{Fixture::i32 + 2, fmt::format("{}_1_1", Fixture::str), Fixture::date + 2},
            }}
        });
    }

    auto root_record = getSerializationResult(schema, header, {std::move(columns), 1});
    auto record_map = root_record.field("record_map").value<avro::GenericMap>();
    const auto & kv_pairs = record_map.value();

    EXPECT_EQ(kv_pairs.size(), 2);

    for (const auto & kv : kv_pairs)
    {
        EXPECT_EQ(kv.second.type(), avro::AVRO_ARRAY);
        const auto & records = kv.second.value<avro::GenericArray>().value();
        if (kv.first == "key_0")
        {
            for (size_t i = 0; const auto & datum : records)
            {
                EXPECT_EQ(datum.type(), avro::AVRO_RECORD);
                const auto & record = datum.value<avro::GenericRecord>();
                EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32 + i);
                EXPECT_EQ(record.field("f_string").value<String>(), fmt::format("{}_0_{}", Fixture::str, i));
                EXPECT_EQ(record.field("f_nullable_date").value<Int32>(), Fixture::date + i);
                i++;
            }
        }
        else if (kv.first == "key_1")
        {
            for (size_t i = 0; const auto & datum : records)
            {
                EXPECT_EQ(datum.type(), avro::AVRO_RECORD);
                const auto & record = datum.value<avro::GenericRecord>();
                EXPECT_EQ(record.field("f_int").value<Int32>(), Fixture::i32 + 1 + i);
                EXPECT_EQ(record.field("f_string").value<String>(), fmt::format("{}_1_{}", Fixture::str, i));
                EXPECT_EQ(record.field("f_nullable_date").value<Int32>(), Fixture::date + 1 + i);
                i++;
            }
        }
        else
            FAIL() << "Unexpected map key " << kv.first;
    }
}
