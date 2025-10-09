#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>

#include <gtest/gtest.h>
#include <Decoder.hh>
#include <Encoder.hh>

/// [avrogencpp BEGIN]
/// The following code was generated using avrogencpp (with some minor changes).
/// When the schemas in test cases are changed, it requires to use avrogencpp to generate the code again.
struct inner_record {
    int32_t f_int;
    std::string f_string;
    std::string f_uuid;
    inner_record() :
        f_int(0)
        { }
};

struct Envelope {
    std::vector<inner_record > record_array;
    Envelope() = default;
};

namespace avro {
template<> struct codec_traits<inner_record> {
    static void encode(Encoder& e, const inner_record& v) {
        avro::encode(e, v.f_int);
        avro::encode(e, v.f_string);
        avro::encode(e, v.f_uuid);
    }
    static void decode(Decoder& d, inner_record& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (size_t it : fo) {
                switch (it) {
                case 0:
                    avro::decode(d, v.f_int);
                    break;
                case 1:
                    avro::decode(d, v.f_string);
                    break;
                case 2:
                    avro::decode(d, v.f_uuid);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.f_int);
            avro::decode(d, v.f_string);
            avro::decode(d, v.f_uuid);
        }
    }
};

template<> struct codec_traits<Envelope> {
    static void encode(Encoder& e, const Envelope& v) {
        avro::encode(e, v.record_array);
    }
    static void decode(Decoder& d, Envelope& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (size_t it : fo) {
                switch (it) {
                case 0:
                    avro::decode(d, v.record_array);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.record_array);
        }
    }
};
/// [avrogencpp END]

}
namespace DB
{

namespace
{

namespace Fixture
{
const Int32 int_value = 123;
const String string_value = "hello world";
const String uuid_string = "789a352d-896a-4c02-a95b-d0c9da945f4f";
}

}

TEST(AvroDeserializer, ArrayOfRecord)
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

    auto valid_schema = avro::compileJsonSchemaFromString(schema);

    inner_record the_inner_record;
    the_inner_record.f_int = Fixture::int_value;
    the_inner_record.f_string = Fixture::string_value;
    the_inner_record.f_uuid = Fixture::uuid_string;

    Envelope envelope;
    envelope.record_array = {the_inner_record};

    auto out = avro::memoryOutputStream();
    auto encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, envelope);
    auto encoded_data = avro::snapshot(*out);

    {
        Block header{
            {DataTypeFactory::instance().get("array(int32)"), "record_array.f_int"},
        };
        AvroDeserializer deserializer{header, valid_schema, /*allow_missing_fields=*/ false, /*null_as_default_=*/ false, {}};

        auto columns = header.mutateColumns();
        RowReadExtension ext;

        auto in = avro::memoryInputStream(encoded_data->data(), encoded_data->size());
        auto decoder = avro::binaryDecoder();
        decoder->init(*in);
        deserializer.deserializeRow(columns, *decoder, ext);

        auto & column = *columns.front();
        const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
        const auto & nested_column = column_array.getData();
        EXPECT_EQ(nested_column.size(), 1);
        EXPECT_EQ(nested_column.getInt(0), Fixture::int_value);
    }

    {
        Block header{
            {DataTypeFactory::instance().get("array(string)"), "record_array.f_string"},
        };
        AvroDeserializer deserializer{header, valid_schema, /*allow_missing_fields=*/ false, /*null_as_default_=*/ false, {}};

        auto columns = header.mutateColumns();
        RowReadExtension ext;

        auto in = avro::memoryInputStream(encoded_data->data(), encoded_data->size());
        auto decoder = avro::binaryDecoder();
        decoder->init(*in);
        deserializer.deserializeRow(columns, *decoder, ext);

        auto & column = *columns.front();
        const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
        const auto & nested_column = column_array.getData();
        EXPECT_EQ(nested_column.size(), 1);
        EXPECT_EQ(nested_column.getDataAt(0).toString(), Fixture::string_value);
    }

    {
        Block header{
            {DataTypeFactory::instance().get("array(string)"), "record_array.f_uuid"},
        };
        AvroDeserializer deserializer{header, valid_schema, /*allow_missing_fields=*/ false, /*null_as_default_=*/ false, {}};

        auto columns = header.mutateColumns();
        RowReadExtension ext;

        auto in = avro::memoryInputStream(encoded_data->data(), encoded_data->size());
        auto decoder = avro::binaryDecoder();
        decoder->init(*in);
        deserializer.deserializeRow(columns, *decoder, ext);

        auto & column = *columns.front();
        const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
        const auto & nested_column = column_array.getData();
        EXPECT_EQ(nested_column.size(), 1);
        EXPECT_EQ(nested_column.getDataAt(0).toString(), Fixture::uuid_string);
    }
}

}
