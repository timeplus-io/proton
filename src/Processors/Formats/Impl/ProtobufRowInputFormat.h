#pragma once

#include "config.h"

#if USE_PROTOBUF
#   include <Processors/Formats/IRowInputFormat.h>
#   include <Processors/Formats/ISchemaReader.h>
#   include <Processors/Formats/ISchemaWriter.h>
#   include <Formats/FormatSchemaInfo.h>

/// proton: starts
#    include <Formats/KafkaSchemaRegistry.h>
/// proton: ends

namespace DB
{
class Block;
class ProtobufReader;
class ProtobufSerializer;
class ReadBuffer;

/** Stream designed to deserialize data from the google protobuf format.
  * One Protobuf message is parsed as one row of data.
  *
  * Input buffer may contain single protobuf message (use_length_delimiters_ = false),
  * or any number of messages (use_length_delimiters = true). In the second case
  * parser assumes messages are length-delimited according to documentation
  * https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/util/delimited_message_util.h
  * Parsing of the protobuf format requires the 'format_schema' setting to be set, e.g.
  * INSERT INTO table FORMAT Protobuf SETTINGS format_schema = 'schema:Message'
  * where schema is the name of "schema.proto" file specifying protobuf schema.
  */
class ProtobufRowInputFormat final : public IRowInputFormat
{
public:
    ProtobufRowInputFormat(
        ReadBuffer & in_,
        const Block & header_,
        const Params & params_,
        const FormatSchemaInfo & schema_info_,
        bool with_length_delimiter_,
        bool flatten_google_wrappers_);

    String getName() const override { return "ProtobufRowInputFormat"; }

    /// proton: starts
    void setReadBuffer(ReadBuffer & buf) override;
    /// proton: ends

private:
    bool readRow(MutableColumns & columns, RowReadExtension & row_read_extension) override;
    bool allowSyncAfterError() const override;
    void syncAfterError() override;

    std::unique_ptr<ProtobufReader> reader;
    std::vector<size_t> missing_column_indices;
    std::unique_ptr<ProtobufSerializer> serializer;
};

class ProtobufSchemaReader : public IExternalSchemaReader
{
public:
    explicit ProtobufSchemaReader(const FormatSettings & format_settings);

    NamesAndTypesList readSchema() override;

private:
    const FormatSchemaInfo schema_info;
    bool skip_unsupported_fields;
};

/// proton: starts
/// Confluent framing + Protobuf binary datum encoding. Mainly used for Kafka.
/// Uses 3 caches:
/// 1. global: schema registry cache (base_url + credentials -> SchemaRegistry)
/// 2. SchemaRegistry: schema cache (schema_id -> schema)
/// 3. ProtobufConfluentRowInputFormat: deserializer cache (schema_id -> AvroDeserializer)
/// This is needed because KafkaStorage creates a new instance of InputFormat per a batch of messages
class ProtobufConfluentRowInputFormat final : public IRowInputFormat
{
public:
    ProtobufConfluentRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);
    String getName() const override { return "ProtobufConfluentRowInputFormat"; }

    // void setReadBuffer(ReadBuffer & buf) override;

    class SchemaRegistryWithCache;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & row_read_extension) override;

    bool flatten_google_wrappers {false};
    std::shared_ptr<SchemaRegistryWithCache> registry;
    std::vector<size_t> missing_column_indices;
    std::unique_ptr<ProtobufSerializer> serializer;
};

class ProtobufSchemaWriter : public IExternalSchemaWriter
{
public:
    explicit ProtobufSchemaWriter(std::string_view schema_body_, const FormatSettings & settings_);

    void validate() override;
};
/// proton: ends

}
#endif
