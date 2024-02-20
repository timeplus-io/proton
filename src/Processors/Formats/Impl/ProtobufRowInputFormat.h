#pragma once

#include "config.h"

#if USE_PROTOBUF
#    include <Formats/FormatSchemaInfo.h>
#    include <Processors/Formats/IRowInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <Processors/Formats/ISchemaWriter.h>

namespace DB
{
class Block;
class FormatSchemaInfo;
class ProtobufReader;
class ProtobufSerializer;


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
        bool with_length_delimiter_);
    ~ProtobufRowInputFormat() override;

    String getName() const override { return "ProtobufRowInputFormat"; }

    /// proton: starts
    void setReadBuffer(ReadBuffer & buf) override;
    /// proton: ends

private:
    bool readRow(MutableColumns & columns, RowReadExtension &) override;
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
    FormatSchemaInfo schema_info;
};

/// proton: starts
/// Confluent framing + Protobuf binary datum encoding. Mainly used for Kafka.
/// Uses 3 caches:
/// 1. global: schema registry cache (base_url -> SchemaRegistry)
/// 2. SchemaRegistry: schema cache (schema_id -> schema)
/// 3. ProtobufConfluentRowInputFormat: deserializer cache (schema_id -> AvroDeserializer)
/// This is needed because KafkaStorage creates a new instance of InputFormat per a batch of messages
class ProtobufConfluentRowInputFormat final : public IRowInputFormat
{
public:
    ProtobufConfluentRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_);
    String getName() const override { return "ProtobufConfluentRowInputFormat"; }

    void setReadBuffer(ReadBuffer & buf) override;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & row_read_extension) override;

    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    using SchemaId = uint32_t;

    String registry_url;
    std::unique_ptr<ProtobufReader> reader;
    std::vector<size_t> missing_column_indices;
    std::unique_ptr<ProtobufSerializer> serializer;
};

class ProtobufSchemaWriter : public IExternalSchemaWriter
{
public:
    explicit ProtobufSchemaWriter(std::string_view schema_body_, const FormatSettings & settings_);

    SchemaValidationErrors validate() override;
    bool write(bool replace_if_exist) override;

private:
    FormatSchemaInfo schema_info;
};
/// proton: ends

}
#endif
