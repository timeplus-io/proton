#include "ProtobufRowInputFormat.h"

#if USE_PROTOBUF
#   include <Core/Block.h>
#   include <Formats/FormatFactory.h>
#   include <Formats/FormatSchemaInfo.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufSchemas.h>
#   include <Formats/ProtobufSerializer.h>
#   include <Interpreters/Context.h>
#   include <IO/WriteBufferFromFile.h>
#   include <base/range.h>


namespace DB
{

ProtobufRowInputFormat::ProtobufRowInputFormat(ReadBuffer & in_, const Block & header_, const Params & params_, const FormatSchemaInfo & schema_info_, bool with_length_delimiter_)
    : IRowInputFormat(header_, in_, params_, ProcessorID::ProtobufRowInputFormatID)
    , reader(std::make_unique<ProtobufReader>(in_))
    , serializer(ProtobufSerializer::create(
          header_.getNames(),
          header_.getDataTypes(),
          missing_column_indices,
          *ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info_),
          with_length_delimiter_,
         *reader))
{
}

ProtobufRowInputFormat::~ProtobufRowInputFormat() = default;

/// proton: starts
void ProtobufRowInputFormat::setReadBuffer(ReadBuffer & buf)
{
    IInputFormat::setReadBuffer(buf);
    reader->setReadBuffer(buf);
}
/// proton: ends

bool ProtobufRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & row_read_extension)
{
    if (reader->eof())
        return false;

    size_t row_num = columns.empty() ? 0 : columns[0]->size();
    if (!row_num)
        serializer->setColumns(columns.data(), columns.size());

    serializer->readRow(row_num);

    row_read_extension.read_columns.clear();
    row_read_extension.read_columns.resize(columns.size(), true);
    for (size_t column_idx : missing_column_indices)
        row_read_extension.read_columns[column_idx] = false;
    return true;
}

bool ProtobufRowInputFormat::allowSyncAfterError() const
{
    return true;
}

void ProtobufRowInputFormat::syncAfterError()
{
    reader->endMessage(true);
}

void registerInputFormatProtobuf(FormatFactory & factory)
{
    for (bool with_length_delimiter : {false, true})
    {
        factory.registerInputFormat(with_length_delimiter ? "Protobuf" : "ProtobufSingle", [with_length_delimiter](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<ProtobufRowInputFormat>(buf, sample, std::move(params),
                FormatSchemaInfo(settings, "Protobuf", true),
                with_length_delimiter);
        });
    }
}

ProtobufSchemaReader::ProtobufSchemaReader(const FormatSettings & format_settings)
    : schema_info(
          format_settings.schema.format_schema,
          "Protobuf",
          true,
          format_settings.schema.is_server,
          format_settings.schema.format_schema_path)
{
}

NamesAndTypesList ProtobufSchemaReader::readSchema()
{
    const auto * message_descriptor = ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info);
    return protobufSchemaToCHSchema(message_descriptor);
}

/// proton: starts
ProtobufSchemaWriter::ProtobufSchemaWriter(std::string_view schema_body_, const FormatSettings & settings_)
    : IExternalSchemaWriter(schema_body_, settings_)
    , schema_info(
          settings.schema.format_schema,
          "Protobuf",
          false,
          settings.schema.is_server,
          settings.schema.format_schema_path)
{
}

SchemaValidationErrors ProtobufSchemaWriter::validate()
{
    return ProtobufSchemas::instance().validateSchema(schema_body);
}

bool ProtobufSchemaWriter::write(bool replace_if_exist)
{
    if (std::filesystem::exists(schema_info.absoluteSchemaPath()))
        if (!replace_if_exist)
            return false;

    WriteBufferFromFile write_buffer {schema_info.absoluteSchemaPath()};
    write_buffer.write(schema_body.data(), schema_body.size());
    return true;
}
/// proton: ends

void registerProtobufSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("Protobuf", [](const FormatSettings & settings)
    {
        return std::make_shared<ProtobufSchemaReader>(settings);
    });
    factory.registerFileExtension("pb", "Protobuf");
    /// proton: starts
    factory.registerSchemaFileExtension("proto", "Protobuf");

    factory.registerExternalSchemaWriter("Protobuf", [](std::string_view schema_body, const FormatSettings & settings)
    {
        return std::make_shared<ProtobufSchemaWriter>(schema_body, settings);
    });
    /// proton: ends

    factory.registerExternalSchemaReader("ProtobufSingle", [](const FormatSettings & settings)
    {
        return std::make_shared<ProtobufSchemaReader>(settings);
    });

    for (const auto & name : {"Protobuf", "ProtobufSingle"})
        factory.registerAdditionalInfoForSchemaCacheGetter(
            name, [](const FormatSettings & settings) { return "Format schema: " + settings.schema.format_schema; });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProtobuf(FormatFactory &) {}

void registerProtobufSchemaReader(FormatFactory &) {}
}

#endif
