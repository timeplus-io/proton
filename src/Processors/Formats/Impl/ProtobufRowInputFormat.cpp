#include "ProtobufRowInputFormat.h"

#if USE_PROTOBUF
#    include <Core/Block.h>
#    include <Formats/FormatFactory.h>
#    include <Formats/FormatSchemaInfo.h>
#    include <Formats/ProtobufReader.h>
#    include <Formats/ProtobufSchemas.h>
#    include <Formats/ProtobufSerializer.h>
#    include <IO/WriteBufferFromFile.h>
#    include <Interpreters/Context.h>
#    include <base/range.h>

/// proton: starts
#    include <Common/LRUCache.h>
#    include <Formats/KafkaSchemaRegistry.h>
#    include <google/protobuf/compiler/parser.h>
#    include <google/protobuf/descriptor.pb.h>
#    include <google/protobuf/io/tokenizer.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_DATA;
}

ProtobufRowInputFormat::ProtobufRowInputFormat(
    ReadBuffer & in_, const Block & header_, const Params & params_, const FormatSchemaInfo & schema_info_, bool with_length_delimiter_)
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

void registerInputFormatProtobuf(FormatFactory & factory){
    /// proton: starts
    /// for (bool with_length_delimiter : {false, true})
    /// {
    ///     factory.registerInputFormat(
    ///         with_length_delimiter ? "Protobuf" : "ProtobufSingle",
    ///         [with_length_delimiter](
    ///             ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings) {
    ///             return std::make_shared<ProtobufRowInputFormat>(
    ///                 buf, sample, std::move(params), FormatSchemaInfo(settings, "Protobuf", true), with_length_delimiter);
    ///         });
    /// }

    factory.registerInputFormat(
        "ProtobufSingle",
        [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings) -> std::shared_ptr<IInputFormat>
        {
            if (settings.schema.kafka_schema_registry_url.empty())
                return std::make_shared<ProtobufRowInputFormat>(
                    buf, sample, std::move(params), FormatSchemaInfo(settings, "Protobuf", true), /*with_length_delimiter=*/false);

            return std::make_shared<ProtobufConfluentRowInputFormat>(
                    buf, sample, std::move(params), settings);
        });

    factory.registerInputFormat(
        "Protobuf",
        [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
        {
            return std::make_shared<ProtobufRowInputFormat>(
                buf, sample, std::move(params), FormatSchemaInfo(settings, "Protobuf", true), /*with_length_delimiter=*/true);
        });
    /// proton: ends

}

ProtobufSchemaReader::ProtobufSchemaReader(const FormatSettings & format_settings)
    : schema_info(
        format_settings.schema.format_schema, "Protobuf", true, format_settings.schema.is_server, format_settings.schema.format_schema_path)
{
}

NamesAndTypesList ProtobufSchemaReader::readSchema()
{
    const auto * message_descriptor = ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info);
    return protobufSchemaToCHSchema(message_descriptor);
}

/// proton: starts
namespace
{
using ConfluentSchemaRegistry = ProtobufConfluentRowInputFormat::SchemaRegistryWithCache;
#define SCHEMA_REGISTRY_CACHE_MAX_SIZE 1000
/// Cache of Schema Registry URL -> SchemaRegistry
LRUCache<std::string, ConfluentSchemaRegistry>  schema_registry_cache(SCHEMA_REGISTRY_CACHE_MAX_SIZE);

std::shared_ptr<ConfluentSchemaRegistry> getConfluentSchemaRegistry(const String & base_url, const String & credentials)
{
    auto [schema_registry, loaded] = schema_registry_cache.getOrSet(
        base_url + credentials,
        [base_url, credentials]()
        {
            return std::make_shared<ConfluentSchemaRegistry>(base_url, credentials);
        }
    );
    return schema_registry;
}
}

class ProtobufConfluentRowInputFormat::SchemaRegistryWithCache
{
public:
    SchemaRegistryWithCache(const String & base_url, const String & credentials)
    : registry(base_url, credentials)
    {
    }

    const google::protobuf::Descriptor * getMessageType(uint32_t schema_id, const std::vector<Int64> & indexes)
    {
        assert(!indexes.empty());

        const auto *descriptor = getSchema(schema_id)->message_type(indexes[0]);

        for (auto i : indexes)
        {
            if (i > static_cast<Int64>(descriptor->nested_type_count()))
                throw Exception(ErrorCodes::INVALID_DATA, "Invalid message index={} max_index={} descriptor={}", i, descriptor->nested_type_count(), descriptor->name());
            descriptor = descriptor->nested_type(i);
        }

        return descriptor;
    }

private:
    const google::protobuf::FileDescriptor * getSchema(uint32_t id)
    {
        const auto * ret = registry_pool()->FindFileByName("");
        if (ret)
            return ret;

        return fetchSchema(id);
    }

    const google::protobuf::FileDescriptor* fetchSchema(uint32_t id)
    {
        auto schema = registry.fetchSchema(id, "PROTOBUF");
        google::protobuf::io::ArrayInputStream input{schema.data(), static_cast<int>(schema.size())};
        google::protobuf::io::Tokenizer tokenizer(&input, /*FIXME*/nullptr);
        google::protobuf::FileDescriptorProto descriptor;
        google::protobuf::compiler::Parser parser;

        parser.RecordErrorsTo(/*FIXME*/nullptr);
        parser.Parse(&tokenizer, &descriptor);
        // if (!error_collector.getErrors().empty())
        //     throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Invalid protobuf schema FIXME");

        return registry_pool()->BuildFile(descriptor);
    }

    KafkaSchemaRegistry registry;
    google::protobuf::DescriptorPool registry_pool_;

    google::protobuf::DescriptorPool* registry_pool() { return &registry_pool_; }
};

ProtobufConfluentRowInputFormat::ProtobufConfluentRowInputFormat(
    ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings &  format_settings_)
    : IRowInputFormat(header_, in_, params_, ProcessorID::ProtobufRowInputFormatID)
    , registry(getConfluentSchemaRegistry(format_settings_.schema.kafka_schema_registry_url, format_settings_.schema.kafka_schema_registry_credentials))
    , reader(std::make_unique<ProtobufReader>(in_))
{
}

void ProtobufConfluentRowInputFormat::setReadBuffer(ReadBuffer & buf)
{
    IInputFormat::setReadBuffer(buf);
    reader->setReadBuffer(buf);
}

void ProtobufConfluentRowInputFormat::syncAfterError()
{
    reader->endMessage(true);
}

bool ProtobufConfluentRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & row_read_extension)
{
    if (reader->eof())
        return false;

    auto schema_id = KafkaSchemaRegistry::readSchemaId(*in);

    auto indexes_count = reader->readSInt();
    std::vector<Int64> indexes(1);
    if (indexes_count < 1)
        indexes.push_back(0);
    else
    {
        for (size_t i = 0; i < static_cast<size_t>(indexes_count); i++)
            indexes.push_back(reader->readSInt());
    }

    const auto & header = getPort().getHeader();

    serializer = ProtobufSerializer::create(
        header.getNames(),
        header.getDataTypes(),
        missing_column_indices,
        *registry->getMessageType(schema_id, indexes),
        /*with_length_delimiter=*/false,
        *reader);

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

ProtobufSchemaWriter::ProtobufSchemaWriter(std::string_view schema_body_, const FormatSettings & settings_)
    : IExternalSchemaWriter(schema_body_, settings_)
    , schema_info(settings.schema.format_schema, "Protobuf", false, settings.schema.is_server, settings.schema.format_schema_path)
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

    WriteBufferFromFile write_buffer{schema_info.absoluteSchemaPath()};
    write_buffer.write(schema_body.data(), schema_body.size());
    return true;
}
/// proton: ends

void registerProtobufSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader(
        "Protobuf", [](const FormatSettings & settings) { return std::make_shared<ProtobufSchemaReader>(settings); });
    factory.registerFileExtension("pb", "Protobuf");
    /// proton: starts
    factory.registerSchemaFileExtension("proto", "Protobuf");

    factory.registerExternalSchemaWriter("Protobuf", [](std::string_view schema_body, const FormatSettings & settings) {
        return std::make_shared<ProtobufSchemaWriter>(schema_body, settings);
    });
    /// proton: ends

    factory.registerExternalSchemaReader(
        "ProtobufSingle", [](const FormatSettings & settings) { return std::make_shared<ProtobufSchemaReader>(settings); });

    for (const auto & name : {"Protobuf", "ProtobufSingle"})
        factory.registerAdditionalInfoForSchemaCacheGetter(
            name, [](const FormatSettings & settings) { return "Format schema: " + settings.schema.format_schema; });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProtobuf(FormatFactory &)
{
}

void registerProtobufSchemaReader(FormatFactory &)
{
}
}

#endif
