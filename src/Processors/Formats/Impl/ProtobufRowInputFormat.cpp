#include "ProtobufRowInputFormat.h"

#if USE_PROTOBUF
#   include <Core/Block.h>
#   include <Formats/FormatFactory.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufSchemas.h>
#   include <Formats/ProtobufSerializer.h>
#   include <IO/WriteBufferFromFile.h>

/// proton: starts
#   include <Common/LRUCache.h>
#   include <Formats/KafkaSchemaRegistry.h>
#   include <IO/VarInt.h>
#   include <span>

#   include <google/protobuf/compiler/parser.h>
#   include <google/protobuf/descriptor.pb.h>
#   include <google/protobuf/io/tokenizer.h>
/// proton: ends

namespace DB
{

/// proton: starts
namespace ErrorCodes
{
extern const int INVALID_DATA;
extern const int INVALID_SETTING_VALUE;
}
/// proton: ends

ProtobufRowInputFormat::ProtobufRowInputFormat(ReadBuffer & in_, const Block & header_, const Params & params_,
    const FormatSchemaInfo & schema_info_, bool with_length_delimiter_, bool flatten_google_wrappers_)
    : IRowInputFormat(header_, in_, params_, ProcessorID::ProtobufRowInputFormatID)
    , reader(std::make_unique<ProtobufReader>(in_))
    , serializer(ProtobufSerializer::create(
          header_.getNames(),
          header_.getDataTypes(),
          missing_column_indices,
          *ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info_, ProtobufSchemas::WithEnvelope::No),
          with_length_delimiter_,
          /* with_envelope = */ false,
          flatten_google_wrappers_,
         *reader))
{
}

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
        [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
            -> std::shared_ptr<IInputFormat>
        {
            if (settings.kafka_schema_registry.url.empty())
                return std::make_shared<ProtobufRowInputFormat>(buf, sample, std::move(params),
                    FormatSchemaInfo(settings, "Protobuf", true),
                    /*with_length_delimiter=*/false,
                    settings.protobuf.input_flatten_google_wrappers);

            if (!settings.schema.format_schema.empty())
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE, "kafka_schema_registry_url and format_schema cannot be used at the same time");

            return std::make_shared<ProtobufConfluentRowInputFormat>(buf, sample, std::move(params), settings);
        });

    factory.registerInputFormat(
        "Protobuf", [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
        {
            return std::make_shared<ProtobufRowInputFormat>(buf, sample, std::move(params),
                FormatSchemaInfo(settings, "Protobuf", true),
                /*with_length_delimiter=*/true,
                settings.protobuf.input_flatten_google_wrappers);
        });
    /// proton: ends
}

ProtobufSchemaReader::ProtobufSchemaReader(const FormatSettings & format_settings)
    : schema_info(
          format_settings.schema.format_schema,
          "Protobuf",
          true,
          format_settings.schema.is_server, format_settings.schema.format_schema_path)
    , skip_unsupported_fields(format_settings.protobuf.skip_fields_with_unsupported_types_in_schema_inference)
{
}

NamesAndTypesList ProtobufSchemaReader::readSchema()
{
    const auto * message_descriptor = ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info, ProtobufSchemas::WithEnvelope::No);
    return protobufSchemaToCHSchema(message_descriptor, skip_unsupported_fields);
}

/// proton: starts
namespace
{
using ConfluentSchemaRegistry = ProtobufConfluentRowInputFormat::SchemaRegistryWithCache;

auto & schemaRegistryCache()
{
    static LRUCache<KafkaSchemaRegistry::CacheKey, ConfluentSchemaRegistry, KafkaSchemaRegistry::CacheHasher> schema_registry_cache(/*max_size_=*/1000);
    return schema_registry_cache;
}

std::shared_ptr<ConfluentSchemaRegistry> getConfluentSchemaRegistry(const FormatSettings & format_settings)
{
    KafkaSchemaRegistry::CacheKey key {
          format_settings.kafka_schema_registry.url,
          format_settings.kafka_schema_registry.credentials,
          format_settings.kafka_schema_registry.private_key_file,
          format_settings.kafka_schema_registry.certificate_file,
          format_settings.kafka_schema_registry.ca_location,
          format_settings.kafka_schema_registry.skip_cert_check};

    auto [schema_registry, loaded] = schemaRegistryCache().getOrSet(
        key,
        [&key]() { return std::make_shared<ConfluentSchemaRegistry>(key.base_url, key.credentials, key.private_key_file, key.certificate_file, key.ca_location, key.skip_cert_check); });
    return schema_registry;
}
}

class ProtobufConfluentRowInputFormat::SchemaRegistryWithCache : public google::protobuf::io::ErrorCollector
{
public:
    SchemaRegistryWithCache(
        const String & base_url,
        const String & credentials,
        const String & private_key_file,
        const String & certificate_file,
        const String & ca_location,
        bool skip_cert_check)
        : registry(base_url, credentials, private_key_file, certificate_file, ca_location, skip_cert_check) { }

    /// Overrides google::protobuf::io::ErrorCollector.
    void AddError(int line, google::protobuf::io::ColumnNumber column, const std::string & message) override
    {
        throw Exception(ErrorCodes::INVALID_DATA, "Failed to parse schema, line={}, column={}, message={}", line, column, message);
    }

    const google::protobuf::Descriptor * getMessageType(uint32_t schema_id, const std::vector<Int64> & indexes)
    {
        assert(!indexes.empty());

        const auto * fd = getSchema(schema_id);

        /// Check https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
        /// for how to get the message with the indexes.
        auto mt_count = fd->message_type_count();
        if (mt_count < indexes[0] + 1)
            throw Exception(ErrorCodes::INVALID_DATA, "Invalid message index={} max_index={}", indexes[0], mt_count);

        const auto * descriptor = fd->message_type(indexes[0]);

        for (auto i : std::span(indexes.begin() + 1, indexes.end()))
        {
            if (i > descriptor->nested_type_count())
                throw Exception(
                    ErrorCodes::INVALID_DATA,
                    "Invalid message index={} max_index={} descriptor={}",
                    i,
                    descriptor->nested_type_count(),
                    descriptor->name());
            descriptor = descriptor->nested_type(i);
        }

        return descriptor;
    }

private:
    const google::protobuf::FileDescriptor * getSchema(uint32_t id)
    {
        const auto * loaded_descriptor = descriptor_pool.FindFileByName(std::to_string(id));
        if (loaded_descriptor)
            return loaded_descriptor;

        return fetchSchema(id);
    }

    const google::protobuf::FileDescriptor * fetchSchema(uint32_t id)
    {
        std::lock_guard lock(mutex);
        /// Just in case we got beaten
        const auto * loaded_descriptor = descriptor_pool.FindFileByName(std::to_string(id));
        if (loaded_descriptor)
            return loaded_descriptor;

        auto schema = registry.fetchSchema(id);
        google::protobuf::io::ArrayInputStream input{schema.data(), static_cast<int>(schema.size())};
        google::protobuf::io::Tokenizer tokenizer(&input, this);
        google::protobuf::FileDescriptorProto file_descriptor;
        file_descriptor.set_name(std::to_string(id));
        google::protobuf::compiler::Parser parser;
        parser.RecordErrorsTo(this);
        parser.Parse(&tokenizer, &file_descriptor);

        auto const * descriptor = descriptor_pool.BuildFile(file_descriptor);
        if (descriptor && descriptor->message_type_count() > 0)
            return descriptor;

        throw Exception(ErrorCodes::INVALID_DATA, "No message type in schema");
    }

    std::mutex mutex;
    KafkaSchemaRegistry registry;
    google::protobuf::DescriptorPool descriptor_pool;
};

ProtobufConfluentRowInputFormat::ProtobufConfluentRowInputFormat(
    ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_, ProcessorID::ProtobufRowInputFormatID)
    , flatten_google_wrappers(format_settings_.protobuf.input_flatten_google_wrappers)
    , registry(getConfluentSchemaRegistry(format_settings_))
{
}

bool ProtobufConfluentRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & row_read_extension)
{
    if (in->eof())
        return false;

    auto schema_id = KafkaSchemaRegistry::readSchemaId(*in);


    Int64 indexes_count = 0;
    readVarInt(indexes_count, *in);

    std::vector<Int64> indexes;
    indexes.reserve(indexes_count < 1 ? 1 : indexes_count);

    if (indexes_count < 1)
    {
        indexes.push_back(0);
    }
    else
    {
        for (Int64 i = 0; i < indexes_count; i++)
        {
            Int64 ind = 0;
            readVarInt(ind, *in);
            indexes.push_back(ind);
        }
    }

    const auto & header = getPort().getHeader();

    ProtobufReader reader{*in};
    serializer = ProtobufSerializer::create(
        header.getNames(),
        header.getDataTypes(),
        missing_column_indices,
        *registry->getMessageType(schema_id, indexes),
        /*with_length_delimiter=*/false,
        /* with_envelope = */ false,
        flatten_google_wrappers,
        reader);

    size_t row_num = columns.empty() ? 0 : columns[0]->size();
    if (!row_num)
        serializer->setColumns(columns.data(), columns.size());

    serializer->readRow(row_num);

    assert(row_read_extension.read_columns.empty());
    if (!missing_column_indices.empty())
    {
        row_read_extension.read_columns.resize(columns.size(), true);
        for (size_t column_idx : missing_column_indices)
            row_read_extension.read_columns[column_idx] = false;
    }
    return true;
}

ProtobufSchemaWriter::ProtobufSchemaWriter(const FormatSettings & settings_)
    : IExternalSchemaWriter(settings_)
{
}

String ProtobufSchemaWriter::getFormatName() const
{
    return "Protobuf";
}

void ProtobufSchemaWriter::validate(std::string_view schema_body)
{
    ProtobufSchemas::instance().validateSchema(schema_body);
}

void ProtobufSchemaWriter::onReplaced()
{
    ProtobufSchemas::instance().clear();
}

void ProtobufSchemaWriter::onDeleted()
{
    ProtobufSchemas::instance().clear();
}
/// proton: ends

void registerProtobufSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader(
        "Protobuf", [](const FormatSettings & settings) { return std::make_shared<ProtobufSchemaReader>(settings); });
    factory.registerFileExtension("pb", "Protobuf");
    /// proton: starts
    factory.registerSchemaFileExtension("proto", "Protobuf");

    factory.registerExternalSchemaWriter("Protobuf", [](const FormatSettings & settings) {
        return std::make_shared<ProtobufSchemaWriter>(settings);
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
void registerInputFormatProtobuf(FormatFactory &) {}
void registerProtobufSchemaReader(FormatFactory &) {}
}

#endif
