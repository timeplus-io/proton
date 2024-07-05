#include "config.h"

#if USE_PROTOBUF
#    include <Common/Exception.h>
#    include <Formats/FormatSchemaInfo.h>
#    include <Formats/KafkaSchemaRegistry.h>
#    include <Formats/ProtobufSchemas.h>
#    include <Processors/Formats/ISchemaWriter.h>
#    include <google/protobuf/compiler/importer.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}

ProtobufSchemas & ProtobufSchemas::instance()
{
    static ProtobufSchemas instance;
    return instance;
}

void ProtobufSchemas::clear()
{
    std::lock_guard lock(mutex);
    importers.clear();
}

class ProtobufSchemas::ImporterWithSourceTree : public google::protobuf::compiler::MultiFileErrorCollector
{
public:
    explicit ImporterWithSourceTree(const String & schema_directory, WithEnvelope with_envelope_)
        : importer(&disk_source_tree, this)
        , with_envelope(with_envelope_)
    {
        disk_source_tree.MapPath("", schema_directory);
    }

    ~ImporterWithSourceTree() override = default;

    const google::protobuf::Descriptor * import(const String & schema_path, const String & message_name)
    {
        // Search the message type among already imported ones.
        const auto * descriptor = importer.pool()->FindMessageTypeByName(message_name);
        if (descriptor)
            return descriptor;

        const auto * file_descriptor = importer.Import(schema_path);
        if (error)
        {
            auto info = error.value();
            error.reset();
            throw Exception(
                ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA,
                "Cannot parse '{}' file, found an error at line {}, column {}, {}",
                info.filename,
                info.line,
                info.column,
                info.message);
        }

        assert(file_descriptor);

        if (with_envelope == WithEnvelope::No)
        {
            const auto * message_descriptor = file_descriptor->FindMessageTypeByName(message_name);
            if (!message_descriptor)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Could not find a message named '{}' in the schema file '{}'",
                    message_name, schema_path);

            return message_descriptor;
        }
        else
        {
            const auto * envelope_descriptor = file_descriptor->FindMessageTypeByName("Envelope");
            if (!envelope_descriptor)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Could not find a message named 'Envelope' in the schema file '{}'",
                    schema_path);

            const auto * message_descriptor = envelope_descriptor->FindNestedTypeByName(message_name); // silly protobuf API disallows a restricting the field type to messages
            if (!message_descriptor)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Could not find a message named '{}' in the schema file '{}'",
                    message_name, schema_path);

            return message_descriptor;
        }
    }

private:
    // Overrides google::protobuf::compiler::MultiFileErrorCollector:
    void AddError(const String & filename, int line, int column, const String & message) override
    {
        /// Protobuf library code is not exception safe, we should
        /// remember the error and throw it later from our side.
        error = ErrorInfo{filename, line, column, message};
    }

    google::protobuf::compiler::DiskSourceTree disk_source_tree;
    google::protobuf::compiler::Importer importer;
    const WithEnvelope with_envelope;

    struct ErrorInfo
    {
        String filename;
        int line;
        int column;
        String message;
    };

    std::optional<ErrorInfo> error;
};


const google::protobuf::Descriptor * ProtobufSchemas::getMessageTypeForFormatSchema(const FormatSchemaInfo & info, WithEnvelope with_envelope)
{
    std::lock_guard lock(mutex);
    auto it = importers.find(info.schemaDirectory());
    if (it == importers.end())
        it = importers.emplace(info.schemaDirectory(), std::make_unique<ImporterWithSourceTree>(info.schemaDirectory(), with_envelope)).first;
    auto * importer = it->second.get();
    return importer->import(info.schemaPath(), info.messageName());
}

/// proton: starts
namespace
{

class ErrorCollectorImpl : public google::protobuf::io::ErrorCollector
{
public:
    struct ErrorInfo
    {
        int line;
        int column;
        String message;
    };

    ErrorCollectorImpl() = default;

    void AddError(int line, google::protobuf::io::ColumnNumber column, const std::string & message) override
    {
        /// Protobuf library code is not exception safe, we should
        /// remember the error and throw it later from our side.
        if (!error_) /// Only remember the first error.
            error_ = {line, column, message};
    }

    std::optional<ErrorInfo> error()
    {
        return error_;
    }

private:

    std::optional<ErrorInfo> error_;
};

}

void ProtobufSchemas::validateSchema(std::string_view schema)
{
    ErrorCollectorImpl error_collector{};

    google::protobuf::io::ArrayInputStream input{schema.data(), static_cast<int>(schema.size())};
    google::protobuf::io::Tokenizer tokenizer(&input, &error_collector);
    google::protobuf::FileDescriptorProto descriptor;
    google::protobuf::compiler::Parser parser;

    parser.RecordErrorsTo(&error_collector);

    parser.Parse(&tokenizer, &descriptor);

    if (auto error = error_collector.error(); error)
        throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA,
            "Cannot parse schema, found an error at line {}, column {}, error: {}", error->line, error->column, error->message);
}
/// proton: ends

}

#endif
