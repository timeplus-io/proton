#include "config.h"

#if USE_PROTOBUF
#    include <Common/Exception.h>
#    include <Common/LRUCache.h>
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
    /// proton: starts
    extern const int INVALID_DATA;
    /// proton: ends
}

/// proton: starts
namespace
{
class ErrorCollector final: public google::protobuf::io::ErrorCollector
{
private:
    SchemaValidationErrors errors;

public:
    void AddError(int line, google::protobuf::io::ColumnNumber column, const std::string & message) override
    {
        errors.emplace_back(line, column, message);
    }

    const SchemaValidationErrors & getErrors() const
    {
        return errors;
    }
};
}
/// proton: starts

ProtobufSchemas & ProtobufSchemas::instance()
{
    static ProtobufSchemas instance;
    return instance;
}

class ProtobufSchemas::ImporterWithSourceTree : public google::protobuf::compiler::MultiFileErrorCollector
{
public:
    explicit ImporterWithSourceTree(const String & schema_directory) : importer(&disk_source_tree, this)
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
        // If there are parsing errors AddError() throws an exception and in this case the following line
        // isn't executed.
        assert(file_descriptor);

        descriptor = file_descriptor->FindMessageTypeByName(message_name);
        if (!descriptor)
            throw Exception(
                "Not found a message named '" + message_name + "' in the schema file '" + schema_path + "'", ErrorCodes::BAD_ARGUMENTS);

        return descriptor;
    }

private:
    // Overrides google::protobuf::compiler::MultiFileErrorCollector:
    void AddError(const String & filename, int line, int column, const String & message) override
    {
        throw Exception(
            "Cannot parse '" + filename + "' file, found an error at line " + std::to_string(line) + ", column " + std::to_string(column)
                + ", " + message,
            ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA);
    }

    google::protobuf::compiler::DiskSourceTree disk_source_tree;
    google::protobuf::compiler::Importer importer;
};


ProtobufSchemas::ProtobufSchemas() = default;
ProtobufSchemas::~ProtobufSchemas() = default;

const google::protobuf::Descriptor * ProtobufSchemas::getMessageTypeForFormatSchema(const FormatSchemaInfo & info)
{
    std::lock_guard lock(mutex);
    auto it = importers.find(info.schemaDirectory());
    if (it == importers.end())
        it = importers.emplace(info.schemaDirectory(), std::make_unique<ImporterWithSourceTree>(info.schemaDirectory())).first;
    auto * importer = it->second.get();
    return importer->import(info.schemaPath(), info.messageName());
}

/// proton: starts
SchemaValidationErrors ProtobufSchemas::validateSchema(std::string_view schema)
{
    google::protobuf::io::ArrayInputStream input{schema.data(), static_cast<int>(schema.size())};
    ErrorCollector error_collector;
    google::protobuf::io::Tokenizer tokenizer(&input, &error_collector);
    google::protobuf::FileDescriptorProto descriptor;
    google::protobuf::compiler::Parser parser;

    parser.RecordErrorsTo(&error_collector);
    parser.Parse(&tokenizer, &descriptor);
    return error_collector.getErrors();
}
/// proton: ends

}

#endif
