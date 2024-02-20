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
class ProtobufSchemas::SchemaRegistry
{
public:
    explicit SchemaRegistry(const std::string & base_url_, size_t schema_cache_max_size = 1000)
        : base_url(base_url_), schema_cache(schema_cache_max_size)
    {
        if (base_url.empty())
            throw Exception("Empty Schema Registry URL", ErrorCodes::BAD_ARGUMENTS);
    }

    google::protobuf::FileDescriptorProto getSchema(uint32_t id)
    {
        auto [schema, loaded] = schema_cache.getOrSet(
            id,
            [this, id](){ return std::make_shared<google::protobuf::FileDescriptorProto>(fetchSchema(id)); }
        );
        return *schema;
    }

private:
    google::protobuf::FileDescriptorProto fetchSchema(uint32_t id)
    {
        auto schema = KafkaSchemaRegistry::instance().fetchSchema(base_url, id);
        google::protobuf::io::ArrayInputStream input{schema.data(), static_cast<int>(schema.size())};
        ErrorCollector error_collector;
        google::protobuf::io::Tokenizer tokenizer(&input, &error_collector);
        google::protobuf::FileDescriptorProto descriptor;
        google::protobuf::compiler::Parser parser;

        parser.RecordErrorsTo(&error_collector);
        parser.Parse(&tokenizer, &descriptor);
        if (!error_collector.getErrors().empty())
            throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Invalid protobuf schema FIXME");

        return descriptor;
    }

    Poco::URI base_url;
    LRUCache<uint32_t, google::protobuf::FileDescriptorProto> schema_cache;
};

using ConfluentSchemaRegistry = ProtobufSchemas::SchemaRegistry;
#define SCHEMA_REGISTRY_CACHE_MAX_SIZE 1000
/// Cache of Schema Registry URL -> SchemaRegistry
static LRUCache<std::string, ConfluentSchemaRegistry>  schema_registry_cache(SCHEMA_REGISTRY_CACHE_MAX_SIZE);

static std::shared_ptr<ConfluentSchemaRegistry> getConfluentSchemaRegistry(const String & base_url)
{
    auto [schema_registry, loaded] = schema_registry_cache.getOrSet(
        base_url,
        [base_url]()
        {
            return std::make_shared<ConfluentSchemaRegistry>(base_url);
        }
    );
    return schema_registry;
}
const google::protobuf::Descriptor * ProtobufSchemas::getMessageTypeForSchemaRegistry(const String & base_url, UInt32 schema_id)
{
    return getMessageTypeForSchemaRegistry(base_url, schema_id, {0});
}

const google::protobuf::Descriptor * ProtobufSchemas::getMessageTypeForSchemaRegistry(const String & base_url, UInt32 schema_id, const std::vector<Int64> & indexes)
{
    assert(!indexes.empty());

    auto registry = getConfluentSchemaRegistry(base_url);
    const auto *fd = registry_pool()->BuildFile(registry->getSchema(schema_id));

    const auto *descriptor = fd->message_type(indexes[0]);

    for (auto i : indexes)
    {
        if (i > static_cast<Int64>(descriptor->nested_type_count()))
            throw Exception(ErrorCodes::INVALID_DATA, "Invalid message index={} max_index={}", i, fd->message_type_count());
        descriptor = descriptor->nested_type(i);
    }
    return descriptor;
}

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
