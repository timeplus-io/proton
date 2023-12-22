#include <Formats/FormatFactory.h>
#include <Formats/FormatSchemaFactory.h>
#include <Formats/FormatSchemaInfo.h>
#include <google/protobuf/compiler/parser.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

namespace DB
{

namespace ErrorCodes
{
extern const int AMBIGUOUS_FORMAT_SCHEMA;
extern const int FORMAT_SCHEMA_ALREADY_EXISTS;
extern const int UNKNOWN_FORMAT_SCHEMA;
extern const int UNKNOWN_FORMAT_SCHEMA_TYPE;
extern const int INVALID_DATA;
}

namespace
{
String basename(const std::filesystem::path & path)
{
    return path.filename().replace_extension().string();
}

String getSchemaTypeFromFileExtension(String ext) noexcept
{
    /// when we support more schema format types, we probably should do it
    /// in a better way and consolidate it with FormatSchemaInfo.
    if (ext == ".proto")
        return "Protobuf";
    else
        return "";
}

class ErrorCollector final: public google::protobuf::io::ErrorCollector
{
private:
    std::vector<String> errors;

public:
    void AddError(int line, google::protobuf::io::ColumnNumber column, const std::string & message) override
    {
        errors.push_back(fmt::format("line: {}, columns: {}, error: {}", line, column, message));
    }

    String toString() const
    {
        return fmt::format("{}", fmt::join(errors, "\n"));
    }
};

void validateProtobufSchema(const std::string_view payload)
{
    google::protobuf::io::ArrayInputStream input{payload.data(), static_cast<int>(payload.size())};
    ErrorCollector error_collector;
    google::protobuf::io::Tokenizer tokenizer(&input, &error_collector);
    google::protobuf::FileDescriptorProto descriptor;
    google::protobuf::compiler::Parser parser;

    parser.RecordErrorsTo(&error_collector);
    if (!parser.Parse(&tokenizer, &descriptor))
        throw Exception(ErrorCodes::INVALID_DATA, "Invalid schema, errors: \n{}", error_collector.toString());
}
}

void FormatSchemaFactory::registerSchema(const String & schema_name, const String & schema_type, std::string_view schema_body, ExistsOP exists_op, const ContextPtr & context)
{
    assert(!schema_name.empty());
    assert(!schema_type.empty());
    assert(!schema_body.empty());

    checkSchemaType(schema_type);

    auto format_settings = getFormatSettings(context);
    FormatSchemaInfo fsinfo{schema_name, schema_type, false, true, format_settings.schema.format_schema_path};

    std::lock_guard lock(mutex);
    if (std::filesystem::exists(fsinfo.absoluteSchemaPath()))
    {
        if (exists_op == ExistsOP::Ignore)
            return;
        if (exists_op == ExistsOP::Throw)
            throw Exception(ErrorCodes::FORMAT_SCHEMA_ALREADY_EXISTS, "Format schema {} of type {} already exists", schema_name, schema_type);
    }

    if (schema_type == "Protobuf")
        validateProtobufSchema(schema_body);

    std::ofstream schema_file(fsinfo.absoluteSchemaPath(), std::ios::binary);
    schema_file << schema_body;
}

void FormatSchemaFactory::unregisterSchema(const String & schema_name, const String & schema_type, bool throw_if_not_exists, const ContextPtr & context)
{
    assert(!schema_name.empty());

    checkSchemaType(schema_type);

    auto format_settings = getFormatSettings(context);
    FormatSchemaInfo fsinfo{schema_name, schema_type, false, true, format_settings.schema.format_schema_path};

    std::lock_guard lock(mutex);
    const auto schema_path = findSchemaFile(schema_name, schema_type, context);
    if (schema_path.empty())
    {
        if (throw_if_not_exists)
        {
            if (schema_type.empty())
                throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA, "Format schema {} doesn't exists", schema_name);
            else
                throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA, "Format schema {} of type {} doesn't exists", schema_name, schema_type);
        }

        return;
    }

    std::filesystem::remove(schema_path);
}

std::vector<FormatSchemaFactory::SchemaEntry> FormatSchemaFactory::getSchemasList(const String & schema_type, const ContextPtr & context) const
{
    checkSchemaType(schema_type);

    auto format_settings = getFormatSettings(context);
    std::filesystem::path schema_path{format_settings.schema.format_schema_path};

    std::lock_guard lock(mutex);
    std::vector<FormatSchemaFactory::SchemaEntry> ret;
    for (const auto & schema_entry : std::filesystem::directory_iterator{schema_path})
    {
        if (schema_entry.is_regular_file())
        {
            const auto & path = schema_entry.path();
            const auto & ext = path.extension().string();
            const auto & inferred_schema_type = getSchemaTypeFromFileExtension(ext);

            if (schema_type.empty() || inferred_schema_type == schema_type)
                ret.emplace_back(basename(path), inferred_schema_type);
        }
    }
    return ret;
}

FormatSchemaFactory::SchemaEntryWithBody FormatSchemaFactory::getSchema(const String & schema_name, const String & schema_type, const ContextPtr & context) const
{
    assert(!schema_name.empty());

    checkSchemaType(schema_type);

    std::lock_guard lock(mutex);
    const auto schema_path = findSchemaFile(schema_name, schema_type, context);
    if (schema_path.empty())
    {
        if (schema_type.empty())
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA, "Format schema {} doesn't exists", schema_name);
        else
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA, "Format schema {} of type {} doesn't exists", schema_name, schema_type);
    }

    std::ifstream schema_file(schema_path);
    std::stringstream content;
    content << schema_file.rdbuf();

    return {{schema_name, getSchemaTypeFromFileExtension(std::filesystem::path(schema_path).extension())}, content.str()};
}

String FormatSchemaFactory::findSchemaFile(const String & schema_name, const String & schema_type, const ContextPtr & context) const
{
    auto format_settings = getFormatSettings(context);
    FormatSchemaInfo fsinfo{schema_name, schema_type, false, true, format_settings.schema.format_schema_path};

    String schema_path;

    std::lock_guard lock(mutex);
    if (schema_type.empty())
    {
        for (const auto & schema_entry : std::filesystem::directory_iterator{fsinfo.schemaDirectory()})
        {
            const auto & base_name = basename(schema_entry.path());
            if (schema_entry.is_regular_file() && base_name == schema_name)
            {
                if (schema_path.empty())
                    schema_path = schema_entry.path();
                else
                    throw Exception(ErrorCodes::AMBIGUOUS_FORMAT_SCHEMA, "Multiple format schemas with name `{}` exists, please specify TYPE", schema_name);
            }
        }
    }
    else if (std::filesystem::exists(fsinfo.absoluteSchemaPath()))
    {
        schema_path = fsinfo.absoluteSchemaPath();
    }

    return schema_path;
}

void FormatSchemaFactory::checkSchemaType(const String & schema_type) const
{
    if (schema_type.empty())
        return;

    if (schema_type != "Protobuf")
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA_TYPE, "Format schema type {} is not supported", schema_type);
}

FormatSchemaFactory & FormatSchemaFactory::instance()
{
    static FormatSchemaFactory ret;
    return ret;
}
}
