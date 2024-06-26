#include <Formats/FormatFactory.h>
#include <Formats/FormatSchemaFactory.h>
#include <Formats/FormatSchemaInfo.h>
#include <IO/ReadBufferFromString.h>
#include <Processors/Formats/ISchemaWriter.h>

#include <format>
#include <fstream>

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

}

void FormatSchemaFactory::registerSchema(const String & schema_name, const String & format, std::string_view schema_body, ExistsOP exists_op, ContextPtr & context)
{
    assert(!schema_name.empty());
    assert(!format.empty());
    assert(!schema_body.empty());

    checkSchemaType(format);

    auto format_settings = getFormatSettings(context);
    format_settings.schema.format_schema = schema_name;
    format_settings.schema.is_server = true;

    std::lock_guard lock(mutex);
    auto writer = FormatFactory::instance().getExternalSchemaWriter(format, context, std::move(format_settings));
    assert(writer); /* confirmed with checkSchemaType */

    try
    {
        writer->validate(schema_body);
    }
    catch (DB::Exception & e)
    {
        e.addMessage(std::format("{} schema {} was invalid", format, schema_name));
        e.rethrow();
    }

    auto result = writer->write(schema_body, exists_op == ExistsOP::Replace);
    if (!result && exists_op == ExistsOP::Throw)
        throw Exception(ErrorCodes::FORMAT_SCHEMA_ALREADY_EXISTS, "Format schema {} of type {} already exists", schema_name, format);
}

void FormatSchemaFactory::unregisterSchema(const String & schema_name, const String & format, bool throw_if_not_exists, ContextPtr & context)
{
    assert(!schema_name.empty());

    checkSchemaType(format);

    auto format_settings = getFormatSettings(context);
    FormatSchemaInfo fsinfo{schema_name, format, false, true, format_settings.schema.format_schema_path};

    std::lock_guard lock(mutex);
    const auto schema_path = findSchemaFile(schema_name, format, context);
    if (schema_path.empty())
    {
        if (throw_if_not_exists)
        {
            if (format.empty())
                throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA, "Format schema {} doesn't exists", schema_name);
            else
                throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA, "Format schema {} of type {} doesn't exists", schema_name, format);
        }

        return;
    }

    std::filesystem::remove(schema_path);

    String format_name = format;
    if (format_name.empty())
        format_name = FormatFactory::instance().getFormatFromSchemaFileName(schema_path);

    if (format_name.empty())
        return;
    auto writer = FormatFactory::instance().getExternalSchemaWriter(format_name, context, std::move(format_settings));
    assert(writer); /* confirmed with checkSchemaType */
    writer->onDeleted();
}

std::vector<FormatSchemaFactory::SchemaEntry> FormatSchemaFactory::getSchemasList(const String & format, const ContextPtr & context) const
{
    checkSchemaType(format);

    auto format_settings = getFormatSettings(context);
    std::filesystem::path schema_path{format_settings.schema.format_schema_path};

    std::lock_guard lock(mutex);
    std::vector<FormatSchemaFactory::SchemaEntry> ret;
    for (const auto & schema_entry : std::filesystem::directory_iterator{schema_path})
    {
        if (schema_entry.is_regular_file())
        {
            const auto & path = schema_entry.path();
            const auto & inferred_format = FormatFactory::instance().getFormatFromSchemaFileName(path);

            if (format.empty() || inferred_format == format)
                ret.emplace_back(basename(path), inferred_format);
        }
    }
    return ret;
}

FormatSchemaFactory::SchemaEntryWithBody FormatSchemaFactory::getSchema(const String & schema_name, const String & format, const ContextPtr & context) const
{
    assert(!schema_name.empty());

    checkSchemaType(format);

    std::lock_guard lock(mutex);
    const auto schema_path = findSchemaFile(schema_name, format, context);
    if (schema_path.empty())
    {
        if (format.empty())
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA, "Format schema {} doesn't exists", schema_name);
        else
            throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA, "Format schema {} of type {} doesn't exists", schema_name, format);
    }

    std::ifstream schema_file(schema_path);
    std::stringstream content;
    content << schema_file.rdbuf();

    return {{schema_name, FormatFactory::instance().getFormatFromSchemaFileName(schema_path)}, content.str()};
}

String FormatSchemaFactory::findSchemaFile(const String & schema_name, const String & format, const ContextPtr & context) const
{
    auto format_settings = getFormatSettings(context);
    FormatSchemaInfo fsinfo{schema_name, format, false, true, format_settings.schema.format_schema_path};

    String schema_path;

    std::lock_guard lock(mutex);
    if (format.empty())
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

void FormatSchemaFactory::checkSchemaType(const String & format) const
{
    if (format.empty())
        return;

    if (!FormatFactory::instance().checkIfFormatHasExternalSchemaWriter(format))
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_SCHEMA_TYPE, "Format schema type {} is not supported", format);
}

FormatSchemaFactory & FormatSchemaFactory::instance()
{
    static FormatSchemaFactory ret;
    return ret;
}
}
