#pragma once

#include <Formats/FormatSchemaInfo.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromFile.h>

namespace DB
{

class IExternalSchemaWriter
{
public:
    IExternalSchemaWriter(const String & format, std::string_view schema_body_, const FormatSettings & settings_)
        : schema_body(schema_body_)
        , settings(settings_)
        , schema_info(settings.schema.format_schema, format, false, settings.schema.is_server, settings.schema.format_schema_path)
        {}

    virtual ~IExternalSchemaWriter() = default;

    /// Validates the schema input. Should throw exceptions on validation failures.
    virtual void validate() = 0;

    /// Persistents the schema.
    /// The returned value indicates the result of replacement.
    /// If the schema does not exist, i.e. it's a new schema and no replacement happens, then it returns an empty result.
    /// Otherwise, it returns true if replacement happened, or returns false if replacement is not allowed.
    /// It throws exceptions if it fails to write.
    std::optional<bool> write(bool replace_if_exist)
    {
        auto already_exists = std::filesystem::exists(schema_info.absoluteSchemaPath());
        if (already_exists && !replace_if_exist)
            return false;

        WriteBufferFromFile write_buffer{schema_info.absoluteSchemaPath()};
        write_buffer.write(schema_body.data(), schema_body.size());
        return already_exists ? true : std::nullopt;
    }

protected:
    std::string_view schema_body;
    const FormatSettings & settings;
    FormatSchemaInfo schema_info;
};

}
