#pragma once

#include <Formats/FormatSchemaInfo.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromFile.h>

#include <filesystem>

namespace DB
{

class IExternalSchemaWriter
{
public:
    IExternalSchemaWriter(const String & format, const FormatSettings & settings)
        : schema_info(settings.schema.format_schema, format, false, settings.schema.is_server, settings.schema.format_schema_path)
        {}

    virtual ~IExternalSchemaWriter() = default;

    /// Validates the schema input. Should throw exceptions on validation failures.
    virtual void validate(std::string_view schema_body) = 0;

    /// Persistents the schema.
    /// Returns false if file was not written, i.e. there was an existing schema file, and it's not replaced.
    /// It throws exceptions if it fails to write.
    bool write(std::string_view schema_body, bool replace_if_exist)
    {
        auto already_exists = std::filesystem::exists(schema_info.absoluteSchemaPath());
        if (already_exists && !replace_if_exist)
            return false;

        WriteBufferFromFile write_buffer{schema_info.absoluteSchemaPath()};
        write_buffer.write(schema_body.data(), schema_body.size());
        if (already_exists)
            onReplaced();

        return true;
    }

    /// A callback will be called when a schema gets deleted.
    virtual void onDeleted() {}

protected:
    /// A callback will be called when an existing schema gets replaced.
    virtual void onReplaced() {}

    FormatSchemaInfo schema_info;
};

}
