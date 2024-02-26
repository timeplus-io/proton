#pragma once

#include "Formats/FormatSettings.h"
#include "IO/ReadBuffer.h"

namespace DB
{

class IExternalSchemaWriter
{
public:
    IExternalSchemaWriter(std::string_view schema_body_, const FormatSettings & settings_)
        : schema_body(schema_body_)
        , settings(settings_)
        {}

    virtual ~IExternalSchemaWriter() = default;

    /// Validates the schema input. Should throws exceptions on validation failures.
    virtual void validate() = 0;

    /// Persistents the schema.
    /// If the schema already exists, and replace_if_exist is false, it returns false.
    /// Otherwise it returns true. Throws exceptions if it fails to write.
    virtual bool write(bool replace_if_exist) = 0;

protected:
    std::string_view schema_body;
    const FormatSettings & settings;
};

}
