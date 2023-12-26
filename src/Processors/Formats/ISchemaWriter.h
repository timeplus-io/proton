#pragma once

#include "Formats/FormatSettings.h"
#include "IO/ReadBuffer.h"

namespace DB
{

struct SchemaValidationError final
{
    /// On which line the error happens. If line < 0, it indicates the error is not related to specific line,
    /// or the validator is unable to provide such information.
    int line;
    /// At which column the error happens. If col < 0, it indicates the error is not related to specific column,
    /// or the validator is unable to provide such information.
    int col;
    /// Error message
    String error;
};

using SchemaValidationErrors = std::vector<SchemaValidationError>;

class IExternalSchemaWriter
{
public:
    IExternalSchemaWriter(std::string_view schema_body_, const FormatSettings & settings_)
        : schema_body(schema_body_)
        , settings(settings_)
        {}

    virtual ~IExternalSchemaWriter() = default;

    /// Validates the schema input.
    virtual SchemaValidationErrors validate() = 0;

    /// Persistents the schema.
    /// If the schema already exists, and replace_if_exist is false, it returns false.
    /// Otherwise it returns true. Throws exceptions if it fails to write.
    virtual bool write(bool replace_if_exist) = 0;

protected:
    std::string_view schema_body;
    const FormatSettings & settings;
};

}
