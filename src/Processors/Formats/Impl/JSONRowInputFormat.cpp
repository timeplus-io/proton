#include <Processors/Formats/Impl/JSONRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

JSONRowInputFormat::JSONRowInputFormat(ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_)
    : JSONEachRowRowInputFormat(in_, header_, params_, format_settings_, false), validate_types_from_metadata(format_settings_.json.validate_types_from_metadata)
{
}

void JSONRowInputFormat::readPrefix()
{
    skipBOMIfExists(*in);
    JSONUtils::skipObjectStart(*in);
    if (validate_types_from_metadata)
        JSONUtils::readMetadataAndValidateHeader(*in, getPort().getHeader(), format_settings.json);
    else
        JSONUtils::readMetadata(*in, format_settings.json);

    JSONUtils::skipComma(*in);
    if (!JSONUtils::skipUntilFieldInObject(*in, "data", format_settings.json))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected field \"data\" with table content");

    JSONUtils::skipArrayStart(*in);
    data_in_square_brackets = true;
}

void JSONRowInputFormat::readSuffix()
{
    JSONUtils::skipArrayEnd(*in);
    JSONUtils::skipTheRestOfObject(*in, format_settings.json); 
}

JSONRowSchemaReader::JSONRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_) : IIRowSchemaReader(in_, format_settings_)
{
}

NamesAndTypesList JSONRowSchemaReader::readSchema()
{
    skipBOMIfExists(in);
    JSONUtils::skipObjectStart(in);
    return JSONUtils::readMetadata(in, format_settings.json);
}

void registerInputFormatJSON(FormatFactory & factory)
{
    factory.registerInputFormat("JSON", [](
                     ReadBuffer & buf,
                     const Block & sample,
                     IRowInputFormat::Params params,
                     const FormatSettings & settings)
    {
        return std::make_shared<JSONRowInputFormat>(buf, sample, std::move(params), settings);
    });

    factory.markFormatSupportsSubsetOfColumns("JSON");
}

void registerJSONSchemaReader(FormatFactory & factory)
{
    auto register_schema_reader = [&](const String & format)
    {
        factory.registerSchemaReader(
            format, [](ReadBuffer & buf, const FormatSettings & format_settings) { return std::make_unique<JSONRowSchemaReader>(buf, format_settings); });
    };
    register_schema_reader("JSON");
    /// JSONCompact has the same suffix with metadata.
    register_schema_reader("JSONCompact");
}

}
