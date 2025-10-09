#include <Processors/Formats/Impl/OpenSearchRowOutputFormat.h>

#include <Core/UUID.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <IO/WriteBufferValidUTF8.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
}

OpenSearchRowOutputFormat::OpenSearchRowOutputFormat(
    WriteBuffer & out_, const Block & header_, const FormatSettings & settings_, ProcessorID pid_)
    : JSONEachRowRowOutputFormat(out_, header_, settings_, false, pid_)
{
    /// FormatFactory needs to create the format instance to call the getContentType function.
    /// In this case, it passes an empty buffer and empty block.
    if (header_.columns() == 0)
        return;

    if (!settings.open_search.index_column.empty())
    {
        auto pos = header_.tryGetPositionByName(settings.open_search.index_column);
        if (!pos)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Index column {} was not provided", settings.open_search.index_column);

        auto data_type = header_.getByPosition(*pos).type;
        if (!isStringOrFixedString(data_type))
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Index column should have a string type, but {} has type of {}",
                settings.open_search.index_column,
                data_type->getName());

        if (!settings.open_search.include_index_column_in_document)
            index_col_pos = pos;

        write_index = [i = *pos, this](const Columns & columns, size_t row_num) {
            const auto & col = columns.at(i);

            /// "index": "xxx",
            writeString(R"("_index":")", *ostr);
            writeString(col->getDataAt(row_num), *ostr);
            writeChar('"', *ostr);
            writeChar(',', *ostr);
        };
    }

    if (!settings.open_search.id_column.empty())
    {
        auto pos = header_.tryGetPositionByName(settings.open_search.id_column);
        if (!pos)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "ID column {} was not provided", settings.open_search.id_column);

        auto data_type = header_.getByPosition(*pos).type;
        if (!isStringOrFixedString(data_type))
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "ID column should have a string type, but {} has type of {}",
                settings.open_search.id_column,
                data_type->getName());

        if (!settings.open_search.include_id_column_in_document)
            id_col_pos = pos;

        write_id = [i = *pos, this](const Columns & columns, size_t row_num) {
            const auto & col = columns.at(i);

            writeString(R"("_id":")", *ostr);
            writeString(col->getDataAt(row_num), *ostr);
            writeChar('"', *ostr);
        };
    }
    else
    {
        write_id = [this](const Columns &, size_t) {
            writeString(R"("_id":")", *ostr);
            writeString("tp-", *ostr);
            writeString(toString(UUIDHelpers::generateV4()), *ostr);
            writeChar('"', *ostr);
        };
    }
}

void OpenSearchRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    writeString(R"({"create":{)", *ostr);
    if (write_index)
        write_index(columns, row_num);
    write_id(columns, row_num);
    writeChar('}', *ostr); /// closing "create"
    writeChar('}', *ostr); /// closing metadata
    writeChar('\n', *ostr);

    JSONEachRowRowOutputFormat::write(columns, row_num);
}

void OpenSearchRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    if ((index_col_pos && field_number == *index_col_pos) || (id_col_pos && field_number == *id_col_pos))
    {
        skip_field = true;
        ++field_number;
        return;
    }

    JSONEachRowRowOutputFormat::writeField(column, serialization, row_num);
}

void OpenSearchRowOutputFormat::writeFieldDelimiter()
{
    if (skip_field)
    {
        skip_field = false;
        return;
    }

    JSONEachRowRowOutputFormat::writeFieldDelimiter();
}

void registerOutputFormatOpenSearch(FormatFactory & factory)
{
    auto creator = [](WriteBuffer & buf, const Block & sample, const FormatSettings & _format_settings) {
        FormatSettings settings = _format_settings;
        settings.json.array_of_rows = false;
        settings.json.serialize_as_strings = false;
        return std::make_shared<OpenSearchRowOutputFormat>(buf, sample, settings);
    };

    for (const auto & name : Strings{"OpenSearch", "ElasticSearch"})
    {
        factory.registerOutputFormat(name, creator);
        factory.markOutputFormatSupportsParallelFormatting(name);
    }
}

}
