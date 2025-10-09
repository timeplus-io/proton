#include <IO/ReadHelpers.h>
#include <Formats/JSONUtils.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferValidUTF8.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>

#include <base/find_symbols.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace JSONUtils
{
    template <const char opening_bracket, const char closing_bracket>
    static std::pair<bool, size_t>
    fileSegmentationEngineJSONEachRowImpl(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows)
    {
        skipWhitespaceIfAny(in);

        char * pos = in.position();
        size_t balance = 0;
        bool quotes = false;
        size_t number_of_rows = 0;

        while (loadAtPosition(in, memory, pos)
               && (balance || memory.size() + static_cast<size_t>(pos - in.position()) < min_chunk_size || number_of_rows < min_rows))
        {
            const auto current_object_size = memory.size() + static_cast<size_t>(pos - in.position());
            if (min_chunk_size != 0 && current_object_size > 10 * min_chunk_size)
                throw ParsingException(ErrorCodes::INCORRECT_DATA,
                    "Size of JSON object is extremely large. Expected not greater than {} bytes, but current is {} bytes per row. "
                    "Increase the value setting 'min_chunk_bytes_for_parallel_parsing' or check your data manually, "
                    "most likely JSON is malformed", min_chunk_size, current_object_size);

            if (quotes)
            {
                pos = find_first_symbols<'\\', '"'>(pos, in.buffer().end());

                if (pos > in.buffer().end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
                else if (pos == in.buffer().end())
                    continue;

                if (*pos == '\\')
                {
                    ++pos;
                    if (loadAtPosition(in, memory, pos))
                        ++pos;
                }
                else if (*pos == '"')
                {
                    ++pos;
                    quotes = false;
                }
            }
            else
            {
                pos = find_first_symbols<opening_bracket, closing_bracket, '\\', '"'>(pos, in.buffer().end());

                if (pos > in.buffer().end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
                else if (pos == in.buffer().end())
                    continue;

                else if (*pos == opening_bracket)
                {
                    ++balance;
                    ++pos;
                }
                else if (*pos == closing_bracket)
                {
                    --balance;
                    ++pos;
                }
                else if (*pos == '\\')
                {
                    ++pos;
                    if (loadAtPosition(in, memory, pos))
                        ++pos;
                }
                else if (*pos == '"')
                {
                    quotes = true;
                    ++pos;
                }

                if (balance == 0)
                    ++number_of_rows;
            }
        }

        saveUpToPosition(in, memory, pos);
        return {loadAtPosition(in, memory, pos), number_of_rows};
    }

    std::pair<bool, size_t> fileSegmentationEngineJSONEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size)
    {
        return fileSegmentationEngineJSONEachRowImpl<'{', '}'>(in, memory, min_chunk_size, 1);
    }

    std::pair<bool, size_t>
    fileSegmentationEngineJSONCompactEachRow(ReadBuffer & in, DB::Memory<> & memory, size_t min_chunk_size, size_t min_rows)
    {
        return fileSegmentationEngineJSONEachRowImpl<'[', ']'>(in, memory, min_chunk_size, min_rows);
    }

    NamesAndTypesList readRowAndGetNamesAndDataTypesForJSONEachRow(ReadBuffer & in, const FormatSettings & settings, JSONInferenceInfo * inference_info)
    {
        skipWhitespaceIfAny(in);
        assertChar('{', in);
        bool first = true;
        NamesAndTypesList names_and_types;
        String field;
        while (!in.eof() && *in.position() != '}')
        {
            if (!first)
                skipComma(in);
            else
                first = false;

            auto name = readFieldName(in, settings.json);
            auto type = tryInferDataTypeForSingleJSONField(in, settings, inference_info);
            names_and_types.emplace_back(name, type);
        }

        if (in.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while reading JSON object");

        assertChar('}', in);
        return names_and_types;
    }

    DataTypes readRowAndGetDataTypesForJSONCompactEachRow(ReadBuffer & in, const FormatSettings & settings, JSONInferenceInfo * inference_info)
    {
        skipWhitespaceIfAny(in);
        assertChar('[', in);
        bool first = true;
        DataTypes types;
        String field;
        while (!in.eof() && *in.position() != ']')
        {
            if (!first)
                skipComma(in);
            else
                first = false;
            auto type = tryInferDataTypeForSingleJSONField(in, settings, inference_info);
            types.push_back(std::move(type));
        }

        if (in.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while reading JSON array");

        assertChar(']', in);
        return types;
    }

    bool nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl(ReadBuffer & buf)
    {
        /// For JSONEachRow we can safely skip whitespace characters
        skipWhitespaceIfAny(buf);
        return buf.eof() || *buf.position() == '[';
    }

    bool readField(
        ReadBuffer & in,
        IColumn & column,
        const DataTypePtr & type,
        const SerializationPtr & serialization,
        const String & column_name,
        const FormatSettings & format_settings,
        bool yield_strings)
    {
        try
        {
            bool as_nullable = format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type);

            if (yield_strings)
            {
                String str;
                readJSONString(str, in, format_settings.json);

                ReadBufferFromString buf(str);

                if (as_nullable)
                    return SerializationNullable::deserializeNullAsDefaultOrNestedWholeText(column, buf, format_settings, serialization);

                serialization->deserializeWholeText(column, buf, format_settings);
                return true;
            }

            if (as_nullable)
                return SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(column, in, format_settings, serialization);

            serialization->deserializeTextJSON(column, in, format_settings);
            return true;
        }
        catch (Exception & e)
        {
            e.addMessage("(while reading the value of key " + column_name + ")");
            throw;
        }
    }

    void writeFieldDelimiter(WriteBuffer & out, size_t new_lines)
    {
        writeChar(',', out);
        writeChar('\n', new_lines, out);
    }

    void writeFieldCompactDelimiter(WriteBuffer & out) { writeCString(", ", out); }

    void writeTitle(const char * title, WriteBuffer & out, size_t indent, const char * after_delimiter)
    {
        writeChar('\t', indent, out);
        writeChar('"', out);
        writeCString(title, out);
        writeCString("\":", out);
        writeCString(after_delimiter, out);
    }

    void writeTitlePretty(const char * title, WriteBuffer & out, const FormatSettings & settings, size_t indent, const char * after_delimiter)
    {
        writeChar(settings.json.pretty_print_indent, indent * settings.json.pretty_print_indent_multiplier, out);
        writeChar('"', out);
        writeCString(title, out);
        writeCString("\":", out);
        writeCString(after_delimiter, out);
    }

    void writeObjectStart(WriteBuffer & out, size_t indent, const char * title)
    {
        if (title)
            writeTitle(title, out, indent, "\n");
        writeChar('\t', indent, out);
        writeCString("{\n", out);
    }

    void writeCompactObjectStart(WriteBuffer & out, size_t indent, const char * title)
    {
        if (title)
            writeTitle(title, out, indent, " ");
        writeCString("{", out);
    }

    void writeCompactObjectEnd(WriteBuffer & out)
    {
        writeChar('}', out);
    }

    void writeObjectEnd(WriteBuffer & out, size_t indent)
    {
        writeChar('\n', out);
        writeChar('\t', indent, out);
        writeChar('}', out);
    }

    void writeArrayStart(WriteBuffer & out, size_t indent, const char * title)
    {
        if (title)
            writeTitle(title, out, indent, "\n");
        writeChar('\t', indent, out);
        writeCString("[\n", out);
    }

    void writeCompactArrayStart(WriteBuffer & out, size_t indent, const char * title)
    {
        if (title)
            writeTitle(title, out, indent, " ");
        else
            writeChar('\t', indent, out);
        writeCString("[", out);
    }

    void writeArrayEnd(WriteBuffer & out, size_t indent)
    {
        writeChar('\n', out);
        writeChar('\t', indent, out);
        writeChar(']', out);
    }

    void writeCompactArrayEnd(WriteBuffer & out) { writeChar(']', out); }

    void writeFieldFromColumn(
        const IColumn & column,
        const ISerialization & serialization,
        size_t row_num,
        bool yield_strings,
        const FormatSettings & settings,
        WriteBuffer & out,
        const std::optional<String> & name,
        size_t indent,
        const char * title_after_delimiter,
        bool pretty_json)
    {
        if (name.has_value())
        {
            if (pretty_json)
            {
                writeTitlePretty(name->data(), out, settings, indent, title_after_delimiter);
            }
            else
            {
                writeTitle(name->data(), out, indent, title_after_delimiter);
            }
        }

        if (yield_strings)
        {
            WriteBufferFromOwnString buf;

            serialization.serializeText(column, row_num, buf, settings);
            writeJSONString(buf.str(), out, settings);
        }
        else
        {
            if (pretty_json)
            {
                serialization.serializeTextJSONPretty(column, row_num, out, settings, indent);
            }
            else
            {
                serialization.serializeTextJSON(column, row_num, out, settings);
            }
        }
    }

    void writeColumns(
        const Columns & columns,
        const Names & names,
        const Serializations & serializations,
        size_t row_num,
        bool yield_strings,
        const FormatSettings & settings,
        WriteBuffer & out,
        size_t indent)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (i != 0)
                writeFieldDelimiter(out);
            writeFieldFromColumn(*columns[i], *serializations[i], row_num, yield_strings, settings, out, names[i], indent);
        }
    }

    void writeCompactColumns(
        const Columns & columns,
        const Serializations & serializations,
        size_t row_num,
        bool yield_strings,
        const FormatSettings & settings,
        WriteBuffer & out)
    {
        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (i != 0)
                writeFieldCompactDelimiter(out);
            writeFieldFromColumn(*columns[i], *serializations[i], row_num, yield_strings, settings, out);
        }
    }

    void writeMetadata(const Names & names, const DataTypes & types, const FormatSettings & settings, WriteBuffer & out)
    {
        writeArrayStart(out, 1, "meta");

        for (size_t i = 0; i < names.size(); ++i)
        {
            writeObjectStart(out, 2);

            writeTitle("name", out, 3, " ");

            /// The field names are pre-escaped to be put into JSON string literal.
            writeChar('"', out);
            writeString(names[i], out);
            writeChar('"', out);

            writeFieldDelimiter(out);
            writeTitle("type", out, 3, " ");
            writeJSONString(types[i]->getName(), out, settings);
            writeObjectEnd(out, 2);

            if (i + 1 < names.size())
                writeFieldDelimiter(out);
        }

        writeArrayEnd(out, 1);
    }

    void writeAdditionalInfo(
        size_t rows,
        size_t rows_before_limit,
        bool applied_limit,
        const Stopwatch & watch,
        const Progress & progress,
        bool write_statistics,
        WriteBuffer & out)
    {
        writeFieldDelimiter(out, 2);
        writeTitle("rows", out, 1, " ");
        writeIntText(rows, out);

        if (applied_limit)
        {
            writeFieldDelimiter(out, 2);
            writeTitle("rows_before_limit_at_least", out, 1, " ");
            writeIntText(rows_before_limit, out);
        }

        if (write_statistics)
        {
            writeFieldDelimiter(out, 2);
            writeObjectStart(out, 1, "statistics");

            writeTitle("elapsed", out, 2, " ");
            writeText(watch.elapsedSeconds(), out);
            writeFieldDelimiter(out);

            writeTitle("rows_read", out, 2, " ");
            writeText(progress.read_rows.load(), out);
            writeFieldDelimiter(out);

            writeTitle("bytes_read", out, 2, " ");
            writeText(progress.read_bytes.load(), out);

            writeObjectEnd(out, 1);
        }
    }

    void writeException(const String & exception_message, WriteBuffer & out, const FormatSettings & settings, size_t indent)
    {
        writeTitle("exception", out, indent, " ");
        writeJSONString(exception_message, out, settings);
    }

    Strings makeNamesValidJSONStrings(const Strings & names, const FormatSettings & settings, bool validate_utf8)
    {
        Strings result;
        result.reserve(names.size());
        for (const auto & name : names)
        {
            WriteBufferFromOwnString buf;
            if (validate_utf8)
            {
                WriteBufferValidUTF8 validating_buf(buf);
                writeJSONString(name, validating_buf, settings);
            }
            else
                writeJSONString(name, buf, settings);

            result.push_back(buf.str().substr(1, buf.str().size() - 2));
        }
        return result;
    }

    void skipColon(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar(':', in);
        skipWhitespaceIfAny(in);
    }

    String readFieldName(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        skipWhitespaceIfAny(in);
        String field;
        readJSONString(field, in, settings);
        skipColon(in);
        return field;
    }

    String readStringField(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        skipWhitespaceIfAny(in);
        String value;
        readJSONString(value, in, settings);
        skipWhitespaceIfAny(in);
        return value;
    }

    void skipArrayStart(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar('[', in);
        skipWhitespaceIfAny(in);
    }

    bool checkAndSkipArrayStart(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        if (!checkChar('[', in))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    void skipArrayEnd(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar(']', in);
        skipWhitespaceIfAny(in);
    }

    bool checkAndSkipArrayEnd(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        if (!checkChar(']', in))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    void skipObjectStart(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar('{', in);
        skipWhitespaceIfAny(in);
    }

    void skipObjectEnd(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar('}', in);
        skipWhitespaceIfAny(in);
    }

    bool checkAndSkipObjectEnd(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        if (!checkChar('}', in))
            return false;
        skipWhitespaceIfAny(in);
        return true;
    }

    void skipComma(ReadBuffer & in)
    {
        skipWhitespaceIfAny(in);
        assertChar(',', in);
        skipWhitespaceIfAny(in);
    }

    std::pair<String, String> readStringFieldNameAndValue(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        auto field_name = readFieldName(in, settings);
        auto field_value = readStringField(in, settings);
        return {field_name, field_value};
    }

    NameAndTypePair readObjectWithNameAndType(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        skipObjectStart(in);
        auto [first_field_name, first_field_value] = readStringFieldNameAndValue(in, settings);
        skipComma(in);
        auto [second_field_name, second_field_value] = readStringFieldNameAndValue(in, settings);

        NameAndTypePair name_and_type;
        if (first_field_name == "name" && second_field_name == "type")
            name_and_type = {first_field_value, DataTypeFactory::instance().get(second_field_value)};
        else if (second_field_name == "name" && first_field_name == "type")
            name_and_type = {second_field_value, DataTypeFactory::instance().get(first_field_value)};
        else
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                R"(Expected two fields "name" and "type" with column name and type, found fields "{}" and "{}")",
                first_field_name,
                second_field_name);
        skipObjectEnd(in);
        return name_and_type;
    }

    NamesAndTypesList readMetadata(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        auto field_name = readFieldName(in, settings);
        if (field_name != "meta")
            throw Exception(ErrorCodes::INCORRECT_DATA, "Expected field \"meta\" with columns names and types, found field {}", field_name);
        skipArrayStart(in);
        NamesAndTypesList names_and_types;
        bool first = true;
        while (!checkAndSkipArrayEnd(in))
        {
            if (!first)
                skipComma(in);
            else
                first = false;

            names_and_types.push_back(readObjectWithNameAndType(in, settings));
        }
        return names_and_types;
    }

    NamesAndTypesList readMetadataAndValidateHeader(ReadBuffer & in, const Block & header, const FormatSettings::JSON & settings)
    {
        auto names_and_types = JSONUtils::readMetadata(in, settings);
        for (const auto & [name, type] : names_and_types)
        {
            auto header_type = header.getByName(name).type;
            if (header.has(name) && !type->equals(*header_type))
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "Type {} of column '{}' from metadata is not the same as type in header {}", type->getName(), name, header_type->getName());
        }
        return names_and_types;
    }

    bool skipUntilFieldInObject(ReadBuffer & in, const String & desired_field_name, const FormatSettings::JSON & settings)
    {
        while (!checkAndSkipObjectEnd(in))
        {
            auto field_name = JSONUtils::readFieldName(in, settings);
            if (field_name == desired_field_name)
                return true;
        }

        return false;
    }

    void skipTheRestOfObject(ReadBuffer & in, const FormatSettings::JSON & settings)
    {
        while (!checkAndSkipObjectEnd(in))
        {
            skipComma(in);
            auto name = readFieldName(in, settings);
            skipWhitespaceIfAny(in);
            skipJSONField(in, name, settings);
        }
    }

}

}
