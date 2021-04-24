#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONEachRowUtils.h>
#include <Processors/Formats/Impl/RawStoreInputFormat.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Query.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int UNRECOGNIZED_ARGUMENTS;
}

namespace
{
    enum
    {
        UNKNOWN_FIELD = size_t(-1),
        NESTED_FIELD = size_t(-2)
    };

}


RawStoreInputFormat::RawStoreInputFormat(
    ReadBuffer & in_, const Block & header_, Params params_, const FormatSettings & format_settings_, bool yield_strings_)
    : IRowInputFormat(header_, in_, std::move(params_))
    , format_settings(format_settings_)
    , name_map(header_.columns())
    , yield_strings(yield_strings_)
{
    size_t num_columns = getPort().getHeader().columns();
    for (size_t i = 0; i < num_columns; ++i)
    {
        const String & column_name = columnName(i);
        name_map[column_name] = i;
    }

    const auto & time_extraction_type = format_settings_.rawstore.rawstore_time_extraction_type;
    const auto & time_extraction_rule = format_settings_.rawstore.rawstore_time_extraction_rule;

    if (!time_extraction_type.empty() && time_extraction_type != "json_path" && time_extraction_type != "regex")
        throw Exception(
            "time_extraction_type should be either 'json' or 'regex' " + time_extraction_type + " is not supported.",
            ErrorCodes::UNRECOGNIZED_ARGUMENTS);

    if (time_extraction_type == "regex")
    {
        time_extraction_regex = std::make_unique<re2::RE2>(time_extraction_rule);

        if (!time_extraction_regex->ok())
        {
            throw Exception(
                "Cannot compile re2: " + time_extraction_rule + " for http handling rule, error: " + time_extraction_regex->error()
                    + ". Look at https://github.com/google/re2/wiki/Syntax for reference.",
                ErrorCodes::CANNOT_COMPILE_REGEXP);
        }

        const std::map<std::string, int> & grp_to_idx = time_extraction_regex->NamedCapturingGroups();

        auto it = grp_to_idx.find("_time");
        if (it != grp_to_idx.end())
            time_group_idx = it->second;
        else
            throw Exception(
                "No '_time' group defined in 'time_extraction_rule': " + time_extraction_rule, ErrorCodes::UNRECOGNIZED_ARGUMENTS);
    }
    else if (time_extraction_type == "json_path" && time_extraction_rule.empty())
        throw Exception("'time_extraction_rule' is empty", ErrorCodes::UNRECOGNIZED_ARGUMENTS);

    prev_positions.resize(num_columns);
    raw_col_idx = columnIndex("_raw", 0);
    time_col_idx = columnIndex("_time", 0);
    if (raw_col_idx == UNKNOWN_FIELD || time_col_idx == UNKNOWN_FIELD)
        throw Exception("It is suitable for RawStoreFormat, either '_raw' or '_time' is missing", ErrorCodes::UNRECOGNIZED_ARGUMENTS);
}

inline const String & RawStoreInputFormat::columnName(size_t i) const
{
    return getPort().getHeader().getByPosition(i).name;
}

inline size_t RawStoreInputFormat::columnIndex(const StringRef & name, size_t key_index)
{
    /// Optimization by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.

    if (prev_positions.size() > key_index && prev_positions[key_index] && name == prev_positions[key_index]->getKey())
    {
        return prev_positions[key_index]->getMapped();
    }
    else
    {
        auto * it = name_map.find(name);

        if (it)
        {
            if (key_index < prev_positions.size())
                prev_positions[key_index] = it;

            return it->getMapped();
        }
        else
            return UNKNOWN_FIELD;
    }
}

/** Read the field name and convert it to column name
  *  (taking into account the current nested name prefix)
  * Resulting StringRef is valid only before next read from buf.
  */
StringRef RawStoreInputFormat::readColumnName(ReadBuffer & buf)
{
    if (!buf.eof() && buf.position() + 1 < buf.buffer().end())
    {
        char * next_pos = find_first_symbols<'\\', '"'>(buf.position() + 1, buf.buffer().end());

        if (next_pos != buf.buffer().end() && *next_pos != '\\')
        {
            /// The most likely option is that there is no escape sequence in the key name, and the entire name is placed in the buffer.
            assertChar('"', buf);
            StringRef res(buf.position(), next_pos - buf.position());
            buf.position() = next_pos + 1;
            return res;
        }
    }

    return current_column_name;
}


static inline void skipColonDelimeter(ReadBuffer & istr)
{
    skipWhitespaceIfAny(istr);
    assertChar(':', istr);
    skipWhitespaceIfAny(istr);
}

void RawStoreInputFormat::skipUnknownField(const StringRef & name_ref)
{
    if (!format_settings.skip_unknown_fields)
        throw Exception("Unknown field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

    skipJSONField(in, name_ref);
}

void RawStoreInputFormat::readField(size_t index, MutableColumns & columns)
{
    if (seen_columns[index])
        throw Exception("Duplicate field found while parsing JSONEachRow format: " + columnName(index), ErrorCodes::INCORRECT_DATA);

    try
    {
        seen_columns[index] = read_columns[index] = true;
        const auto & type = getPort().getHeader().getByPosition(index).type;
        const auto & serialization = serializations[index];

        if (yield_strings)
        {
            String str;
            readJSONString(str, in);

            ReadBufferFromString buf(str);

            if (format_settings.null_as_default && !type->isNullable())
                read_columns[index] = SerializationNullable::deserializeWholeTextImpl(*columns[index], buf, format_settings, serialization);
            else
                serialization->deserializeWholeText(*columns[index], buf, format_settings);
        }
        else
        {
            if (format_settings.null_as_default && !type->isNullable())
                read_columns[index] = SerializationNullable::deserializeTextJSONImpl(*columns[index], in, format_settings, serialization);
            else
                serialization->deserializeTextJSON(*columns[index], in, format_settings);
        }
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading the value of key " + columnName(index) + ")");
        throw;
    }
}

inline bool RawStoreInputFormat::advanceToNextKey(size_t key_index)
{
    skipWhitespaceIfAny(in);

    if (in.eof())
        throw ParsingException("Unexpected end of stream while parsing JSONEachRow format", ErrorCodes::CANNOT_READ_ALL_DATA);
    else if (*in.position() == '}')
    {
        ++in.position();
        return false;
    }

    if (key_index > 0)
    {
        assertChar(',', in);
        skipWhitespaceIfAny(in);
    }
    return true;
}

void RawStoreInputFormat::readJSONObject(MutableColumns & columns)
{
    assertChar('{', in);

    for (size_t key_index = 0; advanceToNextKey(key_index); ++key_index)
    {
        StringRef name_ref = readColumnName(in);
        const size_t column_index = columnIndex(name_ref, key_index);

        if (unlikely(ssize_t(column_index) < 0))
        {
            /// name_ref may point directly to the input buffer
            /// and input buffer may be filled with new data on next read
            /// If we want to use name_ref after another reads from buffer, we must copy it to temporary string.

            current_column_name.assign(name_ref.data, name_ref.size);
            name_ref = StringRef(current_column_name);

            skipColonDelimeter(in);

            if (column_index == UNKNOWN_FIELD)
                skipUnknownField(name_ref);
            else
                throw Exception("Logical error: illegal value of column_index", ErrorCodes::LOGICAL_ERROR);
        }
        else
        {
            skipColonDelimeter(in);
            readField(column_index, columns);
        }
    }

    /// Read _time from _raw column
    if (seen_columns[time_col_idx])
        return;

    if (!seen_columns[raw_col_idx] || !read_columns[raw_col_idx])
    {
        throw Exception("Logical error: no _raw column in the input data", ErrorCodes::LOGICAL_ERROR);
    }

    if (format_settings.rawstore.rawstore_time_extraction_type == "json_path")
        extractTimeFromRawByJSON(*columns[time_col_idx], *columns[raw_col_idx]);
    else if (format_settings.rawstore.rawstore_time_extraction_type == "regex" && time_extraction_regex != nullptr)
        extractTimeFromRawByRegex(*columns[time_col_idx], *columns[raw_col_idx]);
}

void RawStoreInputFormat::extractTimeFromRawByJSON(IColumn & time_col, IColumn & raw_col)
{
    Poco::JSON::Parser parser;
    Poco::DynamicAny result;

    StringRef raw;
    if (raw_col.getDataType() == TypeIndex::String)
        raw = raw_col.getDataAt(raw_col.size() - 1);
    else
        throw Exception("_raw column is not String type", ErrorCodes::LOGICAL_ERROR);

    /// To avoid extra memory copy,
    struct Membuf : std::streambuf
    {
        Membuf(char * begin, char * end) { this->setg(begin, begin, end); }
    };
    Membuf sbuf = Membuf(const_cast<char *>(raw.data), const_cast<char *>(raw.data + raw.size));
    std::istream in(&sbuf);

    try
    {
        auto var = parser.parse(in);
        result = var.extract<Poco::JSON::Object::Ptr>();
    }
    catch (Poco::JSON::JSONException & exception)
    {
        std::cout << exception.message() << std::endl;
        throw Exception("parse _raw field as JSON failed, inner exception is: " + exception.message(), ErrorCodes::INCORRECT_DATA);
    }

    Poco::JSON::Query query(result);

    std::string time = query.findValue(format_settings.rawstore.rawstore_time_extraction_rule.c_str(), "");
    if (time.empty())
        throw Exception(
            "extract _time from _raw failed with rule: " + format_settings.rawstore.rawstore_time_extraction_rule,
            ErrorCodes::INCORRECT_DATA);

    /// Wrap the time with \" to allow deserializeTextJSON to get the correct value
    String s;
    s.append("\"");
    s.append(time);
    s.append("\"");

    ReadBufferFromString buf(s);

    serializations[time_col_idx]->deserializeTextJSON(time_col, buf, format_settings);
    seen_columns[time_col_idx] = read_columns[time_col_idx] = true;
}

void RawStoreInputFormat::extractTimeFromRawByRegex(IColumn & time_col, IColumn & raw_col)
{
    StringRef raw;
    if (raw_col.getDataType() == TypeIndex::String)
        raw = raw_col.getDataAt(raw_col.size() - 1);
    else
        throw Exception("_raw column is not String type", ErrorCodes::INCORRECT_DATA);

    re2::StringPiece in{raw.data, raw.size};
    int num_captures = time_extraction_regex->NumberOfCapturingGroups() + 1;
    re2::StringPiece matches[num_captures];

    if (!time_extraction_regex->Match(in, 0, in.size(), re2::RE2::Anchor::UNANCHORED, matches, num_captures))
        throw Exception(
            "Cannot extract _time from " + in.as_string()
                + " time_extraction_rule: " + format_settings.rawstore.rawstore_time_extraction_rule,
            ErrorCodes::INCORRECT_DATA);

    String s;
    s.append("\"");
    s.append(matches[time_group_idx].data(), matches[time_group_idx].size());
    s.append("\"");
    ReadBufferFromString buf(s);

    serializations[time_col_idx]->deserializeTextJSON(time_col, buf, format_settings);
    seen_columns[time_col_idx] = read_columns[time_col_idx] = true;
}

bool RawStoreInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (!allow_new_rows)
        return false;
    skipWhitespaceIfAny(in);

    /// We consume , or \n before scanning a new row, instead scanning to next row at the end.
    /// The reason is that if we want an exact number of rows read with LIMIT x
    /// from a streaming table engine with text data format, like File or Kafka
    /// then seeking to next ;, or \n would trigger reading of an extra row at the end.

    /// Semicolon is added for convenience as it could be used at end of INSERT query.
    bool is_first_row = getCurrentUnitNumber() == 0 && getTotalRows() == 1;
    if (!in.eof())
    {
        /// There may be optional ',' (but not before the first row)
        if (!is_first_row && *in.position() == ',')
            ++in.position();
        else if (!data_in_square_brackets && *in.position() == ';')
        {
            /// ';' means the end of query (but it cannot be before ']')
            return allow_new_rows = false;
        }
        else if (data_in_square_brackets && *in.position() == ']')
        {
            /// ']' means the end of query
            return allow_new_rows = false;
        }
    }

    skipWhitespaceIfAny(in);
    if (in.eof())
        return false;

    size_t num_columns = columns.size();

    read_columns.assign(num_columns, false);
    seen_columns.assign(num_columns, false);

    readJSONObject(columns);

    const auto & header = getPort().getHeader();
    /// Fill non-visited columns with the default values.
    for (size_t i = 0; i < num_columns; ++i)
        if (!seen_columns[i])
            header.getByPosition(i).type->insertDefaultInto(*columns[i]);

    /// Return info about defaults set
    ext.read_columns = read_columns;
    return true;
}


void RawStoreInputFormat::syncAfterError()
{
    skipToUnescapedNextLineOrEOF(in);
}

void RawStoreInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    read_columns.clear();
    seen_columns.clear();
    prev_positions.clear();
}

void RawStoreInputFormat::readPrefix()
{
    /// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
    skipBOMIfExists(in);

    skipWhitespaceIfAny(in);
    if (!in.eof() && *in.position() == '[')
    {
        ++in.position();
        data_in_square_brackets = true;
    }
}

void RawStoreInputFormat::readSuffix()
{
    skipWhitespaceIfAny(in);
    if (data_in_square_brackets)
    {
        assertChar(']', in);
        skipWhitespaceIfAny(in);
    }
    if (!in.eof() && *in.position() == ';')
    {
        ++in.position();
        skipWhitespaceIfAny(in);
    }
    //    assertEOF(in);
}


void registerInputFormatProcessorRawStoreEachRow(FormatFactory & factory)
{
    factory.registerInputFormatProcessor(
        "RawStoreEachRow",
        [](ReadBuffer & buf,
           const Block & sample,
           IRowInputFormat::Params params,
           const FormatSettings & settings) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
            return std::make_shared<RawStoreInputFormat>(buf, sample, std::move(params), settings, false);
        });
}

void registerFileSegmentationEngineRawStoreEachRow(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("RawStoreEachRow", &fileSegmentationEngineJSONEachRowImpl);
}

}
