#include <DataTypes/Serializations/SerializationUUID.h>
#include <Columns/ColumnsNumber.h>
#include <Formats/ProtobufReader.h>
#include <Formats/ProtobufWriter.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>

/// proton : starts
#include <IO/PrefixTreeEncode.h>
/// proton : ends

namespace DB
{

void SerializationUUID::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnUUID &>(column).getData()[row_num], ostr);
}

void SerializationUUID::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    UUID x;
    readText(x, istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "uuid");
}

bool SerializationUUID::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const
{
    UUID x;
    if (!tryReadText(x, istr) || (whole && !istr.eof()))
        return false;

    assert_cast<ColumnUUID &>(column).getData().push_back(x);
    return true;
}


void SerializationUUID::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('\'', ostr);
}

void SerializationUUID::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    assertChar('\'', istr);
    readText(x, istr);
    assertChar('\'', istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(x);    /// It's important to do this at the end - for exception safety.
}

bool SerializationUUID::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID uuid;
    String field;
    if (!checkChar('\'', istr) || !tryReadText(uuid, istr) || !checkChar('\'', istr))
        return false;

    assert_cast<ColumnUUID &>(column).getData().push_back(std::move(uuid));
    return true;
}

void SerializationUUID::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationUUID::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    assertChar('"', istr);
    readText(x, istr);
    assertChar('"', istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(x);
}

bool SerializationUUID::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    if (!checkChar('"', istr) || !tryReadText(x, istr) || !checkChar('"', istr))
        return false;
    assert_cast<ColumnUUID &>(column).getData().push_back(x);
    return true;
}

void SerializationUUID::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr, settings);
    writeChar('"', ostr);
}

void SerializationUUID::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID value;
    readCSV(value, istr);
    assert_cast<ColumnUUID &>(column).getData().push_back(value);
}

bool SerializationUUID::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID value;
    if (!tryReadCSV(value, istr))
        return false;
    assert_cast<ColumnUUID &>(column).getData().push_back(value);
    return true;
}

void SerializationUUID::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    UUID x = field.get<UUID>();
    writeBinary(x, ostr);
}

void SerializationUUID::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    readBinary(x, istr);
    field = NearestFieldType<UUID>(x);
}

void SerializationUUID::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeBinary(assert_cast<const ColumnVector<UUID> &>(column).getData()[row_num], ostr);
}

void SerializationUUID::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    UUID x;
    readBinary(x, istr);
    assert_cast<ColumnVector<UUID> &>(column).getData().push_back(x);
}

void SerializationUUID::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<UUID>::Container & x = typeid_cast<const ColumnVector<UUID> &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(UUID) * limit);
}

void SerializationUUID::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnVector<UUID>::Container & x = typeid_cast<ColumnVector<UUID> &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char*>(&x[initial_size]), sizeof(UUID) * limit);
    x.resize(initial_size + size / sizeof(UUID));
}

/// proton: starts
void SerializationUUID::deserializeBinaryBulkDiscard(ReadBuffer & istr, size_t limit) const
{
    istr.ignore(sizeof(UUID) * limit);
}

void SerializationUUID::serializeBinaryPrefixTree(const Field & field, String & encoded, const DB::FormatSettings & settings, bool ascending) const
{
    const UUID & uuid = field.get<const UUID &>();
    const auto & u128 = uuid.toUnderType();
    if (ascending)
    {
        PrefixTreeEncode::encodeVarUIntAscending(u128.items[0], encoded);
        PrefixTreeEncode::encodeVarUIntAscending(u128.items[1], encoded);
    }
    else
    {
        PrefixTreeEncode::encodeVarUIntDescending(u128.items[0], encoded);
        PrefixTreeEncode::encodeVarUIntDescending(u128.items[1], encoded);
    }
}

void SerializationUUID::serializeBinaryPrefixTree(
    const IColumn & column, size_t row_num, String & encoded, const FormatSettings & settings, bool ascending) const
{
    serializeBinaryPrefixTree(assert_cast<const ColumnUUID &>(column)[row_num], encoded, settings, ascending);
}

void SerializationUUID::deserializeBinaryPrefixTree(
    IColumn & column, std::string_view & data, const FormatSettings & settings, bool ascending) const
{
    assert_cast<ColumnVector<UUID> &>(column).getData().push_back(deserializeBinaryPrefixTree(data, settings, ascending));
}

void SerializationUUID::deserializeBinaryPrefixTreeDiscard(std::string_view & data, const FormatSettings & settings, bool ascending) const
{
    deserializeBinaryPrefixTree(data, settings, ascending);
}

UUID SerializationUUID::deserializeBinaryPrefixTree(
    std::string_view & data, const FormatSettings & /*settings*/, bool ascending) const
{
    UUID uuid;
    auto & u128 = uuid.toUnderType();

    if (ascending)
    {
        u128.items[0] = PrefixTreeEncode::decodeVarUIntAscending(data);
        u128.items[1] = PrefixTreeEncode::decodeVarUIntAscending(data);
    }
    else
    {
        u128.items[0] = PrefixTreeEncode::decodeVarUIntDescending(data);
        u128.items[1] = PrefixTreeEncode::decodeVarUIntDescending(data);
    }

    return uuid;
}
/// proton: ends
}
