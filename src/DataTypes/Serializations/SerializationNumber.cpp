#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

/// proton : starts
#include <IO/PrefixTreeEncode.h>
/// proton : ends

namespace DB
{

template <typename T>
void SerializationNumber<T>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeText(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], ostr);
}

template <typename T>
void SerializationNumber<T>::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    T x;

    if constexpr (is_integer<T> && is_arithmetic_v<T>)
        readIntTextUnsafe(x, istr);
    /// proton: starts
    else if constexpr (std::is_floating_point_v<T>)
    {
        if (settings.precise_float_parsing)
            readFloatTextPrecise(x, istr);
        else
            readFloatTextFast(x, istr);
    }
    /// proton: ends
    else
        readText(x, istr);

    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "number");
}

template <typename T>
bool SerializationNumber<T>::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    T x;

    bool success;
    if constexpr (is_integer<T> && is_arithmetic_v<T>)
        success = tryReadText(x, istr);
    /// proton: starts
    else if constexpr (std::is_floating_point_v<T>)
    {
        if (settings.precise_float_parsing)
            success = tryReadFloatTextPrecise(x, istr);
        else
            success = tryReadFloatTextFast(x, istr);
    }
    /// proton: ends
    else
        success = tryReadText(x, istr);

    if (!success || (whole && !istr.eof()))
        return false;

    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
    return true;
}

template <typename T>
void SerializationNumber<T>::serializeTextJSON(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto x = assert_cast<const ColumnVector<T> &>(column).getData()[row_num];
    writeJSONNumber(x, ostr, settings);
}

template <typename T, typename ReturnType>
ReturnType deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    bool has_quote = false;
    if (!istr.eof() && *istr.position() == '"') /// We understand the number both in quotes and without.
    {
        has_quote = true;
        ++istr.position();
    }

    T x;

    /// null
    if (!has_quote && !istr.eof() && *istr.position() == 'n')
    {
        ++istr.position();
        if constexpr (throw_exception)
            assertString("ull", istr);
        else if (!checkString("ull", istr))
            return ReturnType(false);

        x = NaNOrZero<T>();
    }
    else
    {
        static constexpr bool is_uint8 = std::is_same_v<T, UInt8>;
        static constexpr bool is_int8 = std::is_same_v<T, Int8>;

        if (settings.json.read_bools_as_numbers || is_uint8 || is_int8)
        {
            // extra conditions to parse true/false strings into 1/0
            if (istr.eof())
            {
                if constexpr (throw_exception)
                    throwReadAfterEOF();
                else
                    return false;
            }

            if (*istr.position() == 't' || *istr.position() == 'f')
            {
                bool tmp = false;
                if constexpr (throw_exception)
                    readBoolTextWord(tmp, istr);
                else if (!readBoolTextWord<bool>(tmp, istr))
                    return ReturnType(false);

                x = tmp;
            }
            else
            {
                if constexpr (throw_exception)
                    readText(x, istr);
                else if (!tryReadText(x, istr))
                    return ReturnType(false);
            }
        }
        else
        {
            if constexpr (throw_exception)
                readText(x, istr);
            else if (!tryReadText(x, istr))
                return ReturnType(false);
        }

        if (has_quote)
        {
            if constexpr (throw_exception)
                assertChar('"', istr);
            else if (!checkChar('"', istr))
                return ReturnType(false);
        }
    }

    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
    return ReturnType(true);
}

template <typename T>
void SerializationNumber<T>::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextJSONImpl<T, void>(column, istr, settings);
}

template <typename T>
bool SerializationNumber<T>::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return deserializeTextJSONImpl<T, bool>(column, istr, settings);
}

template <typename T>
void SerializationNumber<T>::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    FieldType x;
    readCSV(x, istr);
    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

template <typename T>
bool SerializationNumber<T>::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & /*settings*/) const
{
    FieldType x;
    if (!tryReadCSV(x, istr))
        return false;
    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
    return true;
}

template <typename T>
void SerializationNumber<T>::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const
{
    /// ColumnVector<T>::ValueType is a narrower type. For example, UInt8, when the Field type is UInt64
    typename ColumnVector<T>::ValueType x = static_cast<typename ColumnVector<T>::ValueType>(field.get<FieldType>());
    writeBinary(x, ostr);
}

template <typename T>
void SerializationNumber<T>::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const
{
    typename ColumnVector<T>::ValueType x;
    readBinary(x, istr);
    field = NearestFieldType<FieldType>(x);
}

template <typename T>
void SerializationNumber<T>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const
{
    writeBinary(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], ostr);
}

template <typename T>
void SerializationNumber<T>::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const
{
    typename ColumnVector<T>::ValueType x;
    readBinary(x, istr);
    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

template <typename T>
void SerializationNumber<T>::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const typename ColumnVector<T>::Container & x = typeid_cast<const ColumnVector<T> &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    if (limit)
        ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(typename ColumnVector<T>::ValueType) * limit);
}

template <typename T>
void SerializationNumber<T>::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
{
    typename ColumnVector<T>::Container & x = typeid_cast<ColumnVector<T> &>(column).getData();
    size_t initial_size = x.size();
    x.resize(initial_size + limit);
    size_t size = istr.readBig(reinterpret_cast<char *>(&x[initial_size]), sizeof(typename ColumnVector<T>::ValueType) * limit);
    x.resize(initial_size + size / sizeof(typename ColumnVector<T>::ValueType));
}

/// proton: starts
template <typename T>
void SerializationNumber<T>::deserializeBinaryBulkDiscard(ReadBuffer & istr, size_t limit) const
{
    istr.ignore(sizeof(typename ColumnVector<T>::ValueType) * limit);
}

template <typename T>
void SerializationNumber<T>::serializeBinaryPrefixTree(
    const Field & field, String & encoded, const FormatSettings & /*settings*/, bool ascending) const
{
    using VT = typename ColumnVector<T>::ValueType;

    if constexpr (sizeof(VT) > 8)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prefix tree encoding for big number is not supported");

    auto x = static_cast<typename ColumnVector<T>::ValueType>(field.get<FieldType>());

    if constexpr (std::is_same_v<VT, Float64>)
    {
        if (ascending)
            PrefixTreeEncode::encodeDoubleAscending(x, encoded);
        else
            PrefixTreeEncode::encodeDoubleDescending(x, encoded);
    }
    else if constexpr (std::is_same_v<VT, Float32>)
    {
        if (ascending)
            PrefixTreeEncode::encodeFloatAscending(x, encoded);
        else
            PrefixTreeEncode::encodeFloatDescending(x, encoded);
    }
    else if constexpr (std::is_signed_v<VT>)
    {
        if (ascending)
            PrefixTreeEncode::encodeVarIntAscending(x, encoded);
        else
            PrefixTreeEncode::encodeVarIntDescending(x, encoded);
    }
    else
    {
        if (ascending)
            PrefixTreeEncode::encodeVarUIntAscending(x, encoded);
        else
            PrefixTreeEncode::encodeVarUIntDescending(x, encoded);
    }
}

template <typename T>
void SerializationNumber<T>::serializeBinaryPrefixTree(
    const IColumn & column, size_t row_num, String & encoded, const FormatSettings & /*settings*/, bool ascending) const
{
    using VT = typename ColumnVector<T>::ValueType;

    if constexpr (sizeof(VT) > 8)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prefix tree encoding for big number is not supported");

    if constexpr (std::is_same_v<VT, Float64>)
    {
        if (ascending)
            PrefixTreeEncode::encodeDoubleAscending(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], encoded);
        else
            PrefixTreeEncode::encodeDoubleDescending(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], encoded);
    }
    else if constexpr (std::is_same_v<VT, Float32>)
    {
        if (ascending)
            PrefixTreeEncode::encodeFloatAscending(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], encoded);
        else
            PrefixTreeEncode::encodeFloatDescending(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], encoded);
    }
    else if constexpr (std::is_signed_v<VT>)
    {
        if (ascending)
            PrefixTreeEncode::encodeVarIntAscending(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], encoded);
        else
            PrefixTreeEncode::encodeVarIntDescending(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], encoded);
    }
    else
    {
        if (ascending)
            PrefixTreeEncode::encodeVarUIntAscending(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], encoded);
        else
            PrefixTreeEncode::encodeVarUIntDescending(assert_cast<const ColumnVector<T> &>(column).getData()[row_num], encoded);
    }
}

template <typename T>
void SerializationNumber<T>::deserializeBinaryPrefixTree(
    IColumn & column, std::string_view & data, const FormatSettings & settings, bool ascending) const
{
    assert_cast<ColumnVector<T> &>(column).getData().push_back(deserializeBinaryPrefixTree(data, settings, ascending));
}

template <typename T>
ColumnVector<T>::ValueType
SerializationNumber<T>::deserializeBinaryPrefixTree(std::string_view & data, [[maybe_unused]] const FormatSettings & settings, bool ascending) const
{
    using VT = typename ColumnVector<T>::ValueType;

    if constexpr (sizeof(VT) > 8)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prefix tree decoding for big number is not supported");

    VT val = 0;

    if constexpr (std::is_same_v<VT, Float64>)
    {
        if (ascending)
            val = static_cast<VT>(PrefixTreeEncode::decodeDoubleAscending(data));
        else
            val = static_cast<VT>(PrefixTreeEncode::decodeDoubleDescending(data));
    }
    else if constexpr (std::is_same_v<VT, Float32>)
    {
        if (ascending)
            val = static_cast<VT>(PrefixTreeEncode::decodeFloatAscending(data));
        else
            val = static_cast<VT>(PrefixTreeEncode::decodeFloatDescending(data));
    }
    else if constexpr (std::is_signed_v<VT>)
    {
        if (ascending)
            val = static_cast<VT>(PrefixTreeEncode::decodeVarIntAscending(data));
        else
            val = static_cast<VT>(PrefixTreeEncode::decodeVarIntDescending(data));
    }
    else
    {
        if (ascending)
            val = static_cast<VT>(PrefixTreeEncode::decodeVarUIntAscending(data));
        else
            val = static_cast<VT>(PrefixTreeEncode::decodeVarUIntDescending(data));
    }

    return val;
}

template <typename T>
void SerializationNumber<T>::deserializeBinaryPrefixTreeDiscard(
    std::string_view & data, const DB::FormatSettings & settings, bool ascending) const
{
    deserializeBinaryPrefixTree(data, settings, ascending);
}
/// proton: ends

template class SerializationNumber<UInt8>;
template class SerializationNumber<UInt16>;
template class SerializationNumber<UInt32>;
template class SerializationNumber<UInt64>;
template class SerializationNumber<UInt128>;
template class SerializationNumber<UInt256>;
template class SerializationNumber<Int8>;
template class SerializationNumber<Int16>;
template class SerializationNumber<Int32>;
template class SerializationNumber<Int64>;
template class SerializationNumber<Int128>;
template class SerializationNumber<Int256>;
template class SerializationNumber<Float32>;
template class SerializationNumber<Float64>;

}
