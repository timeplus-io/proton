#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/readDecimalText.h>
#include <Core/Field.h>
#include <Core/DecimalComparison.h>
#include <Common/FieldVisitorDump.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorWriteBinary.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_FROM_FIELD_DUMP;
    extern const int DECIMAL_OVERFLOW;
}

inline Field getBinaryValue(UInt8 type, ReadBuffer & buf)
{
    switch (type)
    {
        case Field::Types::Null:
        {
            return Field();
        }
        case Field::Types::UInt64:
        {
            UInt64 value;
            readVarUInt(value, buf);
            return value;
        }
        case Field::Types::UInt128:
        {
            UInt128 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::UInt256:
        {
            UInt256 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::UUID:
        {
            UUID value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::IPv4:
        {
            IPv4 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::IPv6:
        {
            IPv6 value;
            readBinary(value.toUnderType(), buf);
            return value;
        }
        case Field::Types::Int64:
        {
            Int64 value;
            readVarInt(value, buf);
            return value;
        }
        case Field::Types::Int128:
        {
            Int128 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Int256:
        {
            Int256 value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Float64:
        {
            Float64 value;
            readFloatBinary(value, buf);
            return value;
        }
        case Field::Types::String:
        {
            std::string value;
            readStringBinary(value, buf);
            return value;
        }
        case Field::Types::Array:
        {
            Array value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Tuple:
        {
            Tuple value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Map:
        {
            Map value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::Object:
        {
            Object value;
            readBinary(value, buf);
            return value;
        }
        case Field::Types::AggregateFunctionState:
        {
            AggregateFunctionStateData value;
            readStringBinary(value.name, buf);
            readStringBinary(value.data, buf);
            return value;
        }
        case Field::Types::Bool:
        {
            UInt8 value;
            readBinary(value, buf);
            return bool(value);
        }
    }
    return Field();
}

void readBinary(Array & x, ReadBuffer & buf)
{
    size_t size;
    UInt8 type;
    readBinary(type, buf);
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
        x.push_back(getBinaryValue(type, buf));
}

void writeBinary(const Array & x, WriteBuffer & buf)
{
    UInt8 type = Field::Types::Null;
    size_t size = x.size();
    if (size)
        type = x.front().getType();
    writeBinary(type, buf);
    writeBinary(size, buf);

    for (const auto & elem : x)
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, elem);
}

void writeText(const Array & x, WriteBuffer & buf)
{
    String res = applyVisitor(FieldVisitorToString(), Field(x));
    buf.write(res.data(), res.size());
}

void readBinary(Tuple & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        readBinary(type, buf);
        x.push_back(getBinaryValue(type, buf));
    }
}

void writeBinary(const Tuple & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & elem : x)
    {
        const UInt8 type = elem.getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, elem);
    }
}

void writeText(const Tuple & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

void readBinary(Map & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        readBinary(type, buf);
        x.push_back(getBinaryValue(type, buf));
    }
}

void writeBinary(const Map & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & elem : x)
    {
        const UInt8 type = elem.getType();
        writeBinary(type, buf);
        Field::dispatch([&buf] (const auto & value) { FieldVisitorWriteBinary()(value, buf); }, elem);
    }
}

void writeText(const Map & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

void readBinary(Object & x, ReadBuffer & buf)
{
    size_t size;
    readBinary(size, buf);

    for (size_t index = 0; index < size; ++index)
    {
        UInt8 type;
        String key;
        readBinary(type, buf);
        readBinary(key, buf);
        x[key] = getBinaryValue(type, buf);
    }
}

void writeBinary(const Object & x, WriteBuffer & buf)
{
    const size_t size = x.size();
    writeBinary(size, buf);

    for (const auto & [key, value] : x)
    {
        const UInt8 type = value.getType();
        writeBinary(type, buf);
        writeBinary(key, buf);
        Field::dispatch([&buf] (const auto & val) { FieldVisitorWriteBinary()(val, buf); }, value);
    }
}

void writeText(const Object & x, WriteBuffer & buf)
{
    writeFieldText(Field(x), buf);
}

template <typename T>
void readQuoted(DecimalField<T> & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    T value;
    UInt32 scale;
    int32_t exponent;
    uint32_t max_digits = static_cast<uint32_t>(-1);
    readDigits<true>(buf, value, max_digits, exponent, true);
    if (exponent > 0)
    {
        scale = 0;
        if (common::mulOverflow(value.value, DecimalUtils::scaleMultiplier<T>(exponent), value.value))
            throw Exception("The decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }
    else
        scale = -exponent;
    assertChar('\'', buf);
    x = DecimalField<T>{value, scale};
}

template void readQuoted<Decimal32>(DecimalField<Decimal32> & x, ReadBuffer & buf);
template void readQuoted<Decimal64>(DecimalField<Decimal64> & x, ReadBuffer & buf);
template void readQuoted<Decimal128>(DecimalField<Decimal128> & x, ReadBuffer & buf);
template void readQuoted<Decimal256>(DecimalField<Decimal256> & x, ReadBuffer & buf);

void writeFieldText(const Field & x, WriteBuffer & buf)
{
    String res = Field::dispatch(FieldVisitorToString(), x);
    buf.write(res.data(), res.size());
}


String Field::dump() const
{
    return applyVisitor(FieldVisitorDump(), *this);
}

Field Field::restoreFromDump(std::string_view dump_)
{
    auto show_error = [&dump_]
    {
        throw Exception("Couldn't restore Field from dump: " + String{dump_}, ErrorCodes::CANNOT_RESTORE_FROM_FIELD_DUMP);
    };

    std::string_view dump = dump_;
    trim(dump);

    if (dump == "NULL")
        return {};

    std::string_view prefix = std::string_view{"int64_"};
    if (dump.starts_with(prefix))
    {
        Int64 value = parseFromString<Int64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"uint64_"};
    if (dump.starts_with(prefix))
    {
        UInt64 value = parseFromString<UInt64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"int128_"};
    if (dump.starts_with(prefix))
    {
        Int128 value = parseFromString<Int128>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"uint128_"};
    if (dump.starts_with(prefix))
    {
        UInt128 value = parseFromString<UInt128>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"int256_"};
    if (dump.starts_with(prefix))
    {
        Int256 value = parseFromString<Int256>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"uint256_"};
    if (dump.starts_with(prefix))
    {
        UInt256 value = parseFromString<UInt256>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"float64_"};
    if (dump.starts_with(prefix))
    {
        Float64 value = parseFromString<Float64>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"decimal32_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal32> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    prefix = std::string_view{"decimal64_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal64> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    prefix = std::string_view{"decimal128_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal128> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    prefix = std::string_view{"decimal256_"};
    if (dump_.starts_with(prefix))
    {
        DecimalField<Decimal256> decimal;
        ReadBufferFromString buf{dump.substr(prefix.length())};
        readQuoted(decimal, buf);
        return decimal;
    }

    if (dump.starts_with("\'"))
    {
        String str;
        ReadBufferFromString buf{dump};
        readQuoted(str, buf);
        return str;
    }

    prefix = std::string_view{"bool_"};
    if (dump.starts_with(prefix))
    {
        bool value = parseFromString<bool>(dump.substr(prefix.length()));
        return value;
    }

    prefix = std::string_view{"array_["};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Array array;
        while (tail != "]")
        {
            size_t separator = tail.find_first_of(",]");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != "]")
                show_error();
            array.push_back(Field::restoreFromDump(element));
        }
        return array;
    }

    prefix = std::string_view{"tuple_("};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Tuple tuple;
        while (tail != ")")
        {
            size_t separator = tail.find_first_of(",)");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != ")")
                show_error();
            tuple.push_back(Field::restoreFromDump(element));
        }
        return tuple;
    }

    prefix = std::string_view{"map_("};
    if (dump.starts_with(prefix))
    {
        std::string_view tail = dump.substr(prefix.length());
        trimLeft(tail);
        Map map;
        while (tail != ")")
        {
            size_t separator = tail.find_first_of(",)");
            if (separator == std::string_view::npos)
                show_error();
            bool comma = (tail[separator] == ',');
            std::string_view element = tail.substr(0, separator);
            tail.remove_prefix(separator);
            if (comma)
                tail.remove_prefix(1);
            trimLeft(tail);
            if (!comma && tail != ")")
                show_error();
            map.push_back(Field::restoreFromDump(element));
        }
        return map;
    }

    prefix = std::string_view{"aggregate_function_state_("};
    if (dump.starts_with(prefix))
    {
        std::string_view after_prefix = dump.substr(prefix.length());
        size_t comma = after_prefix.find(',');
        size_t end = after_prefix.find(')', comma + 1);
        if ((comma == std::string_view::npos) || (end != after_prefix.length() - 1))
            show_error();
        std::string_view name_view = after_prefix.substr(0, comma);
        std::string_view data_view = after_prefix.substr(comma + 1, end - comma - 1);
        trim(name_view);
        trim(data_view);
        ReadBufferFromString name_buf{name_view};
        ReadBufferFromString data_buf{data_view};
        AggregateFunctionStateData res;
        readQuotedString(res.name, name_buf);
        readQuotedString(res.data, data_buf);
        return res;
    }

    show_error();
    UNREACHABLE();
}


template <typename T>
bool decimalEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool decimalLess(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
bool decimalLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale)
{
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}


template bool decimalEqual<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalEqual<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);

template bool decimalLess<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLess<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);

template bool decimalLessOrEqual<Decimal32>(Decimal32 x, Decimal32 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal64>(Decimal64 x, Decimal64 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal128>(Decimal128 x, Decimal128 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<Decimal256>(Decimal256 x, Decimal256 y, UInt32 x_scale, UInt32 y_scale);
template bool decimalLessOrEqual<DateTime64>(DateTime64 x, DateTime64 y, UInt32 x_scale, UInt32 y_scale);


inline void writeText(const Null & x, WriteBuffer & buf)
{
    if (x.isNegativeInfinity())
        writeText("-inf", buf);
    if (x.isPositiveInfinity())
        writeText("+inf", buf);
    else
        writeText("NULL", buf);
}

String toString(const Field & x)
{
    return Field::dispatch(
        [] (const auto & value)
        {
            // Use explicit type to prevent implicit construction of Field and
            // infinite recursion into toString<Field>.
            return toString<decltype(value)>(value);
        },
        x);
}

String fieldTypeToString(Field::Types::Which type)
{
    switch (type)
    {
        case Field::Types::Which::Null: return "null";
        case Field::Types::Which::Array: return "array";
        case Field::Types::Which::Tuple: return "tuple";
        case Field::Types::Which::Map: return "map";
        case Field::Types::Which::Object: return "json";
        case Field::Types::Which::AggregateFunctionState: return "aggregate_function_state";
        case Field::Types::Which::Bool: return "bool";
        case Field::Types::Which::String: return "string";
        case Field::Types::Which::Decimal32: return "decimal32";
        case Field::Types::Which::Decimal64: return "decimal64";
        case Field::Types::Which::Decimal128: return "decimal128";
        case Field::Types::Which::Decimal256: return "decimal256";
        case Field::Types::Which::Float64: return "float64";
        case Field::Types::Which::Int64: return "int64";
        case Field::Types::Which::Int128: return "int128";
        case Field::Types::Which::Int256: return "int256";
        case Field::Types::Which::UInt64: return "uint64";
        case Field::Types::Which::UInt128: return "uint128";
        case Field::Types::Which::UInt256: return "uint256";
        case Field::Types::Which::UUID: return "uuid";
        case Field::Types::Which::IPv4: return "ipv4";
        case Field::Types::Which::IPv6: return "ipv6";
    }
}

}
