#include <Common/FieldVisitorDump.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>


namespace DB
{

template <typename T>
static inline String formatQuotedWithPrefix(T x, const char * prefix)
{
    WriteBufferFromOwnString wb;
    writeCString(prefix, wb);
    writeQuoted(x, wb);
    return wb.str();
}

template <typename T>
static inline void writeQuoted(const DecimalField<T> & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x.getValue(), x.getScale(), buf, {});
    writeChar('\'', buf);
}

String FieldVisitorDump::operator() (const Null & x) const { return x.isNegativeInfinity() ? "-inf" : (x.isPositiveInfinity() ? "+inf" : "NULL"); }
String FieldVisitorDump::operator() (const UInt64 & x) const { return formatQuotedWithPrefix(x, "uint64_"); }
String FieldVisitorDump::operator() (const Int64 & x) const { return formatQuotedWithPrefix(x, "int64_"); }
String FieldVisitorDump::operator() (const Float64 & x) const { return formatQuotedWithPrefix(x, "float64_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal32> & x) const { return formatQuotedWithPrefix(x, "decimal32_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal64> & x) const { return formatQuotedWithPrefix(x, "decimal64_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal128> & x) const { return formatQuotedWithPrefix(x, "decimal128_"); }
String FieldVisitorDump::operator() (const DecimalField<Decimal256> & x) const { return formatQuotedWithPrefix(x, "decimal256_"); }
String FieldVisitorDump::operator() (const UInt128 & x) const { return formatQuotedWithPrefix(x, "uint128_"); }
String FieldVisitorDump::operator() (const UInt256 & x) const { return formatQuotedWithPrefix(x, "uint256_"); }
String FieldVisitorDump::operator() (const Int128 & x) const { return formatQuotedWithPrefix(x, "int128_"); }
String FieldVisitorDump::operator() (const Int256 & x) const { return formatQuotedWithPrefix(x, "int256_"); }
String FieldVisitorDump::operator() (const UUID & x) const { return formatQuotedWithPrefix(x, "uuid_"); }
String FieldVisitorDump::operator() (const IPv4 & x) const { return formatQuotedWithPrefix(x, "ipv4_"); }
String FieldVisitorDump::operator() (const IPv6 & x) const { return formatQuotedWithPrefix(x, "ipv6_"); }
String FieldVisitorDump::operator() (const bool & x) const { return formatQuotedWithPrefix(x, "bool_"); }


String FieldVisitorDump::operator() (const String & x) const
{
    WriteBufferFromOwnString wb;
    writeQuoted(x, wb);
    return wb.str();
}

String FieldVisitorDump::operator() (const Array & x) const
{
    WriteBufferFromOwnString wb;

    wb << "array_[";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ']';

    return wb.str();
}

String FieldVisitorDump::operator() (const Tuple & x) const
{
    WriteBufferFromOwnString wb;

    wb << "tuple_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const Map & x) const
{
    WriteBufferFromOwnString wb;

    wb << "map_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << applyVisitor(*this, *it);
    }
    wb << ')';

    return wb.str();
}

String FieldVisitorDump::operator() (const Object & x) const
{
    WriteBufferFromOwnString wb;

    wb << "object_(";
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
            wb << ", ";
        wb << "(" << it->first << ", " << applyVisitor(*this, it->second) << ")";
    }
    wb << ')';

    return wb.str();

}

String FieldVisitorDump::operator() (const AggregateFunctionStateData & x) const
{
    WriteBufferFromOwnString wb;
    wb << "aggregate_function_state_(";
    writeQuoted(x.name, wb);
    wb << ", ";
    writeQuoted(x.data, wb);
    wb << ')';
    return wb.str();
}

}

