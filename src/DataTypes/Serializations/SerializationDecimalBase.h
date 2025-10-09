#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <Columns/ColumnDecimal.h>

namespace DB
{

template <typename T>
class SerializationDecimalBase : public SimpleTextSerialization
{
protected:
    const UInt32 precision;
    const UInt32 scale;

public:
    using FieldType = T;
    using ColumnType = ColumnDecimal<T>;

    SerializationDecimalBase(UInt32 precision_, UInt32 scale_)
        : precision(precision_), scale(scale_) {}

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    /// proton: starts
    void deserializeBinaryBulkDiscard(ReadBuffer & istr, size_t limit) const override;

    void serializeBinaryPrefixTree(const Field & field, String & encoded, const FormatSettings & settings, bool ascending) const override;

    void serializeBinaryPrefixTree(
        const IColumn & column, size_t row_num, String & encoded, const FormatSettings & settings, bool ascending) const override;

    void deserializeBinaryPrefixTree(
        IColumn & column, std::string_view & data, const DB::FormatSettings & settings, bool ascending) const override;

    void deserializeBinaryPrefixTreeDiscard(std::string_view & data, const DB::FormatSettings & settings, bool ascending) const override;

private:
    T deserializeBinaryPrefixTree(std::string_view & data, const DB::FormatSettings & settings, bool ascending) const;
    /// proton: ends
};

}
