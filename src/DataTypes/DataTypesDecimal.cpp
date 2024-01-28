#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/Serializations/SerializationDecimal.h>

#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/readDecimalText.h>
#include <Parsers/ASTLiteral.h>

#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int DECIMAL_OVERFLOW;
}


template <is_decimal T>
std::string DataTypeDecimal<T>::doGetName() const
{
    return fmt::format("decimal({}, {})", this->precision, this->scale);
}


template <is_decimal T>
bool DataTypeDecimal<T>::equals(const IDataType & rhs) const
{
    if (auto * ptype = typeid_cast<const DataTypeDecimal<T> *>(&rhs))
        return this->scale == ptype->getScale();
    return false;
}

template <is_decimal T>
DataTypePtr DataTypeDecimal<T>::promoteNumericType() const
{
    using PromotedType = DataTypeDecimal<Decimal128>;
    return std::make_shared<PromotedType>(PromotedType::maxPrecision(), this->scale);
}

template <is_decimal T>
T DataTypeDecimal<T>::parseFromString(const String & str) const
{
    ReadBufferFromMemory buf(str.data(), str.size());
    T x;
    UInt32 unread_scale = this->scale;
    readDecimalText(buf, x, this->precision, unread_scale, true);

    if (common::mulOverflow(x.value, DecimalUtils::scaleMultiplier<T>(unread_scale), x.value))
        throw Exception("The The decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);

    return x;
}

template <is_decimal T>
SerializationPtr DataTypeDecimal<T>::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationDecimal<T>>(this->precision, this->scale);
}


static DataTypePtr create(const ASTPtr & arguments/* proton: starts */, bool compatible_with_clickhouse [[maybe_unused]] = false/* proton: ends */)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("The decimal data type family must have exactly two arguments: precision and scale",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * precision = arguments->children[0]->as<ASTLiteral>();
    const auto * scale = arguments->children[1]->as<ASTLiteral>();

    if (!precision || precision->value.getType() != Field::Types::UInt64 ||
        !scale || !(scale->value.getType() == Field::Types::Int64 || scale->value.getType() == Field::Types::UInt64))
        throw Exception("The decimal data type family must have two numbers as its arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    UInt64 precision_value = precision->value.get<UInt64>();
    UInt64 scale_value = scale->value.get<UInt64>();

    return createDecimal<DataTypeDecimal>(precision_value, scale_value);
}

template <typename T>
static DataTypePtr createExact(const ASTPtr & arguments/* proton: starts */, bool compatible_with_clickhouse [[maybe_unused]] = false/* proton: ends */)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("The decimal data type family must have exactly two arguments: precision and scale",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * scale_arg = arguments->children[0]->as<ASTLiteral>();

    if (!scale_arg || !(scale_arg->value.getType() == Field::Types::Int64 || scale_arg->value.getType() == Field::Types::UInt64))
        throw Exception("The decimal data type family must have a two numbers as its arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    UInt64 precision = DecimalUtils::max_precision<T>;
    UInt64 scale = scale_arg->value.get<UInt64>();

    return createDecimal<DataTypeDecimal>(precision, scale);
}

void registerDataTypeDecimal(DataTypeFactory & factory)
{
    factory.registerDataType("decimal32", createExact<Decimal32>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("decimal64", createExact<Decimal64>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("decimal128", createExact<Decimal128>, DataTypeFactory::CaseInsensitive);
    factory.registerDataType("decimal256", createExact<Decimal256>, DataTypeFactory::CaseInsensitive);

    factory.registerDataType("decimal", create, DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("DEC", "decimal", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NUMERIC", "decimal", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("FIXED", "decimal", DataTypeFactory::CaseInsensitive);

    factory.registerClickHouseAlias("Decimal32", "decimal32");
    factory.registerClickHouseAlias("Decimal64", "decimal64");
    factory.registerClickHouseAlias("Decimal128", "decimal128");
    factory.registerClickHouseAlias("Decimal256", "decimal256");

    factory.registerClickHouseAlias("Decimal", "decimal");
}

/// Explicit template instantiations.
template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128>;
template class DataTypeDecimal<Decimal256>;

}
