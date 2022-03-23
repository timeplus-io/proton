#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeFactory.h>


#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename T>
static DataTypePtr createNumericDataType(const ASTPtr & arguments)
{
    if (arguments)
    {
        if (std::is_integral_v<T>)
        {
            if (arguments->children.size() > 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "{} data type family must not have more than one argument - display width", TypeName<T>);
        }
        else
        {
            if (arguments->children.size() > 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "{} data type family must not have more than two arguments - total number of digits and number of digits following the decimal point", TypeName<T>);
        }
    }
    return std::make_shared<DataTypeNumber<T>>();
}


void registerDataTypeNumbers(DataTypeFactory & factory)
{
    factory.registerDataType("uint8", createNumericDataType<UInt8>);
    factory.registerDataType("uint16", createNumericDataType<UInt16>);
    factory.registerDataType("uint32", createNumericDataType<UInt32>);
    factory.registerDataType("uint64", createNumericDataType<UInt64>);

    factory.registerDataType("int8", createNumericDataType<Int8>);
    factory.registerDataType("int16", createNumericDataType<Int16>);
    factory.registerDataType("int32", createNumericDataType<Int32>);
    factory.registerDataType("int64", createNumericDataType<Int64>);

    factory.registerDataType("float32", createNumericDataType<Float32>);
    factory.registerDataType("float64", createNumericDataType<Float64>);

    factory.registerSimpleDataType("uint128", [] { return DataTypePtr(std::make_shared<DataTypeUInt128>()); });
    factory.registerSimpleDataType("uint256", [] { return DataTypePtr(std::make_shared<DataTypeUInt256>()); });

    factory.registerSimpleDataType("int128", [] { return DataTypePtr(std::make_shared<DataTypeInt128>()); });
    factory.registerSimpleDataType("int256", [] { return DataTypePtr(std::make_shared<DataTypeInt256>()); });

    /// These synonyms are added for compatibility.

    /// factory.registerAlias("tinyint", "int8", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("INT1", "int8", DataTypeFactory::CaseInsensitive);    /// MySQL
    factory.registerAlias("byte", "int8", DataTypeFactory::CaseInsensitive);    /// MS Access
    factory.registerAlias("smallint", "int16", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("int", "int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("integer", "int32", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("bigint", "int64", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("float", "float32", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("REAL", "float32", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("SINGLE", "float32", DataTypeFactory::CaseInsensitive);   /// MS Access
    factory.registerAlias("double", "float64", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("MEDIUMINT", "int32", DataTypeFactory::CaseInsensitive);    /// MySQL

    /// factory.registerAlias("DOUBLE PRECISION", "float64", DataTypeFactory::CaseInsensitive);

    /// MySQL
    /// factory.registerAlias("TINYINT SIGNED", "int8", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("INT1 SIGNED", "int8", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("SMALLINT SIGNED", "int16", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("MEDIUMINT SIGNED", "int32", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("INT SIGNED", "int32", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("INTEGER SIGNED", "int32", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("BIGINT SIGNED", "int64", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("TINYINT UNSIGNED", "uint8", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("INT1 UNSIGNED", "uint8", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("SMALLINT UNSIGNED", "uint16", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("MEDIUMINT UNSIGNED", "uint32", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("INT UNSIGNED", "uint32", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("INTEGER UNSIGNED", "uint32", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("BIGINT UNSIGNED", "uint64", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("BIT", "uint64", DataTypeFactory::CaseInsensitive);  /// MySQL
    /// factory.registerAlias("SET", "uint64", DataTypeFactory::CaseInsensitive);  /// MySQL
    /// factory.registerAlias("YEAR", "uint16", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("TIME", "int64", DataTypeFactory::CaseInsensitive);
}

}
