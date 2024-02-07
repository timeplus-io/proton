#pragma once

#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Common/IFactoryWithAliases.h>
#include <DataTypes/DataTypeCustom.h>


#include <functional>
#include <memory>
#include <unordered_map>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;


/** Creates a data type by name of data type family and parameters.
  */
class DataTypeFactory final : private boost::noncopyable, public IFactoryWithAliases<std::function<DataTypePtr(const ASTPtr & parameters, bool compatible_with_clickhouse [[maybe_unused]])>>
{
private:
    using SimpleCreator = std::function<DataTypePtr()>;
    using DataTypesDictionary = std::unordered_map<String, Value>;
    using CreatorWithCustom = std::function<std::pair<DataTypePtr, DataTypeCustomDescPtr>(const ASTPtr & parameters, bool compatible_with_clickhouse [[maybe_unused]])>;
    using SimpleCreatorWithCustom = std::function<std::pair<DataTypePtr,DataTypeCustomDescPtr>()>;

public:
    static DataTypeFactory & instance();

    /// proton: starts.
    DataTypePtr get(TypeIndex type) const;
    /// proton: ends.

    DataTypePtr get(const String & full_name, bool compatible_with_clickhouse = false) const; /// proton: updated
    DataTypePtr get(const String & family_name, const ASTPtr & parameters, bool compatible_with_clickhouse = false) const; /// proton: updated
    DataTypePtr get(const ASTPtr & ast, bool compatible_with_clickhouse = false) const; /// proton: updated
    DataTypePtr getCustom(DataTypeCustomDescPtr customization) const;

    /// Register a type family by its name.
    void registerDataType(const String & family_name, Value creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a simple data type, that have no parameters.
    void registerSimpleDataType(const String & name, SimpleCreator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a customized type family
    void registerDataTypeCustom(const String & family_name, CreatorWithCustom creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a simple customized data type
    void registerSimpleDataTypeCustom(const String & name, SimpleCreatorWithCustom creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

private:
    const Value & findCreatorByName(const String & family_name) const;

    DataTypesDictionary data_types;

    /// Case insensitive data types will be additionally added here with lowercased name.
    DataTypesDictionary case_insensitive_data_types;

    DataTypeFactory();

    const DataTypesDictionary & getMap() const override { return data_types; }

    const DataTypesDictionary & getCaseInsensitiveMap() const override { return case_insensitive_data_types; }

    String getFactoryName() const override { return "DataTypeFactory"; }
};

void registerDataTypeNumbers(DataTypeFactory & factory);
void registerDataTypeDecimal(DataTypeFactory & factory);
void registerDataTypeDate(DataTypeFactory & factory);
void registerDataTypeDate32(DataTypeFactory & factory);
void registerDataTypeDateTime(DataTypeFactory & factory);
void registerDataTypeString(DataTypeFactory & factory);
void registerDataTypeFixedString(DataTypeFactory & factory);
void registerDataTypeEnum(DataTypeFactory & factory);
void registerDataTypeArray(DataTypeFactory & factory);
void registerDataTypeTuple(DataTypeFactory & factory);
void registerDataTypeMap(DataTypeFactory & factory);
void registerDataTypeNullable(DataTypeFactory & factory);
void registerDataTypeNothing(DataTypeFactory & factory);
void registerDataTypeUUID(DataTypeFactory & factory);
void registerDataTypeIPv4andIPv6(DataTypeFactory & factory);
void registerDataTypeAggregateFunction(DataTypeFactory & factory);
void registerDataTypeNested(DataTypeFactory & factory);
void registerDataTypeInterval(DataTypeFactory & factory);
void registerDataTypeLowCardinality(DataTypeFactory & factory);
void registerDataTypeDomainBool(DataTypeFactory & factory);
void registerDataTypeDomainSimpleAggregateFunction(DataTypeFactory & factory);
void registerDataTypeDomainGeo(DataTypeFactory & factory);
void registerDataTypeObject(DataTypeFactory & factory);

}
