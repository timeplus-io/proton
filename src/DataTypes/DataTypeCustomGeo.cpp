#include <DataTypes/DataTypeCustomGeo.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

void registerDataTypeDomainGeo(DataTypeFactory & factory)
{
    // Custom type for point represented as its coordinates stored as Tuple(Float64, Float64)
    factory.registerSimpleDataTypeCustom("point", []
    {
        return std::make_pair(DataTypeFactory::instance().get("tuple(float64, float64)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypePointName>()));
    });

    // Custom type for simple polygon without holes stored as Array(Point)
    factory.registerSimpleDataTypeCustom("ring", []
    {
        return std::make_pair(DataTypeFactory::instance().get("array(point)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeRingName>()));
    });

    // Custom type for polygon with holes stored as Array(Ring)
    // First element of outer array is outer shape of polygon and all the following are holes
    factory.registerSimpleDataTypeCustom("polygon", []
    {
        return std::make_pair(DataTypeFactory::instance().get("array(ring)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypePolygonName>()));
    });

    // Custom type for multiple polygons with holes stored as Array(Polygon)
    factory.registerSimpleDataTypeCustom("multi_polygon", []
    {
        return std::make_pair(DataTypeFactory::instance().get("array(polygon)"),
            std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeMultiPolygonName>()));
    });

    /// proton: starts
    factory.registerClickHouseAlias("Point", "point");
    factory.registerClickHouseAlias("Ring", "ring");
    factory.registerClickHouseAlias("Polygon", "polygon");
    factory.registerClickHouseAlias("MultiPolygon", "multi_polygon");
    /// proton: ends
}

}
