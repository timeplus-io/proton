#pragma once

#include <DataTypes/DataTypeCustom.h>

namespace DB
{

class DataTypePointName : public DataTypeCustomFixedName
{
public:
    DataTypePointName() : DataTypeCustomFixedName("point") {}
};

class DataTypeRingName : public DataTypeCustomFixedName
{
public:
    DataTypeRingName() : DataTypeCustomFixedName("ring") {}
};

class DataTypePolygonName : public DataTypeCustomFixedName
{
public:
    DataTypePolygonName() : DataTypeCustomFixedName("polygon") {}
};

class DataTypeMultiPolygonName : public DataTypeCustomFixedName
{
public:
    DataTypeMultiPolygonName() : DataTypeCustomFixedName("multi_polygon") {}
};

}
