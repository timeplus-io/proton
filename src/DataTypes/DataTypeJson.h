#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

class DataTypeJsonCustomName final : public IDataTypeCustomName
{
private:
    DataTypes elems;
    Strings names;

public:
    DataTypeJsonCustomName(const DataTypes & elems_, const Strings & names_) : elems(elems_), names(names_) { }

    String getName() const override;
};

DataTypePtr createJson(const DataTypes & types, const Names & names);

}
