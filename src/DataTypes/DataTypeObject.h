#pragma once

#include <DataTypes/IDataType.h>
#include <Core/Field.h>
#include <Columns/ColumnObject.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class DataTypeObject : public IDataType
{
private:
    String schema_format;
    bool is_nullable;

public:
    DataTypeObject(const String & schema_format_, bool is_nullable_);

    const char * getFamilyName() const override { return "json"; }
    String doGetName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::Object; }

    MutableColumnPtr createColumn() const override { return ColumnObject::create(is_nullable); }

    Field getDefault() const override
    {
        throw Exception("Method getDefault() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    bool haveSubtypes() const override { return false; }
    bool equals(const IDataType & rhs) const override;
    bool isParametric() const override { return true; }
    bool hasDynamicSubcolumns() const override { return true; }

    SerializationPtr doGetDefaultSerialization() const override;
    /// proton: starts.
    SerializationInfoPtr getSerializationInfo(const IColumn & column) const override;
    SerializationPtr getSerialization(const SerializationInfo & info) const override;
    MutableSerializationInfoPtr createSerializationInfo(const SerializationInfo::Settings & settings) const override;
    /// proton: ends.

    bool hasNullableSubcolumns() const { return is_nullable; }
};

}
