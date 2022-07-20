#pragma once

#include <DataTypes/DataTypeNumberBase.h>

namespace DB
{

class DataTypeBool final : public DataTypeNumberBase<Bool>
{
public:
    using FieldType = UInt8;
    size_t getSizeOfValueInMemory() const override;

    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }
    bool canBeUsedAsVersion() const override { return true; }
    bool isSummable() const override { return true; }
    bool canBeUsedInBitOperations() const override { return true; }
    bool canBeUsedInBooleanContext() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBePromoted() const override { return false; }

    SerializationPtr doGetDefaultSerialization() const override;
};

}
