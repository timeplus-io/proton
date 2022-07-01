#pragma once

#include <DataTypes/DataTypesNumber.h>

namespace DB
{

class DataTypeBool final : public DataTypeNumberBase<UInt8>
{
public:
    const char * getFamilyName() const override { return TypeName<Bool>.data(); }
    TypeIndex getTypeId() const override { return TypeToTypeIndex<Bool>; }
    MutableColumnPtr createColumn() const override;

    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }

    bool canBeUsedAsVersion() const override { return true; }
    bool isSummable() const override { return true; }
    bool canBeUsedInBitOperations() const override { return true; }
    bool canBeUsedInBooleanContext() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    bool canBePromoted() const override { return true; }
    DataTypePtr promoteNumericType() const override
    {
        using PromotedType = DataTypeNumber<NearestFieldType<Bool>>;
        return std::make_shared<PromotedType>();
    }

    SerializationPtr doGetDefaultSerialization() const override;
};

}
