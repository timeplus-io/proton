#pragma once
#include <Core/Names.h>
#include <DataTypes/Serializations/SerializationInfo.h>

namespace DB
{

class SerializationInfoObject : public SerializationInfo
{
public:
    using SerializationInfo::SerializationInfo;

    void addPartialDeserializedSubcolumns(Names subcolumns_) { partial_deserialized_subcolumns = std::move(subcolumns_); }
    const Names & getPartialDeserializedSubcolumns() const { return partial_deserialized_subcolumns; }
    MutableSerializationInfoPtr clone() const override;

protected:
    Names partial_deserialized_subcolumns;
};

}
