#include "SerializationInfoObject.h"

namespace DB
{

MutableSerializationInfoPtr SerializationInfoObject::clone() const
{
    auto res = std::make_shared<SerializationInfoObject>(kind, settings);
    res->data = data;
    res->partial_deserialized_subcolumns = partial_deserialized_subcolumns;
    return res;
}

}
