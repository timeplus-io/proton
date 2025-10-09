#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/ErrorResponseData.h>

namespace cluster::protocol
{
void ErrorResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);
}

void ErrorResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);
}

size_t ErrorResponseData::approximateSerializedSize() const noexcept
{
    return err.approximateSerializedSize();
}

std::string ErrorResponseData::doString() const
{
    return err.string();
}
}
