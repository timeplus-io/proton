#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/TruncateStreamResponseData.h>

namespace cluster::protocol
{
void TruncateStreamResponseData::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    err.serialize(wb, Nulls::NullVersion);
}

void TruncateStreamResponseData::doDeserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    err.deserialize(rb, Nulls::NullVersion);
}
}
