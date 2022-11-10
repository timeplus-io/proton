#include <Common/Streaming/Substream/Common.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming::Substream
{
void ID::serialize(DB::WriteBuffer & wb) const
{
    assert(!empty());
    (void) wb;
}

void ID::deserialize(DB::ReadBuffer & rb)
{
    (void) rb;
}
}
}
