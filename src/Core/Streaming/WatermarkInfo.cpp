#include "WatermarkInfo.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming
{
void serialize(const SubstreamID & id, WriteBuffer & wb)
{
    for (auto i : id.items)
        writeIntBinary(i, wb);
}

SubstreamID deserialize(ReadBuffer & rb)
{
    SubstreamID id{};
    for (auto & i : id.items)
        readIntBinary(i, rb);

    return id;
}
}

}
