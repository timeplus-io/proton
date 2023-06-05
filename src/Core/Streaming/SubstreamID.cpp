#include <Core/Streaming/SubstreamID.h>

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

void deserialize(SubstreamID & id, ReadBuffer & rb)
{
    for (auto & i : id.items)
        readIntBinary(i, rb);
}
}

}
