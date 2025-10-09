#pragma once

#include <Core/UUID.h>
#include <Cluster/Common/SerdeTag.h>

namespace cluster
{
SERDE using NodeUUID = DB::UUID;
SERDE using NodeID = uint64_t;
}
