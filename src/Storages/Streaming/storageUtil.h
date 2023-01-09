#pragma once

#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{
/// returns whether the storage supports streaming queries
bool supportStreamingQuery(const StoragePtr & storage);
}
