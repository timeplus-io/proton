#include "storageUtil.h"

#include <Storages/IStorage.h>

namespace DB
{
bool supportStreamingQuery(const StoragePtr & storage)
{
    const auto & name = storage->getName();
    return (name == "ProxyStream" || name == "Stream" || name == "View" || name == "MaterializedView" || name == "ExternalStream" || name == "Random");
}
}
