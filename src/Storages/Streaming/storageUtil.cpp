#include "storageUtil.h"

#include <Storages/IStorage.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{
bool supportStreamingQuery(const StoragePtr & storage)
{
    const auto & name = storage->getName();
    return (name == "ProxyStream" || name == "Stream" || name == "View" || name == "MaterializedView" || name == "ExternalStream" || name == "Random");
}

String getStorageName(const ASTCreateQuery & create)
{
    if (create.is_dictionary)
        return "Dictionary";
    else if (create.is_ordinary_view)
        return "View";
    else if (create.is_materialized_view)
        return "MaterializedView";
    else if (create.is_external)
        return "ExternalStream";
    else if (create.is_random)
        return "RandomStream";
    else
        return "Stream";
}
}
