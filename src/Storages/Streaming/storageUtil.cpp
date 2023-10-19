#include <Storages/Streaming/storageUtil.h>

#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/StorageView.h>
#include <Storages/Streaming/ProxyStream.h>

namespace DB
{
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

bool isStreamingStorage(const StoragePtr & storage, ContextPtr context)
{
    if (const auto * proxy = storage->as<Streaming::ProxyStream>(); proxy)
        return proxy->isStreamingQuery();

    if (const auto * view = storage->as<StorageView>(); view)
        return view->isStreamingQuery(context);

    return storage->supportsStreamingQuery();
}
}
