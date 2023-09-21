#include "storageUtil.h"

#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageView.h>
#include <Storages/Streaming/ProxyStream.h>

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

bool isStreamingStorage(const StoragePtr & storage, ContextPtr context)
{
    if (!supportStreamingQuery(storage))
        return false;

    if (const auto * proxy = storage->as<Streaming::ProxyStream>())
        return proxy->isStreaming();

    if (storage->as<StorageView>())
    {
        auto select = storage->getInMemoryMetadataPtr()->getSelectQuery().inner_query;
        auto ctx = Context::createCopy(context);
        ctx->setCollectRequiredColumns(false);
        return InterpreterSelectWithUnionQuery(select, ctx, SelectQueryOptions().noModify().subquery().analyze()).isStreaming();
    }

    return true;
}
}
