#include <Storages/Stream/storageUtil.h>

#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/Proxy/ProxyStream.h>
#include <Storages/StorageView.h>

namespace DB
{
String getStorageName(const ASTCreateQuery & create)
{
    switch (create.type)
    {
        case ASTCreateQuery::Type::Database:
            return "Database";
        case ASTCreateQuery::Type::RandomStream:
            return "RandomStream";
        case ASTCreateQuery::Type::NullStream:
            return "NullStream";
        case ASTCreateQuery::Type::ExternalStream:
            return "ExternalStream";
        case ASTCreateQuery::Type::ExternalTable:
            return "ExternalTable";
        case ASTCreateQuery::Type::Dictionary:
            return "Dictionary";
        case ASTCreateQuery::Type::View:
            return "View";
        case ASTCreateQuery::Type::MaterializedView:
            return "MaterializedView";
        case ASTCreateQuery::Type::ScheduledMaterializedView:
            return "ScheduledMaterializedView";
        default:
            return "Stream";
    }
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
