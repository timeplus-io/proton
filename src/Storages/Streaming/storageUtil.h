#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{
/// returns whether the storage supports streaming queries
bool supportStreamingQuery(const StoragePtr & storage);

class ASTCreateQuery;
String getStorageName(const ASTCreateQuery & create);

bool isStreamingStorage(const StoragePtr & storage, ContextPtr context);
}
