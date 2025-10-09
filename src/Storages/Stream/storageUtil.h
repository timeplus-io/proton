#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{
class ASTCreateQuery;
String getStorageName(const ASTCreateQuery & create);

bool isStreamingStorage(const StoragePtr & storage, ContextPtr context);
}
