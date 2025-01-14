#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
void collectCreateTelemetry(const StoragePtr & storage, ContextPtr context);
}
