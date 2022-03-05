#pragma once

#include <memory>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class IDatabase;

void attachSystemTablesServer(ContextPtr context, IDatabase & system_database);
void attachSystemTablesLocal(ContextPtr context, IDatabase & system_database);

}
