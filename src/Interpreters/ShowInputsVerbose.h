#pragma once

#include <memory>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

class ASTShowTablesQuery;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

QueryPipeline executeShowInputsVerbose(const ASTShowTablesQuery & query, ContextPtr context);

}
