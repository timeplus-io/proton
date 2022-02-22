#pragma once

#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/JIT/compileFunction.h>

namespace DB
{
#if USE_EMBEDDED_COMPILER

class CHJIT;

CHJIT & getJITInstance();

class CompiledAggregateFunctionsHolder final : public CompiledExpressionCacheEntry
{
public:
    explicit CompiledAggregateFunctionsHolder(CompiledAggregateFunctions compiled_function_);

    ~CompiledAggregateFunctionsHolder() override;

    CompiledAggregateFunctions compiled_aggregate_functions;
};

#endif

}
