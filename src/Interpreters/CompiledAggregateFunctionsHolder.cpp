#include "CompiledAggregateFunctionsHolder.h"

#include <Interpreters/JIT/CHJIT.h>

namespace DB
{
#if USE_EMBEDDED_COMPILER

CHJIT & getJITInstance()
{
    static CHJIT jit;
    return jit;
}

CompiledAggregateFunctionsHolder::CompiledAggregateFunctionsHolder(CompiledAggregateFunctions compiled_function_)
    : CompiledExpressionCacheEntry(compiled_function_.compiled_module.size), compiled_aggregate_functions(compiled_function_)
{
}

CompiledAggregateFunctionsHolder::~CompiledAggregateFunctionsHolder()
{
    getJITInstance().deleteCompiledModule(compiled_aggregate_functions.compiled_module);
}

#endif

}
