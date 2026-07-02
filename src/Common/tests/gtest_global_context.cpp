#include "gtest_global_context.h"

namespace DB
{
extern Context * g_global_ctx;
}

const ContextHolder & getContext()
{
    return getMutableContext();
}

ContextHolder & getMutableContext()
{
    static ContextHolder holder;
    [[maybe_unused]] static const bool registered = [] {
        DB::g_global_ctx = holder.context.get();
        return true;
    }();
    return holder;
}

void destroyContext()
{
    auto & holder = getMutableContext();
    /// Nullify first so post-destroy callers chassert instead of UAF.
    DB::g_global_ctx = nullptr;
    holder.destroy();
}
