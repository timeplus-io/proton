#pragma once

namespace nlog
{
/// All NativeLog functions report failures through this thread-local.
extern __thread int nl_err;
}
