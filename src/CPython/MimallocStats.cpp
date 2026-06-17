#include "config.h"

#include <CPython/MimallocStats.h>

#if USE_PYTHON_UDF
/// pyconfig.h carries Py_GIL_DISABLED; patchlevel.h alone does not pull it in.
#include <patchlevel.h>
#include <pyconfig.h>

#ifdef Py_GIL_DISABLED
/// mimalloc is statically compiled into libpython-static when the embedded
/// CPython is built free-threaded. The header lives under
/// contrib/cpython/Include/internal/mimalloc/, and the ch_contrib::python
/// target exposes Include/internal on its public include path.
#include <mimalloc/mimalloc.h>
#define TIMEPLUS_HAVE_MIMALLOC 1
#endif

#endif

namespace DB::cpython
{

#ifdef TIMEPLUS_HAVE_MIMALLOC

bool mimallocEnabled()
{
    return true;
}

MimallocProcessInfo getMimallocProcessInfo()
{
    size_t elapsed_msecs = 0;
    size_t user_msecs = 0;
    size_t system_msecs = 0;
    size_t current_rss = 0;
    size_t peak_rss = 0;
    size_t current_commit = 0;
    size_t peak_commit = 0;
    size_t page_faults = 0;

    ::mi_process_info(
        &elapsed_msecs,
        &user_msecs,
        &system_msecs,
        &current_rss,
        &peak_rss,
        &current_commit,
        &peak_commit,
        &page_faults);

    MimallocProcessInfo info;
    info.current_commit = current_commit;
    info.peak_commit = peak_commit;
    return info;
}

#else

bool mimallocEnabled()
{
    return false;
}

MimallocProcessInfo getMimallocProcessInfo()
{
    return {};
}

#endif

}
