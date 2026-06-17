#pragma once

#include <Python.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace ProfileEvents
{
extern const Event PythonGILAcquired;
extern const Event PythonGILWaitMicroseconds;
}

namespace CurrentMetrics
{
extern const Metric PythonGILWait;
}

namespace DB
{
namespace ErrorCodes
{
extern const int RESOURCE_NOT_INITED;
}
}

namespace DB::cpython
{

/// RAII guard for entering/leaving a Python runtime scope.
///
/// On GIL-enabled builds (CPython <= 3.12 or GIL-enabled 3.13+):
///   PyGILState_Ensure() acquires the GIL, serialising all Python execution.
///
/// On free-threaded builds (CPython 3.13+ with Py_GIL_DISABLED):
///   PyGILState_Ensure() attaches a PyThreadState to the calling thread
///   without global serialisation.  Multiple threads may execute Python
///   concurrently; callers are responsible for object-level thread safety.
///
/// In both modes the guard ensures the calling thread has a valid
/// PyThreadState for the lifetime of the scope.
class GILGuard
{
public:
    /// Returns true when the embedded interpreter was *built* with free-
    /// threading support (CPython 3.13+ compiled with Py_GIL_DISABLED).
    ///
    /// This is a build-capability check, NOT a runtime-behavior guarantee.
    /// Even in a free-threaded build the GIL can be re-enabled at runtime
    /// (PYTHON_GIL=1, -X gil=1, or importing a non-free-thread-safe
    /// extension).  Code that needs to know the actual runtime GIL state
    /// should query the interpreter (e.g. PyUnstable_GIL_IsEnabled() on
    /// 3.13+), not this flag.
    static constexpr bool buildSupportsFreeThreading() noexcept
    {
#ifdef Py_GIL_DISABLED
        return true;
#else
        return false;
#endif
    }

    /// Default: UDF / streaming contexts (need_cleanup = false).
    GILGuard() : GILGuard(false) { }

    /// \param need_cleanup_ Controls thread-state release on destruction:
    ///   - true:  PyGILState_Release — proper cleanup, suitable for
    ///            short-lived scopes (SYSTEM commands, tests).
    ///   - false: PyEval_SaveThread — detaches the thread state but
    ///            intentionally keeps the PyThreadState alive in
    ///            thread-local storage.  This avoids dangling-pointer
    ///            issues with libraries (e.g. pybind11) that cache
    ///            per-thread PyThreadState pointers, which matters when
    ///            Python work runs in a reusable thread pool.
    explicit GILGuard(bool need_cleanup_) : acquired(false), need_cleanup(need_cleanup_)
    {
        if (!Py_IsInitialized())
            throw DB::Exception(
                DB::ErrorCodes::RESOURCE_NOT_INITED, "Embedded Python interpreter is not initialized or the initialization failed");

        try
        {
            Stopwatch watch;
            {
                CurrentMetrics::Increment waiting{CurrentMetrics::PythonGILWait};
                state = PyGILState_Ensure();
            }
            ProfileEvents::increment(ProfileEvents::PythonGILAcquired);
            ProfileEvents::increment(ProfileEvents::PythonGILWaitMicroseconds, watch.elapsedMicroseconds());
            acquired = true;
        }
        catch (...)
        {
            throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_INITED, "Failed to enter Python runtime scope");
        }
    }

    ~GILGuard()
    {
        if (acquired)
        {
            try
            {
                if (need_cleanup)
                    PyGILState_Release(state);
                else
                    PyEval_SaveThread();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    GILGuard(const GILGuard &) = delete;
    GILGuard & operator=(const GILGuard &) = delete;

    bool isAcquired() const noexcept { return acquired; }

private:
    PyGILState_STATE state{};
    bool acquired;
    bool need_cleanup;
};
}
