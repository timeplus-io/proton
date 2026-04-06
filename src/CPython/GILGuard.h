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
class GILGuard
{
public:
    /// Constructor for UDF contexts where pybind11 compatibility is needed (default behavior)
    GILGuard() : GILGuard(false) { }

    /// Constructor with explicit cleanup control
    /// \param use_need_cleanup if true, uses PyGILState_Release for proper cleanup
    ///                           if false, uses PyEval_SaveThread for pybind11 compatibility
    explicit GILGuard(bool use_need_cleanup) : acquired(false), need_cleanup(use_need_cleanup)
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
            /// If PyGILState_Ensure fails, don't set acquired=true to avoid cleanup issues
            throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_INITED, "Failed to acquire Python GIL");
        }
    }

    /// https://github.com/timeplus-io/proton-enterprise/issues/7226
    /// `PyGILState_Release` will release the `PyThreadState` object when its reference
    /// count drops to zero, while `PyEval_SaveThread` will not release the `PyThreadState`
    /// object.
    /// Some Python libraries like pybind11 assume that `PyThreadState` is bound to the thread
    /// itself, with each thread having a `PyThreadState` and storing a pointer to this
    /// object internally.
    /// In our case, Python UDFs are called within a thread pool. If the `PyThreadState`
    /// object is destructed after a query execution ends, it will cause a dangling pointer
    /// issue in pybind11.
    /// For UDF contexts, this leads to a leak of the `PyThreadState` object in the thread
    /// local storage to avoid dangling pointers.
    /// For non-UDF contexts (like SYSTEM commands), we use proper cleanup to avoid
    /// shutdown hangs.
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

    /// Check if GIL was successfully acquired
    bool isAcquired() const noexcept { return acquired; }

private:
    PyGILState_STATE state{};
    bool acquired;
    bool need_cleanup;
};
}
