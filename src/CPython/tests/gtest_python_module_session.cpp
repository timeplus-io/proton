#include "config.h"

#if USE_PYTHON_UDF
#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/PythonModuleSession.h>
#include <CPython/Utils.h>
#include <Common/Exception.h>

#include <CPython/tests/CPythonTest.h>

using namespace DB::cpython;

namespace
{
/// Python source recording every hook invocation into builtins so the order
/// survives module unload and can be inspected from the test after close.
const char * hooks_source = R"(
import builtins

def init_hooks():
    builtins._tp_session_hook_calls = []

def write_row():
    builtins._tp_session_hook_calls.append('write')

def flush_buffer():
    builtins._tp_session_hook_calls.append('flush')

def failing_flush():
    builtins._tp_session_hook_calls.append('flush_failed')
    raise RuntimeError('flush exploded')

def deinit_hooks():
    builtins._tp_session_hook_calls.append('deinit')
)";

std::string getRecordedHookCalls()
{
    GILGuard gil_guard(/*use_need_cleanup=*/true);
    PyObjectPtr builtins{PyImport_ImportModule("builtins")};
    if (!builtins)
        return "<no builtins>";

    PyObjectPtr value{PyObject_GetAttrString(builtins.get(), "_tp_session_hook_calls")};
    if (!value)
    {
        PyErr_Clear();
        return "<missing>";
    }

    return convertPyObjectToString(value);
}

void clearRecordedHookCalls()
{
    GILGuard gil_guard(/*use_need_cleanup=*/true);
    PyObjectPtr builtins{PyImport_ImportModule("builtins")};
    if (builtins && PyObject_HasAttrString(builtins.get(), "_tp_session_hook_calls"))
        PyObject_DelAttrString(builtins.get(), "_tp_session_hook_calls");
}

void executeEntryFunction(const PythonModuleSessionPtr & session)
{
    GILGuard gil_guard(/*use_need_cleanup=*/true);
    auto py_args = PyObjectPtr{PyTuple_New(0)};
    session->execute(py_args);
}

/// The CPythonTest suite assumes the main thread keeps holding the GIL (the state
/// right after Py_Initialize). PythonModuleSession's internal GILGuard releases it
/// with PyEval_SaveThread, so re-acquire it when the test ends to keep later tests
/// in the suite working.
struct RestoreMainThreadGIL
{
    ~RestoreMainThreadGIL()
    {
        if (Py_IsInitialized() && !PyGILState_Check())
            PyGILState_Ensure();
    }
};
}

TEST_F(CPythonTest, moduleSessionFlushHookOrder)
{
    RestoreMainThreadGIL restore_gil;

    auto session = PythonModuleSession::create(
        "TestFlushHook",
        PythonFunction{
            .init_function_name = "init_hooks",
            .init_parameters = {},
            .deinit_function_name = "deinit_hooks",
            .flush_function_name = "flush_buffer",
            .entry_function_name = "write_row",
            .source_code = hooks_source,
        });

    executeEntryFunction(session);

    /// Explicit flushes, the way PythonSink flushes on every checkpoint
    session->flush();
    session->flush();

    executeEntryFunction(session);

    /// Graceful close must flush once more (before deinit) so buffered rows are not lost
    PythonModuleSession::closeSession(session, /*ignore_exceptions=*/false);

    EXPECT_EQ(getRecordedHookCalls(), "['write', 'flush', 'flush', 'write', 'flush', 'deinit']");

    clearRecordedHookCalls();
}

TEST_F(CPythonTest, moduleSessionFlushNotConfigured)
{
    RestoreMainThreadGIL restore_gil;

    auto session = PythonModuleSession::create(
        "TestNoFlushHook",
        PythonFunction{
            .init_function_name = "init_hooks",
            .init_parameters = {},
            .deinit_function_name = "deinit_hooks",
            .flush_function_name = {},
            .entry_function_name = "write_row",
            .source_code = hooks_source,
        });

    executeEntryFunction(session);

    /// No flush function configured, flush() must be a no-op
    session->flush();

    PythonModuleSession::closeSession(session, /*ignore_exceptions=*/false);

    EXPECT_EQ(getRecordedHookCalls(), "['write', 'deinit']");

    clearRecordedHookCalls();
}

TEST_F(CPythonTest, moduleSessionFlushAfterCloseIsNoop)
{
    RestoreMainThreadGIL restore_gil;

    auto session = PythonModuleSession::create(
        "TestFlushAfterClose",
        PythonFunction{
            .init_function_name = "init_hooks",
            .init_parameters = {},
            .deinit_function_name = "deinit_hooks",
            .flush_function_name = "flush_buffer",
            .entry_function_name = "write_row",
            .source_code = hooks_source,
        });

    session->close(/*ignore_exceptions=*/false);

    EXPECT_NO_THROW(session->flush());

    EXPECT_EQ(getRecordedHookCalls(), "['flush', 'deinit']");

    clearRecordedHookCalls();
}

TEST_F(CPythonTest, moduleSessionExplicitFlushErrorPropagates)
{
    RestoreMainThreadGIL restore_gil;

    auto session = PythonModuleSession::create(
        "TestFlushError",
        PythonFunction{
            .init_function_name = "init_hooks",
            .init_parameters = {},
            .deinit_function_name = "deinit_hooks",
            .flush_function_name = "failing_flush",
            .entry_function_name = "write_row",
            .source_code = hooks_source,
        });

    EXPECT_THROW(session->flush(), DB::Exception);

    PythonModuleSession::closeSession(session, /*ignore_exceptions=*/true);

    /// close attempts flush once more (which fails again), then still runs deinit
    EXPECT_EQ(getRecordedHookCalls(), "['flush_failed', 'flush_failed', 'deinit']");

    clearRecordedHookCalls();
}

TEST_F(CPythonTest, moduleSessionFlushErrorOnCloseStillRunsDeinit)
{
    RestoreMainThreadGIL restore_gil;

    auto session = PythonModuleSession::create(
        "TestFlushErrorOnClose",
        PythonFunction{
            .init_function_name = "init_hooks",
            .init_parameters = {},
            .deinit_function_name = "deinit_hooks",
            .flush_function_name = "failing_flush",
            .entry_function_name = "write_row",
            .source_code = hooks_source,
        });

    auto module_name = session->getModuleName();

    EXPECT_THROW(session->close(/*ignore_exceptions=*/false), DB::Exception);

    /// flush failed but deinit must still have run and the module must be unloaded
    EXPECT_EQ(getRecordedHookCalls(), "['flush_failed', 'deinit']");
    {
        GILGuard gil_guard(/*use_need_cleanup=*/true);
        EXPECT_FALSE(hasModule(module_name));
    }

    clearRecordedHookCalls();
}

#endif
