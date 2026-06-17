#pragma once

#include <CPython/PyObjectPtr.h>

#include <base/types.h>

#include <memory>
#include <mutex>

namespace DB::cpython
{
struct PythonFunction
{
    String init_function_name;
    String init_parameters;
    String deinit_function_name;
    String flush_function_name;
    String entry_function_name;
    String source_code;
};

class PythonModuleSession;
using PythonModuleSessionPtr = std::shared_ptr<PythonModuleSession>;

/// Owns a per-query Python module: compiles source, runs init/deinit hooks,
/// and holds the entry function for repeated execute() calls.
///
/// Thread-safety contract (free-threading ready):
///   - A session is owned by exactly one processor (single-writer).
///   - execute() is called from the owning processor's pipeline thread.
///     It requires the caller to hold a GILGuard.
///   - close() may be called explicitly or from the destructor; guarded
///     by std::once_flag to prevent double execution.
///   - Concurrent execute() + close() is NOT supported.  The engine's
///     function/query registry ensures that close() only runs after all
///     execute() calls have returned.
class PythonModuleSession
{
public:
    PythonModuleSession(String module_prefix_, PythonFunction function_);

    static PythonModuleSessionPtr create(String module_prefix_, PythonFunction function_);

    static void closeSession(PythonModuleSessionPtr & session, bool ignore_exceptions, bool acquire_gil = true);

    ~PythonModuleSession();

    const String & getModuleName() const { return module_name; }

    const PythonFunction & getFunction() const { return function; }

    /// Execute the module's entry function.  Caller MUST hold the Python
    /// runtime scope (GILGuard) — this method does not acquire it internally
    /// so that callers can batch multiple Python calls under one scope.
    PyObjectPtr execute(const PyObjectPtr & args = PyObjectPtr()) const;

    /// Invoke the flush hook (if configured) to let Python code flush buffered data.
    /// No-op when no flush function is configured or the session is already closed.
    void flush(bool acquire_gil = true) const;

    void close(bool ignore_exceptions, bool acquire_gil = true);

private:
    void init();
    void closeImpl();

    String module_prefix;
    PythonFunction function;

    String module_name;
    PyObjectPtr py_function;

    /// Guards closeImpl() against double execution from concurrent
    /// explicit close() + destructor.
    std::once_flag close_once;
    bool module_loaded = false;
    bool flush_attempted = false;
    bool deinit_attempted = false;
};
}
