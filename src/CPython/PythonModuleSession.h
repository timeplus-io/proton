#pragma once

#include <CPython/PyObjectPtr.h>

#include <base/types.h>

#include <memory>

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

class PythonModuleSession
{
public:
    PythonModuleSession(String module_prefix_, PythonFunction function_);

    static PythonModuleSessionPtr create(String module_prefix_, PythonFunction function_);

    static void closeSession(PythonModuleSessionPtr & session, bool ignore_exceptions, bool acquire_gil = true);

    ~PythonModuleSession();

    const String & getModuleName() const { return module_name; }

    const PythonFunction & getFunction() const { return function; }

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
    bool closed = false;
    bool module_loaded = false;
    bool flush_attempted = false;
    bool deinit_attempted = false;
};
}
