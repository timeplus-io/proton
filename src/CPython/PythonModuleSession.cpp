#include <CPython/PythonModuleSession.h>

#include <CPython/GILGuard.h>
#include <CPython/Utils.h>
#include <Common/Exception.h>
#include <Access/LocalApiToken.h>
#include <base/types.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_RUNNING_ERROR;
}
}

namespace DB::cpython
{
namespace
{
void cleanupPythonModule(
    const String & module_name,
    const String & flush_function_name,
    const String & deinit_function_name,
    bool run_hooks,
    bool & flush_attempted,
    bool & deinit_attempted)
{
    if (module_name.empty())
        return;

    std::exception_ptr cleanup_exception;

    /// Flush any data still buffered by the Python code before running deinit,
    /// so a graceful close does not lose buffered rows.
    if (run_hooks && !flush_function_name.empty() && !flush_attempted)
    {
        flush_attempted = true;
        try
        {
            executeFunction(flush_function_name, module_name);
        }
        catch (...)
        {
            cleanup_exception = std::current_exception();
        }
    }

    if (run_hooks && !deinit_function_name.empty() && !deinit_attempted)
    {
        deinit_attempted = true;
        try
        {
            executeFunction(deinit_function_name, module_name);
        }
        catch (...)
        {
            if (!cleanup_exception)
                cleanup_exception = std::current_exception();
        }
    }

    if (hasModule(module_name))
    {
        try
        {
            unloadModule(module_name);
        }
        catch (...)
        {
            if (!cleanup_exception)
                cleanup_exception = std::current_exception();
        }
    }

    if (cleanup_exception)
        std::rethrow_exception(cleanup_exception);
}
}

PythonModuleSessionPtr PythonModuleSession::create(String module_prefix_, PythonFunction function_)
{
    auto session = std::make_shared<PythonModuleSession>(std::move(module_prefix_), std::move(function_));
    session->init();
    return session;
}

PythonModuleSession::PythonModuleSession(String module_prefix_, PythonFunction function_)
    : module_prefix(std::move(module_prefix_)), function(std::move(function_))
{
}

PythonModuleSession::~PythonModuleSession()
{
    close(/*ignore_exceptions=*/true);
}

void PythonModuleSession::closeSession(PythonModuleSessionPtr & session, bool ignore_exceptions, bool acquire_gil)
{
    if (!session)
        return;

    if (Py_IsInitialized() == 0)
    {
        session.reset();
        return;
    }

    try
    {
        session->close(ignore_exceptions, acquire_gil);
        session.reset();
    }
    catch (...)
    {
        if (!ignore_exceptions)
            throw;
    }
}

void PythonModuleSession::init()
{
    if (Py_IsInitialized() == 0)
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Python Interpreter is not initialized, please check the python_path configuration");

    module_name = module_prefix + randomModuleName();

    GILGuard gil_guard;
    try
    {
        auto byte_code = compile(function.source_code);
        executeByteCode(byte_code, module_name);
        module_loaded = true;

        /// Inject local API credentials directly into the module's global namespace
        /// so UDF code can access them as bare names without going through os.environ.
        /// This keeps the token out of the process environment (no subprocess leakage,
        /// no /proc/<pid>/environ exposure).
        if (LocalApiToken::isEnabled())
        {
            auto mod = getModule(module_name);
            PyObjectPtr user_val(PyUnicode_FromString(LocalApiToken::username().c_str()));
            PyObjectPtr pass_val(PyUnicode_FromString(LocalApiToken::token().c_str()));
            PyObject_SetAttrString(mod.get(), "__timeplus_local_api_user", user_val.get());
            PyObject_SetAttrString(mod.get(), "__timeplus_local_api_password", pass_val.get());
        }

        if (!function.init_function_name.empty())
        {
            Strings args;
            if (!function.init_parameters.empty())
                args.emplace_back(function.init_parameters);
            auto py_args = createArgumentsTuple(args);
            executeFunction(function.init_function_name, module_name, py_args);
        }

        py_function = cpython::getFunction(function.entry_function_name, module_name);
    }
    catch (...)
    {
        auto original_exception = std::current_exception();
        try
        {
            py_function.reset();
            closeImpl();
        }
        catch (...)
        {
        }
        std::rethrow_exception(original_exception);
    }
}

PyObjectPtr PythonModuleSession::execute(const PyObjectPtr & args) const
{
    chassert(PyGILState_Check());
    return executeObject(py_function, args);
}

void PythonModuleSession::flush(bool acquire_gil) const
{
    /// module_name is cleared by closeImpl(), so this also no-ops after close.
    if (function.flush_function_name.empty() || module_name.empty())
        return;

    if (acquire_gil)
    {
        GILGuard gil_guard;
        executeFunction(function.flush_function_name, module_name);
    }
    else
    {
        executeFunction(function.flush_function_name, module_name);
    }
}

void PythonModuleSession::close(bool ignore_exceptions, bool acquire_gil)
{
    /// std::call_once guarantees exactly-once execution even when called
    /// concurrently from an explicit close() and the destructor.
    std::call_once(close_once, [&] {
        if (Py_IsInitialized() == 0)
            return;

        auto runClose = [&] {
            if (acquire_gil)
            {
                GILGuard gil_guard;
                closeImpl();
            }
            else
            {
                closeImpl();
            }
        };

        if (ignore_exceptions)
        {
            try
            {
                runClose();
            }
            catch (...)
            {
            }
        }
        else
        {
            runClose();
        }
    });
}

void PythonModuleSession::closeImpl()
{
    chassert(PyGILState_Check());
    py_function.reset();

    std::exception_ptr cleanup_exception;
    try
    {
        cleanupPythonModule(
            module_name, function.flush_function_name, function.deinit_function_name, module_loaded, flush_attempted, deinit_attempted);
        module_name.clear();
        module_loaded = false;
    }
    catch (...)
    {
        cleanup_exception = std::current_exception();
        bool module_unloaded = false;
        if (!module_name.empty())
        {
            try
            {
                module_unloaded = !hasModule(module_name);
            }
            catch (...)
            {
            }
        }

        if (module_unloaded)
        {
            module_name.clear();
            module_loaded = false;
        }
    }

    if (cleanup_exception)
        std::rethrow_exception(cleanup_exception);
}
}
