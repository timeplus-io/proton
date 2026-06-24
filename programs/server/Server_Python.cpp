#include "Server.h"

#include <CPython/PythonInterpreterInfo.h>

#if USE_PYTHON_UDF
#include <CPython/GILGuard.h>

#include <Python.h>
#endif

#include <boost/algorithm/string.hpp>

#include <filesystem>

namespace DB
{
void Server::initPythonInterpreter()
{
#if USE_PYTHON_UDF
    /**
     * Set the location of additional libraries, such as numpy and other self installed module, you can check the path through pip, for example `pip show numpy`,
     * it can tell you which location the numpy is installed.
     * You can see these two environment variables definition at https://docs.python.org/3/using/cmdline.html#environment-variables
    */
    std::string default_path = getDefaultPath();
    std::string python_dir = config().getString("python_path", fmt::format("{}/python", default_path));

    if (python_dir.ends_with('/'))
        python_dir.pop_back();

    auto [interpreter_info, error_message] = cpython::PythonInterpreterInfo::tryCollect(config().getString("application.dir"));
    if (!interpreter_info.has_value())
    {
        LOG_INFO(
            &logger(),
            "Skip Embedded Python interpreter initialization: no compatible Python interpreter found. {}",
            error_message);
        return;
    }

    std::string user_site_packages_dir = fmt::format("{}/{}", python_dir, interpreter_info->user_site_suffix);
    if (!std::filesystem::exists(user_site_packages_dir))
    {
        std::error_code error_code;
        auto created = std::filesystem::create_directories(user_site_packages_dir, error_code);
        if (!created || error_code)
        {
            LOG_ERROR(
                &logger(),
                "Failed to create directory for python interpreter: dir='{}', error='{} ({})'",
                user_site_packages_dir,
                error_code.message(),
                error_code.value());
            return;
        }
    }

    std::string python_path = boost::join(interpreter_info->site_packages, ":");

    /// PYTHONPATH is a colon-separated list of directories that will be added to sys.path.
    /// It is used to locate the standard library in python
    const char * path = getenv("PYTHONPATH");
    if (path != nullptr)
        python_path = fmt::format("{}:{}", path, python_path);

    python_path = fmt::format("{}:{}", user_site_packages_dir, python_path);

    setenv("PYTHONPATH", python_path.c_str(), 1);
    setenv("PYTHONHOME", interpreter_info->prefix.c_str(), 1);
    setenv("PYTHONUSERBASE", python_dir.c_str(), 1);

    wchar_t * program = Py_DecodeLocale(interpreter_info->path.c_str(), NULL);
    Py_SetProgramName(program);

    std::vector<std::string> python_lib_paths;
    boost::split(python_lib_paths, python_path, boost::is_any_of(":"), boost::token_compress_on);
    bool any_path_exists = std::ranges::any_of(python_lib_paths, [](const auto & lib_path) { return std::filesystem::exists(lib_path); });

    if (!any_path_exists)
    {
        LOG_INFO(
            &logger(),
            "Skip Embedded Python interpreter initialization: no Python standard library found in PYTHONPATH");
        return;
    }

    constexpr bool embedded_free_threaded = cpython::GILGuard::buildSupportsFreeThreading();
    LOG_INFO(
        &logger(),
        "Embedded Python {} interpreter is initializing: embedded_free_threaded={}, "
        "external_free_threaded={}, python_home={}, python_path={}",
        interpreter_info->versionString(),
        embedded_free_threaded,
        interpreter_info->is_free_threaded,
        interpreter_info->prefix,
        python_path);

    /// initialize python interpreter
    Py_Initialize();
    /// release GIL, since the main thread won't execute python code
    mainstate = PyEval_SaveThread();

    chassert(mainstate);
    chassert(mainstate->interp);

    if (Py_IsInitialized())
        LOG_INFO(
            &logger(),
            "Embedded Python {} interpreter initialized successfully with python_home={} python_path={}",
            interpreter_info->versionString(),
            python_dir,
            python_path);
    else
        LOG_ERROR(
            &logger(),
            "Embedded Python interpreter initialization failed with python_home={} python_path={}",
            python_dir,
            python_path);
#endif
}

void Server::finalizePythonInterpreter()
{
#if USE_PYTHON_UDF
    /// restore GIL, and finalize python interpreter
    if (Py_IsInitialized())
        PyEval_RestoreThread(mainstate);
    Py_Finalize();
#endif
}
}
