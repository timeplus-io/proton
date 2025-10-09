#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/PythonPackage.h>
#include <CPython/Utils.h>

#include <Common/getExecutablePath.h>
#include <Common/logger_useful.h>

#include <Python.h>
#include <re2/re2.h>


namespace DB
{

namespace ErrorCodes
{
extern const int PYTHON_EXECUTE_ERROR;
extern const int PYTHON_IMPORT_ERROR;
extern const int PYTHON_PIP_ERROR;
extern const int BAD_ARGUMENTS;
extern const int RESOURCE_NOT_INITED;
}

namespace cpython
{
namespace
{
/// Get the Python interpreter info that matches the UDF runtime configuration
PythonInterpreterInfo getUDFPythonInterpreterInfo()
{
    /// Use the same discovery logic as the UDF runtime in Server_Python.cpp
    /// This ensures we use the exact same Python interpreter
    std::string program_dir = getExecutablePath();
    size_t last_slash = program_dir.find_last_of('/');
    if (last_slash != std::string::npos)
        program_dir = program_dir.substr(0, last_slash);

    return PythonInterpreterInfo::collect(program_dir);
}

/// Build pip command arguments with consistent user strategy
/// Always uses --user for install-like operations to ensure consistent permissions in containers
std::vector<std::string> buildPipArgs(const std::vector<std::string> & base_args, bool for_install_like_ops = true)
{
    std::vector<std::string> args = base_args;
    if (for_install_like_ops)
        args.push_back("--user");

    return args;
}

void runPipCommand(const std::vector<std::string> & args, bool log_failure_as_error, LoggerPtr logger)
{
    std::string args_str = fmt::format(" {}", fmt::join(args, " "));

    /// Get the correct Python interpreter path that matches UDF runtime
    PythonInterpreterInfo interpreter_info = getUDFPythonInterpreterInfo();

    try
    {
        GILGuard gil_guard(true);

        /// Use subprocess to call 'python -m pip' is best practice
        /// https://stackoverflow.com/a/47059613 and https://github.com/jgonggrijp/pip-review/issues/44
        auto subprocess_module = PyObjectPtr{PyImport_ImportModule("subprocess")};
        if (!subprocess_module)
            throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to import subprocess module");

        auto subprocess_run = PyObjectPtr{PyObject_GetAttrString(subprocess_module.get(), "run")};
        if (!subprocess_run || !PyCallable_Check(subprocess_run.get()))
            throw Exception(ErrorCodes::PYTHON_EXECUTE_ERROR, "Failed to get subprocess.run function");

        /// Build command list: [interpreter_path, "-m", "pip"] + args
        auto cmd_list = PyObjectPtr{PyList_New(args.size() + 3)};
        auto python_arg = PyObjectPtr{PyUnicode_FromString(interpreter_info.path.c_str())};
        auto m_arg = PyObjectPtr{PyUnicode_FromString("-m")};
        auto pip_arg = PyObjectPtr{PyUnicode_FromString("pip")};

        PyList_SetItem(cmd_list.get(), 0, python_arg.release());
        PyList_SetItem(cmd_list.get(), 1, m_arg.release());
        PyList_SetItem(cmd_list.get(), 2, pip_arg.release());

        for (size_t i = 0; i < args.size(); ++i)
        {
            auto arg = PyObjectPtr{PyUnicode_FromString(args[i].c_str())};
            PyList_SetItem(cmd_list.get(), i + 3, arg.release());
        }

        /// Call subprocess.run() with command list
        auto run_args = PyObjectPtr{PyTuple_New(1)};
        PyTuple_SetItem(run_args.get(), 0, cmd_list.release());

        /// Add keyword arguments: check=True, capture_output=True, text=True
        auto kwargs = PyObjectPtr{PyDict_New()};
        auto check_key = PyObjectPtr{PyUnicode_FromString("check")};
        auto check_value = PyObjectPtr{PyBool_FromLong(1)};
        PyDict_SetItem(kwargs.get(), check_key.get(), check_value.get());

        auto capture_key = PyObjectPtr{PyUnicode_FromString("capture_output")};
        auto capture_value = PyObjectPtr{PyBool_FromLong(1)};
        PyDict_SetItem(kwargs.get(), capture_key.get(), capture_value.get());

        auto text_key = PyObjectPtr{PyUnicode_FromString("text")};
        auto text_value = PyObjectPtr{PyBool_FromLong(1)};
        PyDict_SetItem(kwargs.get(), text_key.get(), text_value.get());

        /// Set environment to ensure consistent paths with UDF runtime
        /// This ensures packages are installed to the same location UDF runtime expects
        auto env_key = PyObjectPtr{PyUnicode_FromString("env")};
        auto env_dict = PyObjectPtr{PyDict_New()};

        /// Copy current environment
        auto os_module = PyObjectPtr{PyImport_ImportModule("os")};
        auto environ = PyObjectPtr{PyObject_GetAttrString(os_module.get(), "environ")};
        auto copy_method = PyObjectPtr{PyObject_GetAttrString(environ.get(), "copy")};
        auto current_env = PyObjectPtr{PyObject_CallObject(copy_method.get(), nullptr)};

        /// Ensure PYTHONHOME and PYTHONUSERBASE are set correctly
        auto pythonhome_key = PyObjectPtr{PyUnicode_FromString("PYTHONHOME")};
        auto pythonhome_value = PyObjectPtr{PyUnicode_FromString(interpreter_info.prefix.c_str())};
        PyDict_SetItem(current_env.get(), pythonhome_key.get(), pythonhome_value.get());

        PyDict_SetItem(kwargs.get(), env_key.get(), current_env.get());

        auto result = PyObjectPtr{PyObject_Call(subprocess_run.get(), run_args.get(), kwargs.get())};
        if (!result)
        {
            /// Extract stderr from CalledProcessError before clearing Python exception
            std::string stderr_msg;
            if (PyErr_Occurred())
            {
                PyObject *ptype, *pvalue, *ptraceback;
                PyErr_Fetch(&ptype, &pvalue, &ptraceback);

                /// Check if it's a CalledProcessError and extract stderr
                if (pvalue && PyObject_HasAttrString(pvalue, "stderr"))
                {
                    auto stderr_obj = PyObjectPtr{PyObject_GetAttrString(pvalue, "stderr")};
                    if (stderr_obj && PyUnicode_Check(stderr_obj.get()))
                    {
                        const char * stderr_str = PyUnicode_AsUTF8(stderr_obj.get());
                        if (stderr_str)
                            stderr_msg = stderr_str;
                    }
                }

                /// Clean up the fetched exception objects
                Py_XDECREF(ptype);
                Py_XDECREF(pvalue);
                Py_XDECREF(ptraceback);
            }

            /// Clear any Python exception to avoid GIL issues in destructor
            PyErr_Clear();

            if (log_failure_as_error)
            {
                if (!stderr_msg.empty())
                    LOG_ERROR(logger, "pip command failed: {} -m pip{}, stderr: {}", interpreter_info.path, args_str, stderr_msg);
                else
                    LOG_ERROR(logger, "pip command failed: {} -m pip{}", interpreter_info.path, args_str);
            }

            std::string error_message = fmt::format("pip command failed: {} -m pip{}", interpreter_info.path, args_str);
            if (!stderr_msg.empty())
                error_message += fmt::format(", stderr: {}", stderr_msg);

            throw Exception::createRuntime(ErrorCodes::PYTHON_PIP_ERROR, error_message);
        }

        /// Get return code
        auto returncode_attr = PyObjectPtr{PyObject_GetAttrString(result.get(), "returncode")};
        long returncode = PyLong_AsLong(returncode_attr.get());

        if (returncode != 0)
        {
            /// Get stderr for error message
            auto stderr_attr = PyObjectPtr{PyObject_GetAttrString(result.get(), "stderr")};
            const char * stderr_str = PyUnicode_AsUTF8(stderr_attr.get());
            std::string error_message = stderr_str ? stderr_str : "Unknown error";

            if (log_failure_as_error)
                LOG_ERROR(
                    logger,
                    "pip command failed with return code {}: {} -m pip{}, stderr: {}",
                    returncode,
                    interpreter_info.path,
                    args_str,
                    error_message);

            throw Exception(
                ErrorCodes::PYTHON_PIP_ERROR,
                "pip command failed with return code {}: {} -m pip{}, stderr: {}",
                returncode,
                interpreter_info.path,
                args_str,
                error_message);
        }
    }
    catch (const Exception &)
    {
        if (log_failure_as_error)
            LOG_ERROR(logger, "pip command failed with C++ exception: {} -m pip{}", interpreter_info.path, args_str);
        throw;
    }
    catch (...)
    {
        if (log_failure_as_error)
            LOG_ERROR(logger, "pip command failed with unknown exception: {} -m pip{}", interpreter_info.path, args_str);

        throw Exception(
            ErrorCodes::PYTHON_PIP_ERROR, "pip command failed with unknown exception: {} -m pip{}", interpreter_info.path, args_str);
    }
}
}

void PythonPackage::ensurePythonInterpreterReady()
{
    try
    {
        if (Py_IsInitialized() == 0)
        {
            throw Exception(
                ErrorCodes::RESOURCE_NOT_INITED,
                "Python Interpreter is not initialized, please check the python_path configuration, ensure each path is valid");
        }
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (...)
    {
        throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "Failed to verify Python interpreter status");
    }
}

void PythonPackage::validatePackageSpecification(const std::string & package_spec)
{
    if (package_spec.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Package specification cannot be empty");

    /// Prevent excessively long package specifications
    if (package_spec.length() > 255)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Package specification '{}' exceeds maximum length of 255 characters", package_spec);

    /// Allows package names with version specifiers like "requests>=2.25.0"
    static const re2::RE2 valid_package_pattern(
        R"(^([a-zA-Z0-9]([a-zA-Z0-9\-_\.])*[a-zA-Z0-9]|[a-zA-Z0-9])(\s*[><=!~]+\s*[a-zA-Z0-9\.\-_]+(\s*,\s*[><=!~]+\s*[a-zA-Z0-9\.\-_]+)*)?$)");

    if (!re2::RE2::FullMatch(package_spec, valid_package_pattern))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid package specification '{}'. Must follow Python package naming conventions (PEP 508)",
            package_spec);
    }

    /// Security check: prevent known malicious patterns
    static const std::vector<std::string> blocked_patterns = {"__pycache__", "..", "/", "\\", ":", "*", "?", "\"", "|"};
    for (const auto & pattern : blocked_patterns)
    {
        if (package_spec.find(pattern) != std::string::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Package specification '{}' contains forbidden pattern '{}'", package_spec, pattern);
    }
}

void PythonPackage::parsePackageWithVersion()
{
    /// Parse package_with_version to extract the package name
    /// Examples: "slack_sdk===2.1.0" -> name="slack_sdk", version_spec="===2.1.0"
    ///           "slack_sdk>=2.1.0,<2.3.0" -> name="slack_sdk", version_spec=">=2.1.0,<2.3.0"
    ///           "slack_sdk" -> name="slack_sdk", version_spec=""

    if (package_with_version.empty())
        throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Package specification cannot be empty");

    /// Find the first version operator (===, ==, >=, <=, >, <, !=, ~=)
    size_t version_start = std::string::npos;
    std::vector<std::string> operators = {"===", "==", ">=", "<=", "!=", "~=", ">", "<"};

    for (const auto & op : operators)
    {
        size_t pos = package_with_version.find(op);
        if (pos != std::string::npos && (version_start == std::string::npos || pos < version_start))
            version_start = pos;
    }

    if (version_start == std::string::npos)
    {
        /// No version specified
        name = package_with_version;
        version_spec = "";
    }
    else
    {
        /// Version specified
        name = package_with_version.substr(0, version_start);
        version_spec = package_with_version.substr(version_start);
    }

    /// Trim whitespace from name
    size_t name_start = name.find_first_not_of(" \t");
    size_t name_end = name.find_last_not_of(" \t");
    if (name_start != std::string::npos && name_end != std::string::npos)
        name = name.substr(name_start, name_end - name_start + 1);

    if (name.empty())
        throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Package name cannot be empty in specification: {}", package_with_version);
}

void PythonPackage::validateInstall(LoggerPtr logger) const
{
    /// Use pip show command to check if package is already installed locally
    /// pip show returns exit code 0 if package is installed, non-zero if not installed
    auto show_args = buildPipArgs({"show", "--quiet"});
    show_args.push_back(name);

    try
    {
        runPipCommand(
            show_args,
            /*log_failure_as_error=*/false,
            logger);
        return;
    }
    catch (...)
    {
        /// Package not installed locally, try dry-run install to validate it can actually be installed
        /// This will catch packages that exist in PyPI but are broken/malformed

        try
        {
            auto dry_run_args = buildPipArgs({"install", "--dry-run", "--quiet"});
            dry_run_args.push_back(package_with_version);

            runPipCommand(
                dry_run_args,
                /*log_failure_as_error=*/false,
                logger);
        }
        catch (const Exception & e)
        {
            std::string enhanced_message = e.message();
            if (enhanced_message.find("No matching distribution found") != std::string::npos)
            {
                enhanced_message = fmt::format(
                    "Package '{}' version not found. Please check:\n"
                    "1. Package name spelling is correct\n"
                    "2. Version exists (visit https://pypi.org/project/{}/#history)\n"
                    "3. Version is compatible with Python 3.10\n"
                    "Common versions: use 'latest' or check PyPI for available versions",
                    package_with_version,
                    name);
            }
            else if (enhanced_message.find("Requires-Python") != std::string::npos)
            {
                enhanced_message = fmt::format(
                    "Package '{}' requires a different Python version. "
                    "Current Python: 3.10. Please choose a version compatible with Python 3.10 or use an older version of the package.",
                    package_with_version);
            }
            else if (enhanced_message.find("returned non-zero exit status") != std::string::npos)
            {
                enhanced_message = fmt::format(
                    "Package '{}' validation failed. This could be due to:\n"
                    "1. Package or version doesn't exist\n"
                    "2. Network connectivity issues\n"
                    "3. Package compatibility issues\n"
                    "Please verify the package name and version at https://pypi.org/project/{}/",
                    package_with_version,
                    name);
            }

            LOG_DEBUG(logger, "Package validation failed for '{}': {}", package_with_version, enhanced_message);
            throw Exception::createRuntime(e.code(), enhanced_message);
        }
    }
}

void PythonPackage::validateUninstall(LoggerPtr logger) const
{
    /// For uninstall, we only need to check if the package exists
    /// Use the same robust logic as getActualInstalledVersion()
    std::string installed_version = getActualInstalledVersion(logger);
    if (installed_version.empty())
    {
        throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Package '{}' is not installed and cannot be uninstalled", name);
    }

    LOG_DEBUG(logger, "Package '{}' (version: {}) found and can be uninstalled", name, installed_version);
}

std::vector<PythonPackage> PythonPackage::list()
{
    GILGuard gil_guard(true);

    auto metadata = PyObjectPtr{PyImport_ImportModule("importlib.metadata")};
    if (!metadata)
        throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to import importlib.metadata module");

    auto distributions = PyObjectPtr{PyObject_GetAttrString(metadata.get(), "distributions")};
    if (!distributions || !PyCallable_Check(distributions.get()))
        throw Exception(ErrorCodes::PYTHON_EXECUTE_ERROR, "Failed to get distributions, {}", getExceptionMessage());

    auto dist_iter = PyObjectPtr{PyObject_CallObject(distributions.get(), NULL)};
    if (!dist_iter)
        throw Exception(ErrorCodes::PYTHON_EXECUTE_ERROR, "Failed to get distributions iterator, {}", getExceptionMessage());

    PyObjectPtr dist;
    PythonPackages packages;

    for (auto dist = PyObjectPtr{PyIter_Next(dist_iter.get())}; dist; dist.reset(PyIter_Next(dist_iter.get())))
    {
        auto name = PyObjectPtr{PyObject_GetAttrString(dist.get(), "name")};
        auto version = PyObjectPtr{PyObject_GetAttrString(dist.get(), "version")};

        if (name && version)
        {
            const char * package_name = PyUnicode_AsUTF8(name.get());
            const char * package_version = PyUnicode_AsUTF8(version.get());
            /// For list(), we construct the package_with_version as name==version
            std::string package_with_version = std::string{package_name} + "==" + std::string{package_version};
            packages.emplace_back(package_with_version);
        }
    }

    return packages;
}

PythonInterpreterInfo PythonPackage::getPythonInterpreterInfo()
{
    return getUDFPythonInterpreterInfo();
}

void PythonPackage::install(LoggerPtr logger) const
{
    LOG_INFO(logger, "Starting Python package installation: {}", package_with_version);

    /// Pre-install validation
    validateInstall(logger);

    try
    {
        auto args = buildPipArgs({"install"});
        args.push_back(package_with_version);

        runPipCommand(
            args,
            /*log_failure_as_error=*/true,
            logger);

        LOG_INFO(logger, "Successfully installed Python package: {}", package_with_version);
    }
    catch (...)
    {
        throw;
    }
}

void PythonPackage::uninstall(LoggerPtr logger) const
{
    if (name.empty())
        throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Failed to uninstall package: name is empty");

    LOG_INFO(logger, "Starting Python package uninstallation: {}", name);

    try
    {
        /// pip uninstall automatically finds packages regardless of where they were installed
        /// (user site-packages, target directory, or system site-packages)
        /// It doesn't support --user or --target flags, so we use for_install_like_ops=false
        auto args = buildPipArgs({"uninstall", "-y"}, /*for_install_like_ops=*/false);
        args.push_back(name);

        runPipCommand(
            args,
            /*log_failure_as_error=*/true,
            logger);
        LOG_INFO(logger, "Successfully uninstalled Python package: {}", name);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "Failed to uninstall Python package {}: {}", name, e.message());
        throw;
    }
    catch (...)
    {
        LOG_ERROR(logger, "Failed to uninstall Python package {}: Unknown exception", name);
        throw Exception(ErrorCodes::PYTHON_EXECUTE_ERROR, "Failed to uninstall Python package: {}", name);
    }
}

std::string PythonPackage::getActualInstalledVersion(LoggerPtr logger) const
{
    if (name.empty())
    {
        LOG_DEBUG(logger, "Cannot get version for empty package name");
        return "";
    }

    try
    {
        GILGuard gil_guard(true);

        /// Use subprocess to call 'python -m pip show' to get package information
        PythonInterpreterInfo interpreter_info = getUDFPythonInterpreterInfo();

        auto subprocess_module = PyObjectPtr{PyImport_ImportModule("subprocess")};
        if (!subprocess_module)
        {
            LOG_DEBUG(logger, "Failed to import subprocess module for version extraction");
            return "";
        }

        auto subprocess_run = PyObjectPtr{PyObject_GetAttrString(subprocess_module.get(), "run")};
        if (!subprocess_run || !PyCallable_Check(subprocess_run.get()))
        {
            LOG_DEBUG(logger, "Failed to get subprocess.run function for version extraction");
            return "";
        }

        /// Build command: [interpreter_path, "-m", "pip", "show", package_name]
        auto cmd_list = PyObjectPtr{PyList_New(5)};
        auto python_arg = PyObjectPtr{PyUnicode_FromString(interpreter_info.path.c_str())};
        auto m_arg = PyObjectPtr{PyUnicode_FromString("-m")};
        auto pip_arg = PyObjectPtr{PyUnicode_FromString("pip")};
        auto show_arg = PyObjectPtr{PyUnicode_FromString("show")};
        auto package_arg = PyObjectPtr{PyUnicode_FromString(name.c_str())};

        PyList_SetItem(cmd_list.get(), 0, python_arg.release());
        PyList_SetItem(cmd_list.get(), 1, m_arg.release());
        PyList_SetItem(cmd_list.get(), 2, pip_arg.release());
        PyList_SetItem(cmd_list.get(), 3, show_arg.release());
        PyList_SetItem(cmd_list.get(), 4, package_arg.release());

        /// Call subprocess.run() with command list
        auto run_args = PyObjectPtr{PyTuple_New(1)};
        PyTuple_SetItem(run_args.get(), 0, cmd_list.release());

        /// Add keyword arguments: capture_output=True, text=True, check=False (we handle errors)
        auto kwargs = PyObjectPtr{PyDict_New()};
        auto capture_key = PyObjectPtr{PyUnicode_FromString("capture_output")};
        auto capture_value = PyObjectPtr{PyBool_FromLong(1)};
        PyDict_SetItem(kwargs.get(), capture_key.get(), capture_value.get());

        auto text_key = PyObjectPtr{PyUnicode_FromString("text")};
        auto text_value = PyObjectPtr{PyBool_FromLong(1)};
        PyDict_SetItem(kwargs.get(), text_key.get(), text_value.get());

        auto check_key = PyObjectPtr{PyUnicode_FromString("check")};
        auto check_value = PyObjectPtr{PyBool_FromLong(0)}; /// Don't raise exception on non-zero exit
        PyDict_SetItem(kwargs.get(), check_key.get(), check_value.get());

        /// Set environment to ensure consistent paths
        auto env_key = PyObjectPtr{PyUnicode_FromString("env")};
        auto os_module = PyObjectPtr{PyImport_ImportModule("os")};
        auto environ = PyObjectPtr{PyObject_GetAttrString(os_module.get(), "environ")};
        auto copy_method = PyObjectPtr{PyObject_GetAttrString(environ.get(), "copy")};
        auto current_env = PyObjectPtr{PyObject_CallObject(copy_method.get(), nullptr)};

        auto pythonhome_key = PyObjectPtr{PyUnicode_FromString("PYTHONHOME")};
        auto pythonhome_value = PyObjectPtr{PyUnicode_FromString(interpreter_info.prefix.c_str())};
        PyDict_SetItem(current_env.get(), pythonhome_key.get(), pythonhome_value.get());
        PyDict_SetItem(kwargs.get(), env_key.get(), current_env.get());

        auto result = PyObjectPtr{PyObject_Call(subprocess_run.get(), run_args.get(), kwargs.get())};
        if (!result)
        {
            LOG_DEBUG(logger, "Failed to execute pip show command for package '{}'", name);
            PyErr_Clear();
            return "";
        }

        /// Check return code
        auto returncode_attr = PyObjectPtr{PyObject_GetAttrString(result.get(), "returncode")};
        long returncode = PyLong_AsLong(returncode_attr.get());

        if (returncode != 0)
        {
            LOG_DEBUG(logger, "Package '{}' is not installed (pip show returned {})", name, returncode);
            return "";
        }

        /// Get stdout and parse version
        auto stdout_attr = PyObjectPtr{PyObject_GetAttrString(result.get(), "stdout")};
        const char * stdout_str = PyUnicode_AsUTF8(stdout_attr.get());
        if (!stdout_str)
        {
            LOG_DEBUG(logger, "Failed to get stdout from pip show for package '{}'", name);
            return "";
        }

        std::string output(stdout_str);

        /// Parse the Version line from pip show output
        /// Expected format: "Version: 2.31.0"
        static const re2::RE2 version_pattern(R"((?m)^Version:\s*([^\r\n]+))");
        std::string version;
        if (re2::RE2::PartialMatch(output, version_pattern, &version))
        {
            /// Trim whitespace
            size_t start = version.find_first_not_of(" \t\r\n");
            size_t end = version.find_last_not_of(" \t\r\n");
            if (start != std::string::npos && end != std::string::npos)
            {
                version = version.substr(start, end - start + 1);
                LOG_DEBUG(logger, "Extracted version '{}' for package '{}'", version, name);
                return version;
            }
        }

        LOG_DEBUG(logger, "Could not parse version from pip show output for package '{}'", name);
        return "";
    }
    catch (const Exception & e)
    {
        LOG_DEBUG(logger, "Exception while getting version for package '{}': {}", name, e.message());
        return "";
    }
    catch (...)
    {
        LOG_DEBUG(logger, "Unknown exception while getting version for package '{}'", name);
        return "";
    }
}
}
}
