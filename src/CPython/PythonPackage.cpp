#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/PythonPackage.h>
#include <CPython/Utils.h>

#include <Common/getExecutablePath.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>

#include <Python.h>
#include <fmt/ranges.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <string_view>


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
PythonInterpreterInfo getUDFPythonInterpreterInfo()
{
    std::string program_dir = getExecutablePath();
    size_t last_slash = program_dir.find_last_of('/');
    if (last_slash != std::string::npos)
        program_dir = program_dir.substr(0, last_slash);

    return PythonInterpreterInfo::collect(program_dir);
}

std::vector<std::string> buildPipArgs(const std::vector<std::string> & base_args, bool for_install_like_ops = true)
{
    std::vector<std::string> args = base_args;
    if (for_install_like_ops)
        args.push_back("--user");

    return args;
}

void appendPipIndexOptions(std::vector<std::string> & args, const PipInstallOptions & install_options)
{
    if (!install_options.index_url.empty())
    {
        args.push_back("--index-url");
        args.push_back(install_options.index_url);
    }

    for (const auto & extra_index_url : install_options.extra_index_urls)
    {
        args.push_back("--extra-index-url");
        args.push_back(extra_index_url);
    }
}

std::string trimCopy(const std::string & value)
{
    const auto begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos)
        return "";

    const auto end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

std::string stripPackageExtras(const std::string & package_name)
{
    const auto extras_start = package_name.find('[');
    if (extras_start == std::string::npos)
        return package_name;

    return trimCopy(package_name.substr(0, extras_start));
}

std::string extractLookupNameFromPackageSpec(const std::string & package_spec)
{
    static constexpr std::array<std::string_view, 8> operators{
        "===",
        "==",
        ">=",
        "<=",
        "!=",
        "~=",
        ">",
        "<",
    };

    size_t version_start = std::string::npos;
    for (std::string_view op : operators)
    {
        const size_t pos = package_spec.find(op);
        if (pos != std::string::npos)
            version_start = std::min(version_start, pos);
    }

    return stripPackageExtras(trimCopy(package_spec.substr(0, version_start)));
}

std::string canonicalizePythonPackageName(std::string_view package_name)
{
    std::string canonical_name;
    canonical_name.reserve(package_name.size());

    bool previous_was_separator = false;
    for (unsigned char ch : package_name)
    {
        const char lower = static_cast<char>(std::tolower(ch));
        const bool is_separator = lower == '-' || lower == '_' || lower == '.';
        if (is_separator)
        {
            if (!previous_was_separator)
                canonical_name.push_back('-');

            previous_was_separator = true;
            continue;
        }

        canonical_name.push_back(lower);
        previous_was_separator = false;
    }

    return canonical_name;
}

void addRefreshSiteDirectory(
    std::vector<std::string> & directories, std::unordered_set<std::string> & seen, const std::string & site_directory)
{
    if (!site_directory.empty() && seen.emplace(site_directory).second)
        directories.push_back(site_directory);
}

std::vector<std::string> getRefreshSiteDirectories(PyObject * site_module)
{
    std::vector<std::string> directories;
    std::unordered_set<std::string> seen;

    /// Pull the cached interpreter info up front so the user-site derivation
    /// uses the same suffix the install side (programs/python.cpp) writes
    /// under. Deriving it inline from Py_GIL_DISABLED + PY_MAJOR/MINOR_VERSION
    /// works for the default layout but silently diverges for custom layouts
    /// (e.g. a Python built with `--with-platlibdir=lib64`), leaving
    /// freshly-installed packages invisible to refreshImportState.
    auto interpreter_info = getUDFPythonInterpreterInfo();

    if (const char * user_base = std::getenv("PYTHONUSERBASE"); user_base && *user_base
        && !interpreter_info.user_site_suffix.empty())
    {
        const auto user_site_packages = fmt::format(
            "{}/{}", user_base, interpreter_info.user_site_suffix);
        addRefreshSiteDirectory(directories, seen, user_site_packages);
    }

    auto get_user_site_packages = PyObjectPtr{PyObject_GetAttrString(site_module, "getusersitepackages")};
    if (!get_user_site_packages || !PyCallable_Check(get_user_site_packages.get()))
        throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to get site.getusersitepackages: {}", getExceptionMessage());

    auto user_site_packages = PyObjectPtr{PyObject_CallFunctionObjArgs(get_user_site_packages.get(), nullptr)};
    if (!user_site_packages)
        throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to get Python user site-packages path: {}", getExceptionMessage());

    if (user_site_packages.get() != Py_None)
    {
        const char * user_site_packages_str = PyUnicode_AsUTF8(user_site_packages.get());
        if (!user_site_packages_str)
            throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to decode Python user site-packages path: {}", getExceptionMessage());

        addRefreshSiteDirectory(directories, seen, user_site_packages_str);
    }

    for (const auto & site_package : interpreter_info.site_packages)
        addRefreshSiteDirectory(directories, seen, site_package);

    return directories;
}

void logConfluentKafkaCompatibilityWarning(const std::string & package_spec, LoggerPtr logger)
{
    const auto lookup_name = canonicalizePythonPackageName(extractLookupNameFromPackageSpec(package_spec));
    if (lookup_name != "confluent-kafka")
        return;

    LOG_WARNING(
        logger,
        "Installing Python package '{}': the confluent-kafka wheel bundles its own librdkafka "
        "which may conflict with the librdkafka already linked into the Proton process. "
        "If you experience import errors or segfaults, try pinning confluent-kafka==2.1.1 "
        "(known-good version) and restart Proton after installation. "
        "You can verify the active library version at runtime via confluent_kafka.libversion().",
        package_spec);
}

struct TemporaryRequirementsFile
{
    explicit TemporaryRequirementsFile(const std::vector<std::string> & requirement_lines)
    {
        std::error_code ec;
        std::filesystem::create_directories("tmp", ec);
        if (ec)
            throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Failed to create tmp directory for requirements file: {}", ec.message());

        static std::atomic<uint64_t> counter = 0;
        file_path = std::filesystem::path("tmp")
            / fmt::format(
                        "python_requirements_{}_{}.txt",
                        std::chrono::steady_clock::now().time_since_epoch().count(),
                        counter.fetch_add(1, std::memory_order_relaxed));
        file_path_string = file_path.string();

        std::ofstream out(file_path, std::ios::out | std::ios::trunc);
        if (!out.is_open())
            throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Failed to create temporary requirements file '{}'", file_path_string);

        for (const auto & requirement_line : requirement_lines)
            out << requirement_line << '\n';

        out.flush();
        if (!out.good())
            throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Failed to write temporary requirements file '{}'", file_path_string);
    }

    ~TemporaryRequirementsFile()
    {
        std::error_code ec;
        if (!file_path.empty())
            std::filesystem::remove(file_path, ec);
    }

    const std::string & path() const { return file_path_string; }

private:
    std::filesystem::path file_path;
    std::string file_path_string;
};

void runPipCommand(const std::vector<std::string> & args, bool log_failure_as_error, LoggerPtr logger)
{
    std::string args_str = fmt::format(" {}", fmt::join(args, " "));
    PythonInterpreterInfo interpreter_info = getUDFPythonInterpreterInfo();

    try
    {
        GILGuard gil_guard(true);

        auto subprocess_module = PyObjectPtr{PyImport_ImportModule("subprocess")};
        if (!subprocess_module)
            throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to import subprocess module");

        auto subprocess_run = PyObjectPtr{PyObject_GetAttrString(subprocess_module.get(), "run")};
        if (!subprocess_run || !PyCallable_Check(subprocess_run.get()))
            throw Exception(ErrorCodes::PYTHON_EXECUTE_ERROR, "Failed to get subprocess.run function");

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

        auto run_args = PyObjectPtr{PyTuple_New(1)};
        PyTuple_SetItem(run_args.get(), 0, cmd_list.release());

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
            std::string stderr_msg;
            if (PyErr_Occurred())
            {
                PyObject * ptype = nullptr;
                PyObject * pvalue = nullptr;
                PyObject * ptraceback = nullptr;
                PyErr_Fetch(&ptype, &pvalue, &ptraceback);

                /// Check if it's a CalledProcessError and extract stderr
                if (pvalue && PyObject_HasAttrStringWithError(pvalue, "stderr") > 0)
                {
                    auto stderr_obj = PyObjectPtr{PyObject_GetAttrString(pvalue, "stderr")};
                    if (stderr_obj && PyUnicode_Check(stderr_obj.get()))
                    {
                        const char * stderr_str = PyUnicode_AsUTF8(stderr_obj.get());
                        if (stderr_str)
                            stderr_msg = stderr_str;
                    }
                }

                Py_XDECREF(ptype);
                Py_XDECREF(pvalue);
                Py_XDECREF(ptraceback);
            }

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

        auto returncode_attr = PyObjectPtr{PyObject_GetAttrString(result.get(), "returncode")};
        long returncode = PyLong_AsLong(returncode_attr.get());

        if (returncode != 0)
        {
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

/// Collect legacy namespace-package names from installed distribution metadata.
/// Reads `namespace_packages.txt` from each Distribution via
/// importlib.metadata — the same file setuptools writes when a project uses
/// `namespace_packages=...` or `declare_namespace()`. Works under
/// setuptools >= 81 where pkg_resources is gone. Silently skips
/// distributions whose metadata is absent or malformed; returns an empty set
/// rather than throwing so package install/uninstall refresh never fails on
/// metadata hiccups. Results are deduplicated across distributions.
/// GIL is assumed held by caller.
std::set<std::string> collectLegacyNamespacesFromDistributions(LoggerPtr logger)
{
    std::set<std::string> names;

    auto md_module = PyObjectPtr{PyImport_ImportModule("importlib.metadata")};
    if (!md_module)
    {
        PyErr_Clear();
        return names;
    }

    auto distributions_func = PyObjectPtr{PyObject_GetAttrString(md_module.get(), "distributions")};
    if (!distributions_func || !PyCallable_Check(distributions_func.get()))
    {
        PyErr_Clear();
        return names;
    }

    auto distributions = PyObjectPtr{PyObject_CallNoArgs(distributions_func.get())};
    if (!distributions)
    {
        PyErr_Clear();
        return names;
    }

    auto iterator = PyObjectPtr{PyObject_GetIter(distributions.get())};
    if (!iterator)
    {
        PyErr_Clear();
        return names;
    }

    auto ns_filename = PyObjectPtr{PyUnicode_FromString("namespace_packages.txt")};
    if (!ns_filename)
    {
        PyErr_Clear();
        return names;
    }

    size_t malformed_entries = 0;
    while (auto dist = PyObjectPtr{PyIter_Next(iterator.get())})
    {
        auto read_text = PyObjectPtr{PyObject_GetAttrString(dist.get(), "read_text")};
        if (!read_text || !PyCallable_Check(read_text.get()))
        {
            PyErr_Clear();
            continue;
        }

        auto text = PyObjectPtr{PyObject_CallFunctionObjArgs(read_text.get(), ns_filename.get(), nullptr)};
        if (!text)
        {
            /// read_text() raises on malformed/encoded metadata — tolerate.
            PyErr_Clear();
            ++malformed_entries;
            continue;
        }

        if (text.get() == Py_None)
            continue;

        if (!PyUnicode_Check(text.get()))
        {
            ++malformed_entries;
            continue;
        }

        Py_ssize_t size = 0;
        const char * utf8 = PyUnicode_AsUTF8AndSize(text.get(), &size);
        if (!utf8)
        {
            PyErr_Clear();
            ++malformed_entries;
            continue;
        }

        std::istringstream stream(std::string(utf8, static_cast<size_t>(size)));
        std::string line;
        while (std::getline(stream, line))
        {
            const auto trimmed = trimCopy(line);
            if (trimmed.empty() || trimmed[0] == '#')
                continue;
            names.insert(trimmed);
        }
    }

    if (PyErr_Occurred())
        PyErr_Clear();

    if (malformed_entries > 0 && logger)
        LOG_WARNING(logger,
            "Skipped {} distribution(s) with unreadable namespace_packages.txt during import refresh",
            malformed_entries);

    return names;
}

/// Legacy fallback: read pkg_resources._namespace_packages. Only used when
/// the distribution-metadata scan returns nothing, which can happen on older
/// envs that stamp namespace packages via declare_namespace() without a
/// corresponding namespace_packages.txt in the .dist-info. Returns an empty
/// set if pkg_resources is not importable (setuptools >= 81 removed it).
/// GIL is assumed held by caller.
std::set<std::string> collectLegacyNamespacesFromPkgResources()
{
    std::set<std::string> names;

    auto pkg_resources_module = PyObjectPtr{PyImport_ImportModule("pkg_resources")};
    if (!pkg_resources_module)
    {
        PyErr_Clear();
        return names;
    }

    auto ns_packages_dict = PyObjectPtr{PyObject_GetAttrString(pkg_resources_module.get(), "_namespace_packages")};
    if (!ns_packages_dict || !PyDict_Check(ns_packages_dict.get()))
    {
        PyErr_Clear();
        return names;
    }

    PyObject * key = nullptr;
    PyObject * value = nullptr;
    Py_ssize_t pos = 0;
    while (PyDict_Next(ns_packages_dict.get(), &pos, &key, &value))
    {
        if (!key || !PyUnicode_Check(key))
            continue;
        const char * utf8 = PyUnicode_AsUTF8(key);
        if (!utf8)
        {
            PyErr_Clear();
            continue;
        }
        names.emplace(utf8);
    }

    if (PyErr_Occurred())
        PyErr_Clear();

    return names;
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

PythonPackage::PythonPackage(const std::string & package_with_version_, PipInstallOptions install_options_)
    : PythonPackage(
          package_with_version_,
          /*requirements_text_*/ "",
          /*install_from_requirements_*/ false,
          std::move(install_options_))
{
}

PythonPackage::PythonPackage(
    const std::string & package_with_version_,
    const std::string & requirements_text_,
    bool install_from_requirements_,
    PipInstallOptions install_options_)
    : package_with_version(package_with_version_)
    , requirements_text(requirements_text_)
    , install_from_requirements(install_from_requirements_)
    , install_options(std::move(install_options_))
    , logger(getLogger("PythonPackage"))
{
    if (!install_options.index_url.empty())
    {
        validatePipIndexUrl(install_options.index_url, "--index-url");
        install_options.index_url = trimCopy(install_options.index_url);
    }

    for (auto & extra_index_url : install_options.extra_index_urls)
    {
        validatePipIndexUrl(extra_index_url, "--extra-index-url");
        extra_index_url = trimCopy(extra_index_url);
    }

    if (install_from_requirements)
    {
        parseRequirementsText(requirements_text);
    }
    else
    {
        validatePackageSpecification(package_with_version);
        parsePackageWithVersion();
    }
}

PythonPackage PythonPackage::fromRequirementsText(const std::string & requirements_text_, PipInstallOptions install_options_)
{
    return PythonPackage(
        /*package_with_version_*/ "",
        requirements_text_,
        /*install_from_requirements_*/ true,
        std::move(install_options_));
}

void PythonPackage::validatePipIndexUrl(const std::string & index_url, const char * option_name)
{
    const auto trimmed = trimCopy(index_url);
    if (trimmed.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty value is not allowed for {}", option_name);

    if (trimmed.size() > 2048)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} value exceeds maximum length of 2048 characters", option_name);

    if (trimmed.find_first_of(" \t\r\n") != std::string::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} value cannot contain whitespace", option_name);

    static const re2::RE2 valid_index_url_pattern(R"(^(https?://)[^\s]+$)");
    if (!re2::RE2::FullMatch(trimmed, valid_index_url_pattern))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid {} value '{}'. Only http(s) URLs are supported", option_name, index_url);
    }
}

void PythonPackage::refreshImportState(LoggerPtr logger)
{
    if (!Py_IsInitialized())
        return;

    if (!logger)
        logger = getLogger("PythonPackage");

    /// Free-threading (cp314t) limitation: this function mutates process-global
    /// import state (site.addsitedir widens sys.path, the namespace loop below
    /// does a non-atomic read-modify-write of each package __path__, and
    /// importlib.invalidate_caches runs). GILGuard(true) only attaches a thread
    /// state under Py_GIL_DISABLED — it does NOT serialize against UDF worker
    /// threads importing/executing Python concurrently. It runs on the
    /// AsyncPythonPackageManager thread, so a `SYSTEM INSTALL PYTHON PACKAGE`
    /// issued while UDFs are actively importing can observe torn import state
    /// (sporadic ImportError for the freshly-installed package). Per-object
    /// C-API atomicity rules out memory corruption. Hot-installing packages
    /// during active UDF execution is therefore not guaranteed safe on
    /// free-threaded builds; a proper fix would quiesce UDF execution across
    /// the install. Tracked as a follow-up.
    GILGuard gil_guard(true);

    auto site_module = PyObjectPtr{PyImport_ImportModule("site")};
    if (!site_module)
        throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to import site module: {}", getExceptionMessage());

    auto add_site_dir = PyObjectPtr{PyObject_GetAttrString(site_module.get(), "addsitedir")};
    if (!add_site_dir || !PyCallable_Check(add_site_dir.get()))
        throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to get site.addsitedir: {}", getExceptionMessage());

    size_t refreshed_paths = 0;
    for (const auto & site_directory : getRefreshSiteDirectories(site_module.get()))
    {
        std::error_code ec;
        if (!std::filesystem::exists(site_directory, ec))
        {
            if (ec)
                LOG_WARNING(logger, "Skipping Python site directory '{}' during refresh: {}", site_directory, ec.message());
            continue;
        }

        auto site_dir_arg = PyObjectPtr{PyUnicode_FromString(site_directory.c_str())};
        auto add_result = PyObjectPtr{PyObject_CallFunctionObjArgs(add_site_dir.get(), site_dir_arg.get(), nullptr)};
        if (!add_result)
        {
            throw Exception(
                ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to refresh Python site directory '{}': {}", site_directory, getExceptionMessage());
        }

        ++refreshed_paths;
    }

    auto importlib_module = PyObjectPtr{PyImport_ImportModule("importlib")};
    if (!importlib_module)
        throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to import importlib module: {}", getExceptionMessage());

    auto invalidate_caches = PyObjectPtr{PyObject_GetAttrString(importlib_module.get(), "invalidate_caches")};
    if (!invalidate_caches || !PyCallable_Check(invalidate_caches.get()))
        throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to get importlib.invalidate_caches: {}", getExceptionMessage());

    auto invalidate_result = PyObjectPtr{PyObject_CallFunctionObjArgs(invalidate_caches.get(), nullptr)};
    if (!invalidate_result)
        throw Exception(ErrorCodes::PYTHON_IMPORT_ERROR, "Failed to invalidate Python import caches: {}", getExceptionMessage());

    /// Extend __path__ only on packages explicitly registered as legacy
    /// namespace packages.  This is the targeted fix for the "google"
    /// namespace-package problem: google-auth installs google/__init__.py that
    /// calls declare_namespace('google'), which freezes google.__path__ at
    /// import time.  A later install of protobuf adds google/protobuf/ under
    /// a different site-packages dir but it stays invisible until __path__ is
    /// widened.
    ///
    /// Preferred source is distribution metadata (importlib.metadata +
    /// namespace_packages.txt), which works under setuptools >= 81 where
    /// pkg_resources is gone.  Fall back to pkg_resources._namespace_packages
    /// only if the metadata scan yields nothing — covers old envs where
    /// declare_namespace() was called but no dist-info is present.
    ///
    /// We intentionally do NOT extend every loaded package — that would widen
    /// ordinary packages that never opted into namespace behavior, which is a
    /// broader import-semantics change than we need here.
    auto namespace_names = collectLegacyNamespacesFromDistributions(logger);
    const char * namespace_source = "importlib.metadata";
    if (namespace_names.empty())
    {
        auto fallback = collectLegacyNamespacesFromPkgResources();
        if (!fallback.empty())
        {
            namespace_names = std::move(fallback);
            namespace_source = "pkg_resources";
        }
        else
        {
            namespace_source = "none";
        }
    }

    size_t extended_namespaces = 0;
    if (!namespace_names.empty())
    {
        auto pkgutil_module = PyObjectPtr{PyImport_ImportModule("pkgutil")};
        if (pkgutil_module)
        {
            auto extend_path_func = PyObjectPtr{PyObject_GetAttrString(pkgutil_module.get(), "extend_path")};
            if (extend_path_func && PyCallable_Check(extend_path_func.get()))
            {
                PyObject * modules_dict = PyImport_GetModuleDict(); /// borrowed ref
                for (const auto & name : namespace_names)
                {
                    auto name_obj = PyObjectPtr{PyUnicode_FromString(name.c_str())};
                    if (!name_obj)
                    {
                        PyErr_Clear();
                        continue;
                    }

                    PyObject * mod_obj = PyDict_GetItem(modules_dict, name_obj.get()); /// borrowed ref
                    if (!mod_obj || mod_obj == Py_None)
                        continue;

                    const int has_path = PyObject_HasAttrStringWithError(mod_obj, "__path__");
                    if (has_path <= 0)
                    {
                        /// -1 sets an exception (e.g. a raising __getattribute__);
                        /// clear it here so it does not leak into the next
                        /// iteration's C-API calls — the loop-tail PyErr_Clear()
                        /// is skipped by this continue.
                        if (has_path < 0)
                            PyErr_Clear();
                        continue;
                    }

                    auto mod_path = PyObjectPtr{PyObject_GetAttrString(mod_obj, "__path__")};
                    if (!mod_path)
                    {
                        PyErr_Clear();
                        continue;
                    }

                    auto new_path
                        = PyObjectPtr{PyObject_CallFunctionObjArgs(extend_path_func.get(), mod_path.get(), name_obj.get(), nullptr)};
                    if (new_path)
                    {
                        if (PyObject_SetAttrString(mod_obj, "__path__", new_path.get()) == 0)
                            ++extended_namespaces;
                    }

                    if (PyErr_Occurred())
                        PyErr_Clear();
                }
            }
        }
    }
    if (PyErr_Occurred())
        PyErr_Clear();

    LOG_DEBUG(
        logger,
        "Refreshed Python import state after package change: refreshed_paths={}, extended_namespaces={}, source={}",
        refreshed_paths,
        extended_namespaces,
        namespace_source);
}

bool PythonPackage::hasEffectiveRequirementLines(const std::string & requirements_text)
{
    std::istringstream input(requirements_text);
    std::string line;
    while (std::getline(input, line))
    {
        auto trimmed = trimCopy(line);
        if (!trimmed.empty() && trimmed[0] != '#')
            return true;
    }
    return false;
}

std::vector<std::string> PythonPackage::parseRequirementsText(const std::string & requirements_text_value)
{
    if (trimCopy(requirements_text_value).empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Requirements text cannot be empty");

    constexpr size_t max_requirements_lines = 1024;
    std::vector<std::string> requirements;
    requirements.reserve(32);

    std::istringstream input(requirements_text_value);
    std::string line;
    while (std::getline(input, line))
    {
        auto trimmed = trimCopy(line);
        if (trimmed.empty() || trimmed[0] == '#')
            continue;

        if (trimmed.starts_with("-"))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Requirements line '{}' is not supported. Please pass index options via API fields instead",
                trimmed);
        }

        validatePackageSpecification(trimmed);
        requirements.push_back(trimmed);

        if (requirements.size() > max_requirements_lines)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Requirements text exceeds maximum number of {} effective lines", max_requirements_lines);
        }
    }

    if (requirements.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Requirements text does not contain any valid package specification");

    return requirements;
}

void PythonPackage::validatePackageSpecification(const std::string & package_spec)
{
    if (package_spec.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Package specification cannot be empty");

    if (package_spec.length() > 255)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Package specification '{}' exceeds maximum length of 255 characters", package_spec);

    static const re2::RE2 valid_package_pattern(
        R"(^([a-zA-Z0-9]([a-zA-Z0-9\-_\.]*[a-zA-Z0-9])?)(\[\s*[a-zA-Z0-9]([a-zA-Z0-9\-_\.]*[a-zA-Z0-9])?(\s*,\s*[a-zA-Z0-9]([a-zA-Z0-9\-_\.]*[a-zA-Z0-9])?)*\s*\])?(\s*[><=!~]+\s*[a-zA-Z0-9\.\-_]+(\s*,\s*[><=!~]+\s*[a-zA-Z0-9\.\-_]+)*)?$)");

    if (!re2::RE2::FullMatch(package_spec, valid_package_pattern))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid package specification '{}'. Must follow Python package naming conventions (PEP 508)",
            package_spec);
    }

    /// Security check: prevent known malicious patterns
    static constexpr std::array<std::string_view, 9> blocked_patterns = {
        "__pycache__", "..", "/", "\\", ":", "*", "?", "\"", "|"
    };
    for (const auto & pattern : blocked_patterns)
    {
        if (package_spec.find(pattern) != std::string::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Package specification '{}' contains forbidden pattern '{}'", package_spec, pattern);
    }
}

void PythonPackage::parsePackageWithVersion()
{
    if (package_with_version.empty())
        throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Package specification cannot be empty");

    size_t version_start = std::string::npos;
    static constexpr std::array<std::string_view, 8> operators = {
        "===", "==", ">=", "<=", "!=", "~=", ">", "<"
    };

    for (const auto & op : operators)
    {
        size_t pos = package_with_version.find(op);
        if (pos != std::string::npos && (version_start == std::string::npos || pos < version_start))
            version_start = pos;
    }

    if (version_start == std::string::npos)
    {
        name = package_with_version;
        version_spec = "";
    }
    else
    {
        name = package_with_version.substr(0, version_start);
        version_spec = package_with_version.substr(version_start);
    }

    const size_t name_start = name.find_first_not_of(" \t");
    const size_t name_end = name.find_last_not_of(" \t");
    if (name_start != std::string::npos && name_end != std::string::npos)
        name = name.substr(name_start, name_end - name_start + 1);

    lookup_name = stripPackageExtras(name);
    if (lookup_name.empty())
        throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Package name cannot be empty in specification: {}", package_with_version);
}

void PythonPackage::validateInstall(LoggerPtr logger) const
{
    if (install_from_requirements)
    {
        auto requirements = parseRequirementsText(requirements_text);
        TemporaryRequirementsFile requirements_file(requirements);

        auto dry_run_args = buildPipArgs({"install", "--dry-run", "--quiet"});
        appendPipIndexOptions(dry_run_args, install_options);
        dry_run_args.push_back("-r");
        dry_run_args.push_back(requirements_file.path());

        try
        {
            runPipCommand(
                dry_run_args,
                /*log_failure_as_error=*/false,
                logger);
        }
        catch (const Exception & e)
        {
            LOG_DEBUG(logger, "Requirements validation failed: {}", e.message());
            throw Exception::createRuntime(e.code(), fmt::format("Failed to validate requirements installation: {}", e.message()));
        }

        return;
    }

    auto show_args = buildPipArgs({"show", "--quiet"});
    show_args.push_back(lookup_name);

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
        try
        {
            auto dry_run_args = buildPipArgs({"install", "--dry-run", "--quiet"});
            appendPipIndexOptions(dry_run_args, install_options);
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
                    "3. Version is compatible with Python {}.{}\n"
                    "Common versions: use 'latest' or check PyPI for available versions",
                    package_with_version,
                    name,
                    PY_MAJOR_VERSION,
                    PY_MINOR_VERSION);
            }
            else if (enhanced_message.find("Requires-Python") != std::string::npos)
            {
                enhanced_message = fmt::format(
                    "Package '{}' requires a different Python version. "
                    "Current Python: {}.{}. Please choose a compatible version or use an older version of the package.",
                    package_with_version,
                    PY_MAJOR_VERSION,
                    PY_MINOR_VERSION);
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
    std::string installed_version = getActualInstalledVersion(logger);
    if (installed_version.empty())
        throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Package '{}' is not installed and cannot be uninstalled", name);

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

    auto dist_iter = PyObjectPtr{PyObject_CallObject(distributions.get(), nullptr)};
    if (!dist_iter)
        throw Exception(ErrorCodes::PYTHON_EXECUTE_ERROR, "Failed to get distributions iterator, {}", getExceptionMessage());

    PythonPackages packages;
    for (auto dist = PyObjectPtr{PyIter_Next(dist_iter.get())}; dist; dist.reset(PyIter_Next(dist_iter.get())))
    {
        auto name_obj = PyObjectPtr{PyObject_GetAttrString(dist.get(), "name")};
        auto version_obj = PyObjectPtr{PyObject_GetAttrString(dist.get(), "version")};

        if (!name_obj || !version_obj)
            continue;

        if (!PyUnicode_Check(name_obj.get()) || !PyUnicode_Check(version_obj.get()))
        {
            PyErr_Clear();
            continue;
        }

        const char * package_name = PyUnicode_AsUTF8(name_obj.get());
        const char * package_version = PyUnicode_AsUTF8(version_obj.get());
        if (!package_name || !package_version)
        {
            PyErr_Clear();
            continue;
        }

        std::string package_with_version = std::string{package_name} + "==" + std::string{package_version};
        packages.emplace_back(package_with_version);
    }

    return packages;
}

PythonInterpreterInfo PythonPackage::getPythonInterpreterInfo()
{
    return getUDFPythonInterpreterInfo();
}

void PythonPackage::install(LoggerPtr logger) const
{
    if (install_from_requirements)
    {
        LOG_INFO(logger, "Starting Python requirements installation");

        validateInstall(logger);

        auto requirements = parseRequirementsText(requirements_text);
        TemporaryRequirementsFile requirements_file(requirements);

        auto args = buildPipArgs({"install"});
        appendPipIndexOptions(args, install_options);
        args.push_back("-r");
        args.push_back(requirements_file.path());

        runPipCommand(
            args,
            /*log_failure_as_error=*/true,
            logger);

        try
        {
            refreshImportState(logger);
        }
        catch (const Exception & e)
        {
            LOG_WARNING(logger, "Installed Python requirements but failed to refresh runtime imports: {}", e.message());
        }

        for (const auto & requirement : requirements)
            logConfluentKafkaCompatibilityWarning(requirement, logger);

        LOG_INFO(logger, "Successfully installed Python requirements");
        return;
    }

    LOG_INFO(logger, "Starting Python package installation: {}", package_with_version);

    validateInstall(logger);

    auto args = buildPipArgs({"install"});
    appendPipIndexOptions(args, install_options);
    args.push_back(package_with_version);

    runPipCommand(
        args,
        /*log_failure_as_error=*/true,
        logger);

    try
    {
        refreshImportState(logger);
    }
    catch (const Exception & e)
    {
        LOG_WARNING(logger, "Installed Python package '{}' but failed to refresh runtime imports: {}", package_with_version, e.message());
    }

    logConfluentKafkaCompatibilityWarning(package_with_version, logger);

    LOG_INFO(logger, "Successfully installed Python package: {}", package_with_version);
}

void PythonPackage::uninstall(LoggerPtr logger) const
{
    if (lookup_name.empty())
        throw Exception(ErrorCodes::PYTHON_PIP_ERROR, "Failed to uninstall package: name is empty");

    LOG_INFO(logger, "Starting Python package uninstallation: {}", name);

    try
    {
        auto args = buildPipArgs({"uninstall", "-y"}, /*for_install_like_ops=*/false);
        args.push_back(lookup_name);

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
    if (lookup_name.empty())
    {
        LOG_DEBUG(logger, "Cannot get version for empty package name");
        return "";
    }

    try
    {
        GILGuard gil_guard(true);

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

        auto cmd_list = PyObjectPtr{PyList_New(5)};
        auto python_arg = PyObjectPtr{PyUnicode_FromString(interpreter_info.path.c_str())};
        auto m_arg = PyObjectPtr{PyUnicode_FromString("-m")};
        auto pip_arg = PyObjectPtr{PyUnicode_FromString("pip")};
        auto show_arg = PyObjectPtr{PyUnicode_FromString("show")};
        auto package_arg = PyObjectPtr{PyUnicode_FromString(lookup_name.c_str())};

        PyList_SetItem(cmd_list.get(), 0, python_arg.release());
        PyList_SetItem(cmd_list.get(), 1, m_arg.release());
        PyList_SetItem(cmd_list.get(), 2, pip_arg.release());
        PyList_SetItem(cmd_list.get(), 3, show_arg.release());
        PyList_SetItem(cmd_list.get(), 4, package_arg.release());

        auto run_args = PyObjectPtr{PyTuple_New(1)};
        PyTuple_SetItem(run_args.get(), 0, cmd_list.release());

        auto kwargs = PyObjectPtr{PyDict_New()};
        auto capture_key = PyObjectPtr{PyUnicode_FromString("capture_output")};
        auto capture_value = PyObjectPtr{PyBool_FromLong(1)};
        PyDict_SetItem(kwargs.get(), capture_key.get(), capture_value.get());

        auto text_key = PyObjectPtr{PyUnicode_FromString("text")};
        auto text_value = PyObjectPtr{PyBool_FromLong(1)};
        PyDict_SetItem(kwargs.get(), text_key.get(), text_value.get());

        auto check_key = PyObjectPtr{PyUnicode_FromString("check")};
        auto check_value = PyObjectPtr{PyBool_FromLong(0)};
        PyDict_SetItem(kwargs.get(), check_key.get(), check_value.get());

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
            LOG_DEBUG(logger, "Failed to execute pip show command for package '{}'", lookup_name);
            PyErr_Clear();
            return "";
        }

        auto returncode_attr = PyObjectPtr{PyObject_GetAttrString(result.get(), "returncode")};
        long returncode = PyLong_AsLong(returncode_attr.get());

        if (returncode != 0)
        {
            LOG_DEBUG(logger, "Package '{}' is not installed (pip show returned {})", lookup_name, returncode);
            return "";
        }

        auto stdout_attr = PyObjectPtr{PyObject_GetAttrString(result.get(), "stdout")};
        const char * stdout_str = PyUnicode_AsUTF8(stdout_attr.get());
        if (!stdout_str)
        {
            LOG_DEBUG(logger, "Failed to get stdout from pip show for package '{}'", lookup_name);
            return "";
        }

        std::string output(stdout_str);

        static const re2::RE2 version_pattern(R"((?m)^Version:\s*([^\r\n]+))");
        std::string version;
        if (re2::RE2::PartialMatch(output, version_pattern, &version))
        {
            size_t start = version.find_first_not_of(" \t\r\n");
            size_t end = version.find_last_not_of(" \t\r\n");
            if (start != std::string::npos && end != std::string::npos)
            {
                version = version.substr(start, end - start + 1);
                LOG_DEBUG(logger, "Extracted version '{}' for package '{}'", version, lookup_name);
                return version;
            }
        }

        LOG_DEBUG(logger, "Could not parse version from pip show output for package '{}'", lookup_name);
        return "";
    }
    catch (const Exception & e)
    {
        LOG_DEBUG(logger, "Exception while getting version for package '{}': {}", lookup_name, e.message());
        return "";
    }
    catch (...)
    {
        LOG_DEBUG(logger, "Unknown exception while getting version for package '{}'", lookup_name);
        return "";
    }
}
}
}
