#include "config_tools.h"

#if ENABLE_PROTON_PYTHON

#include <CPython/PythonInterpreterInfo.h>
#include <Core/Defines.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>

#include <Python.h>
#include <boost/algorithm/string.hpp>
#include <fmt/format.h>

#include <iostream>
#include <cstdlib>
#include <filesystem>
#include <optional>

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_INTERNAL_ERROR;
}
}

namespace
{
std::pair<std::string, std::vector<char *>> extractConfigFileOption(int argc, char ** argv)
{
    std::vector<char *> new_argv;
    std::string config_file_path;
    for (int i = 0; i < argc; ++i)
    {
        if (std::string(argv[i]) == "--config-file")
        {
            ++i;
            config_file_path = argv[i];
            continue;
        }

        new_argv.push_back(argv[i]);
    }

    return {config_file_path, new_argv};
}

DB::ConfigurationPtr getConfig(const std::string & config_file_path)
{
    DB::ConfigProcessor config_processor(config_file_path);
    return config_processor.loadConfig().configuration;
}

std::string getDefaultPath(const DB::ConfigurationPtr & config)
{
    if (!config)
        return DBMS_DEFAULT_PATH;

    std::string default_path = config->getString("path", DBMS_DEFAULT_PATH);
    std::error_code ec;
    std::filesystem::path canonical_path = std::filesystem::canonical(default_path, ec);
    if (ec)
        return DBMS_DEFAULT_PATH;

    return canonical_path;
}

std::string loadPythonDirFromConfig(const DB::ConfigurationPtr & config)
{
    auto python_path = config->getString("python_path", "");
    if (!python_path.empty())
        return python_path;

    return fmt::format("{}/python", getDefaultPath(config));
}

std::string prepareUserSitePackages(const std::string & python_dir, const std::string & user_site_suffix)
{
    std::string user_site_packages_dir = fmt::format("{}/{}", python_dir, user_site_suffix);
    if (!std::filesystem::exists(user_site_packages_dir))
    {
        auto status = std::filesystem::create_directories(user_site_packages_dir);
        if (!status)
        {
            throw DB::Exception(
                DB::ErrorCodes::UDF_INTERNAL_ERROR, "Failed to create directory for python interpreter: {}", user_site_packages_dir);
        }
    }

    return user_site_packages_dir;
}

std::filesystem::path getApplicationPath(const std::string & command)
{
    if (command.find('/') != std::string::npos)
    {
        std::filesystem::path path(command);
        if (path.is_absolute())
            return path;
        else
            return std::filesystem::current_path() / path;
    }
    else
    {
        std::string path_str = getenv("PATH");
        std::vector<std::string> path_dirs;
        boost::split(path_dirs, path_str, boost::is_any_of(":"));

        for (const auto & dir : path_dirs)
        {
            std::filesystem::path path(dir);
            path /= command;
            if (std::filesystem::exists(path))
                return path;
        }

        return std::filesystem::current_path() / command;
    }
}

std::string getApplicationDir(const std::string & command)
{
    return getApplicationPath(command).parent_path().string();
}

void checkPythonPathExists(const std::string & python_path)
{
    std::vector<std::string> python_lib_paths;
    boost::split(python_lib_paths, python_path, boost::is_any_of(":"), boost::token_compress_on);
    bool any_path_exists = std::ranges::any_of(python_lib_paths, [](const auto & lib_path) { return std::filesystem::exists(lib_path); });

    if (!any_path_exists)
    {
        throw DB::Exception(
            DB::ErrorCodes::UDF_INTERNAL_ERROR,
            "Skip Embedded Python interpreter initialization: no Python standard library found in PYTHONPATH");
    }
}

bool preparePythonEnv(const std::string & config_file_path, const std::string & app_dir)
{
    try
    {
        auto config = getConfig(config_file_path);
        auto python_dir = loadPythonDirFromConfig(config);

        if (python_dir.ends_with('/'))
            python_dir.pop_back();

        auto [interpreter_info, error_message] = DB::cpython::PythonInterpreterInfo::tryCollect(app_dir);
        if (!interpreter_info.has_value())
        {
            std::cerr << error_message << std::endl;
            return false;
        }

        auto user_site_packages_dir = prepareUserSitePackages(python_dir, interpreter_info->user_site_suffix);

        auto python_home = interpreter_info->prefix;
        auto python_path = boost::join(interpreter_info->site_packages, ":");

        /// PYTHONPATH is a colon-separated list of directories that will be added to sys.path.
        /// It is used to locate the standard library in python
        const char * path = getenv("PYTHONPATH");
        if (path != nullptr)
            python_path = fmt::format("{}:{}", path, python_path);

        python_path = fmt::format("{}:{}", user_site_packages_dir, python_path);

        checkPythonPathExists(python_path);

        setenv("PYTHONPATH", python_path.c_str(), 1);
        setenv("PYTHONHOME", python_home.c_str(), 1);
        setenv("PYTHONUSERBASE", python_dir.c_str(), 1);

        wchar_t * program = Py_DecodeLocale(interpreter_info->path.c_str(), NULL);
        Py_SetProgramName(program);
    }
    catch (const std::exception & e)
    {
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}
}

int mainPython(int argc, char ** argv)
{
    auto [config_file_path, new_argv] = extractConfigFileOption(argc, argv);
    auto app_dir = getApplicationDir(new_argv[0]);

    if (!preparePythonEnv(config_file_path, app_dir))
    {
        std::cerr << "Failed to prepare Python running environment. Please specify the --config-file option." << std::endl;
        return -1;
    }

    int new_argc = static_cast<int>(new_argv.size());
    char ** new_argv_ptr = new_argv.data();
    return Py_BytesMain(new_argc, new_argv_ptr);
}

#else

int mainPython(int argc, char ** argv)
{
    std::cerr << "Python module is not enabled" << std::endl;
    return -1;
}

#endif
