#include "config.h"

#include <CPython/PythonInterpreterInfo.h>

#if USE_PYTHON_UDF
#include <patchlevel.h>
/// pyconfig.h carries Py_GIL_DISABLED; patchlevel.h alone does not pull it in.
#include <pyconfig.h>

#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Common/ShellCommand.h>

#include <boost/algorithm/string.hpp>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <string_view>
#include <unistd.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_INTERNAL_ERROR;
}
}

namespace DB::cpython
{
namespace
{
bool isPathAccessible(const std::filesystem::path & path)
{
    std::error_code ec;
    auto resolved_path = std::filesystem::weakly_canonical(path, ec);
    if (ec)
        return false;

    return std::filesystem::is_regular_file(resolved_path, ec) && !ec && ::access(resolved_path.c_str(), X_OK) == 0;
}

std::optional<std::string> findExternalPythonInterpreter(const std::string & program_base_dir)
{
    /// Prefer versioned binaries matching the embedded CPython ABI first.
    /// The "t" variant is the free-threaded build (CPython 3.13+) and must
    /// only be selected when the embedded interpreter is itself free-threaded
    /// (Py_GIL_DISABLED). Picking it for a GIL-enabled embedded build would
    /// derive PYTHONHOME / site-packages from the wrong ABI and install
    /// extensions that cannot be loaded.
#define CPYTHON_STRINGIFY_IMPL(x) #x
#define CPYTHON_STRINGIFY(x) CPYTHON_STRINGIFY_IMPL(x)
#define CPYTHON_VER_STR "python" CPYTHON_STRINGIFY(PY_MAJOR_VERSION) "." CPYTHON_STRINGIFY(PY_MINOR_VERSION)
#ifdef Py_GIL_DISABLED
    static constexpr std::array<std::string_view, 4> candidate_names = {
        CPYTHON_VER_STR "t",
        CPYTHON_VER_STR,
        "python3",
        "python",
    };
#else
    static constexpr std::array<std::string_view, 3> candidate_names = {
        CPYTHON_VER_STR,
        "python3",
        "python",
    };
#endif
#undef CPYTHON_VER_STR
#undef CPYTHON_STRINGIFY
#undef CPYTHON_STRINGIFY_IMPL

    std::vector<std::string> candidate_dirs;
    candidate_dirs.reserve(8);
    candidate_dirs.emplace_back("/usr/share/proton/bin");
    candidate_dirs.emplace_back(program_base_dir);

    /// getenv may return nullptr when PATH is unset (containers, stripped
    /// environments). std::string(nullptr) is UB — guard explicitly.
    if (const char * path_env = std::getenv("PATH"))
    {
        std::vector<std::string> path_dirs;
        boost::split(path_dirs, path_env, boost::is_any_of(":"));
        candidate_dirs.insert(
            candidate_dirs.end(),
            std::make_move_iterator(path_dirs.begin()),
            std::make_move_iterator(path_dirs.end()));
    }

    for (const auto & dir : candidate_dirs)
    {
        for (const auto & name : candidate_names)
        {
            std::filesystem::path path{fmt::format("{}/{}", dir, name)};
            if (isPathAccessible(path))
                return path;
        }
    }

    return {};
}
}

PythonInterpreterInfo PythonInterpreterInfo::collect(const std::string & program_base_dir)
{
    auto python_interpreter_path = findExternalPythonInterpreter(program_base_dir);
    if (!python_interpreter_path.has_value())
        throw DB::Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Cannot find python interpreter");

    auto command = python_interpreter_path.value() + R"""( <<'EOF'
import sys
import sysconfig
import site
import os
import json

try:
    info = {
        "prefix": sys.prefix,
        "site-packages": site.getsitepackages(),
        "version_major": sys.version_info.major,
        "version_minor": sys.version_info.minor,
    }
    # Detect the build ABI (free-threaded / cp314t), NOT the runtime GIL mode:
    # a cp314t interpreter run with PYTHON_GIL=1 still has the free-threaded ABI
    # and site-packages layout, so the ABI guard must match on the build flag.
    # Py_GIL_DISABLED is 1 for free-threaded builds, 0/None otherwise.
    info["free_threaded"] = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

    # Derive the user site-packages suffix relative to user base.
    # This is correct for both regular and free-threaded builds.
    user_base = site.getuserbase()
    user_site = site.getusersitepackages()
    if user_base and user_site and user_site.startswith(user_base):
        info["user_site_suffix"] = user_site[len(user_base):].lstrip(os.sep)
    else:
        info["user_site_suffix"] = "lib/python{}.{}/site-packages".format(
            sys.version_info.major, sys.version_info.minor)

    print(json.dumps(info))
except Exception as e:
    sys.stderr.write(str(e))
EOF
)""";
    WriteBufferFromOwnString out;
    WriteBufferFromOwnString err;

    try
    {
        auto res = ShellCommand::execute(command);

        copyData(res->out, out);
        copyData(res->err, err);

        res->wait();
    }
    catch (const std::exception & e)
    {
        std::cerr << e.what() << '\n';
    }

    auto err_msg = err.str();
    if (!err_msg.empty())
        throw DB::Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Cannot detect system python interpreter: {}", err_msg);

    PythonInterpreterInfo res;
    res.path = python_interpreter_path.value();

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(out.str());
    auto object = result.extract<Poco::JSON::Object::Ptr>();

    res.prefix = object->optValue<std::string>("prefix", "");
    if (res.prefix.empty())
        throw DB::Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Cannot detect system python interpreter: empty prefix");

    res.version_major = object->optValue<int>("version_major", 0);
    res.version_minor = object->optValue<int>("version_minor", 0);
    res.is_free_threaded = object->optValue<bool>("free_threaded", false);
    res.user_site_suffix = object->optValue<std::string>("user_site_suffix", "");

    if (res.version_major != PY_MAJOR_VERSION || res.version_minor != PY_MINOR_VERSION)
        throw DB::Exception(
            ErrorCodes::UDF_INTERNAL_ERROR,
            "System Python interpreter version {}.{} does not match embedded CPython {}.{}. "
            "Please install python{}.{}",
            res.version_major,
            res.version_minor,
            PY_MAJOR_VERSION,
            PY_MINOR_VERSION,
            PY_MAJOR_VERSION,
            PY_MINOR_VERSION);

    /// C-extension wheels for cp314 and cp314t live in separate site-packages
    /// layouts and are ABI-incompatible. The external interpreter's ABI must
    /// match the embedded build so site-packages discovery surfaces the right
    /// set and pip installs land where Py_Initialize() will find them.
#ifdef Py_GIL_DISABLED
    if (!res.is_free_threaded)
        throw DB::Exception(
            ErrorCodes::UDF_INTERNAL_ERROR,
            "Embedded CPython is free-threaded (cp{}{}t) but external interpreter at {} "
            "has the GIL enabled. Install python{}.{}t and place it on PATH.",
            PY_MAJOR_VERSION,
            PY_MINOR_VERSION,
            res.path,
            PY_MAJOR_VERSION,
            PY_MINOR_VERSION);
#else
    if (res.is_free_threaded)
        throw DB::Exception(
            ErrorCodes::UDF_INTERNAL_ERROR,
            "Embedded CPython is GIL-enabled (cp{}{}) but external interpreter at {} "
            "is free-threaded. Install python{}.{} and place it on PATH.",
            PY_MAJOR_VERSION,
            PY_MINOR_VERSION,
            res.path,
            PY_MAJOR_VERSION,
            PY_MINOR_VERSION);
#endif

    auto site_packages = object->getArray("site-packages");
    if (!site_packages)
        return res;

    for (size_t i = 0; i < site_packages->size(); ++i)
        res.site_packages.push_back(site_packages->getElement<std::string>(i));

    return res;
}

PythonInterpreterInfo::Result PythonInterpreterInfo::tryCollect(const std::string & program_base_dir)
{
    try
    {
        return {collect(program_base_dir), ""};
    }
    catch (const std::exception & e)
    {
        return {{}, e.what()};
    }
}
}

#else

namespace DB::cpython
{
PythonInterpreterInfo PythonInterpreterInfo::collect(const std::string &)
{
    throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF is not supported");
}

PythonInterpreterInfo::Result PythonInterpreterInfo::tryCollect(const std::string & program_base_dir)
{
    return {{}, "The USE_PYTHON compilation option is disabled, Python UDF is not supported."};
}
}

#endif

namespace DB::cpython
{
std::string PythonInterpreterInfo::versionString() const
{
    return std::to_string(version_major) + "." + std::to_string(version_minor);
}
}
