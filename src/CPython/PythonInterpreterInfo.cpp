#include "config.h"

#include <CPython/PythonInterpreterInfo.h>

#if USE_PYTHON_UDF
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Common/ShellCommand.h>

#include <boost/algorithm/string.hpp>

#include <filesystem>
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
    if (!std::filesystem::exists(path))
        return false;

    auto resolved_path = std::filesystem::is_symlink(path) ? std::filesystem::read_symlink(path) : path;

    return std::filesystem::is_regular_file(resolved_path) && ::access(resolved_path.c_str(), X_OK) == 0;
}

std::optional<std::string> findExternalPythonInterpreter(const std::string & program_base_dir)
{
    std::vector<std::string> candidate_names = {"python3.10", "python3", "python"};

    std::vector<std::string> candidate_dirs = {
        "/usr/share/proton/bin",
        program_base_dir,
    };

    std::string path_env = getenv("PATH");
    std::vector<std::string> path_dirs;
    boost::split(path_dirs, path_env, boost::is_any_of(":"));

    candidate_dirs.insert(candidate_dirs.end(), path_dirs.begin(), path_dirs.end());

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
import json

try:
    if sys.version_info.major == 3 and sys.version_info.minor == 10:
        site_packages = site.getsitepackages()
        print(json.dumps({
            "prefix": sys.prefix,
            "site-packages": site_packages
        }))
    else:
        sys.stderr.write("Python version is not 3.10")
except Error as e:
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
