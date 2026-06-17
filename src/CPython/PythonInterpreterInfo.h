#pragma once

#include <optional>
#include <string>
#include <vector>

namespace DB::cpython
{
struct PythonInterpreterInfo
{
    struct Result;
    std::string path;
    std::string prefix;
    std::vector<std::string> site_packages;
    int version_major = 0;
    int version_minor = 0;
    bool is_free_threaded = false;
    /// Relative path from a user base directory to user site-packages,
    /// e.g. "lib/python3.14/site-packages". Derived from the interpreter's
    /// own sysconfig/site layout so it is correct for free-threaded builds.
    std::string user_site_suffix;

    /// Returns "X.Y" version string, e.g. "3.14"
    std::string versionString() const;

    static PythonInterpreterInfo collect(const std::string & program_full_path);
    static Result tryCollect(const std::string & program_full_path);
};

struct PythonInterpreterInfo::Result
{
    std::optional<PythonInterpreterInfo> interpreter_info;
    std::string error_message;
};
}
