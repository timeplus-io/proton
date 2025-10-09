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

    static PythonInterpreterInfo collect(const std::string & program_full_path);
    static Result tryCollect(const std::string & program_full_path);
};

struct PythonInterpreterInfo::Result
{
    std::optional<PythonInterpreterInfo> interpreter_info;
    std::string error_message;
};
}
