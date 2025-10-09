#pragma once

#include <string>

namespace cluster::protocol
{
enum UDFType : uint8_t
{
    Executable = 0,
    Remote = 1,
    Javascript = 2,
    Python = 3,
    SQL = 4
};

std::string udf(UDFType type);
UDFType udf(const std::string & type);
}
