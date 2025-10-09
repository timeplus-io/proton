#include <Cluster/Protocol/UDFType.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace cluster::protocol
{

std::string udf(UDFType type)
{
    switch (type)
    {
        case UDFType::Executable:
            return "executable";
        case UDFType::Remote:
            return "remote";
        case UDFType::Javascript:
            return "javascript";
        case UDFType::Python:
            return "python";
        case UDFType::SQL:
            return "sql";
    }
}

UDFType udf(const std::string & type)
{
    if (type == "executable")
        return UDFType::Executable;
    else if (type == "remote")
        return UDFType::Remote;
    else if (type == "javascript")
        return UDFType::Javascript;
    else if (type == "python")
        return UDFType::Python;
    else if (type == "sql")
        return UDFType::SQL;
    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown UDF type: {}", type);
}
}
