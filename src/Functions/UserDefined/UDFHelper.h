#pragma once

#include <Interpreters/Context_fwd.h>

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{

class IAST;
class UserDefinedExecutableFunction;
using UserDefinedExecutableFunctionPtr = std::shared_ptr<const UserDefinedExecutableFunction>;

namespace Streaming
{

/// Valid UDF function name is [a-zA-Z0-9_] and has to start with _ or [a-zA-Z]
void validateUDFName(const std::string & func_name);

/// Create UserDefinedExecutableFunction from JSON configuration
UserDefinedExecutableFunctionPtr
createUserDefinedExecutableFunction(ContextPtr context, const std::string & name, const Poco::Util::AbstractConfiguration & config);
}
}
