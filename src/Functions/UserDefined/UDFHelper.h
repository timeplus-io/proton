#pragma once

#include "config.h"

#include <Cluster/Requests/CreateUserDefinedFunctionRequest.h>
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
using ASTPtr = std::shared_ptr<IAST>;

class UserDefinedExecutableFunction;
using UserDefinedExecutableFunctionPtr = std::shared_ptr<const UserDefinedExecutableFunction>;

namespace Streaming
{

/// Valid UDF function name is [a-zA-Z0-9_] and has to start with _ or [a-zA-Z]
void validateUDFName(const std::string & func_name);

/// Make CreateUserDefinedFunctionRequest from JSON configuration
cluster::CreateUserDefinedFunctionRequestPtr createUserDefinedFunctionRequest(
    ContextPtr context,
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    cluster::protocol::ExistsOperation exists_op,
    cluster::protocol::UserDefinedFunctionDescriptorPtr original_desc);

#if USE_PYTHON_UDF
/// Resolve init parameters and run the configured init hook for a freshly
/// loaded Python UDF module. No-op when the payload has no init function.
/// `udf_name` is used in error messages. Caller must hold the GIL.
void runPythonUDFInitHook(
    const cluster::protocol::PythonUserDefinedFunctionPayload & payload, const String & udf_name, const String & module_name);
#endif

/// Check if the query AST has user defined function
bool hasUserDefinedFunction(const ASTPtr & query, ContextPtr context);
}
}
