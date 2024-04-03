#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <Common/NamePrompter.h>
#include <Interpreters/Context_fwd.h>
#include <Functions/IFunction.h>

/// proton: starts.
#include <AggregateFunctions/IAggregateFunction.h>
/// proton: ends

namespace DB
{

class UserDefinedFunctionFactory
{
public:
    using Creator = std::function<FunctionOverloadResolverPtr(ContextPtr)>;

    static UserDefinedFunctionFactory & instance();

    static FunctionOverloadResolverPtr get(const String & function_name, ContextPtr context);

    /// proton: starts. For User Defined Aggregation functions
    static AggregateFunctionPtr getAggregateFunction(
        const String & name,
        const DataTypes & types,
        const Array & parameters,
        AggregateFunctionProperties & properties,
        /// whether input of aggregation function is changelog, aggregate function does not pass _tp_delta column to UDA if it is false
        bool is_changelog_input = false);

    static bool isAggregateFunctionName(const String & function_name);

    static bool isOrdinaryFunctionName(const String & function_name);

    bool registerFunction(
        ContextPtr context,
        const String & function_name,
        Poco::JSON::Object::Ptr json_func,
        bool throw_if_exists,
        bool replace_if_exists);
    
    void validateJavaScriptFunction(Poco::JSON::Object::Ptr config);
    
    void validatePythonFunction(Poco::JSON::Object::Ptr config);

    bool unregisterFunction(const ContextMutablePtr & context, const String & function_name, bool throw_if_not_exists);
    /// proton: ends

    static FunctionOverloadResolverPtr tryGet(const String & function_name, ContextPtr context);

    static bool has(const String & function_name, ContextPtr context);

    static std::vector<String> getRegisteredNames(ContextPtr context);
};

}
