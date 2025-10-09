#pragma once

#include <base/types.h>

namespace DB
{

namespace ClickHouse
{

class ITypeNameProvider
{
public:
    virtual ~ITypeNameProvider() = default;

    /// Returns the column name and type name pairs.
    virtual const std::unordered_map<String, String> & getColumnTypeNames() const = 0;
};

using TypeNameProviderPtr = std::shared_ptr<const ITypeNameProvider>;

}

}
