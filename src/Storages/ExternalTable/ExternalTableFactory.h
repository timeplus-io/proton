#pragma once

#include <boost/core/noncopyable.hpp>
#include "Storages/ExternalTable/ExternalTableImpl.h"
#include "Storages/ExternalTable/ExternalTableSettings.h"

namespace DB
{

/// Allows to create an IExternalTable by the name of they type.
class ExternalTableFactory final : private boost::noncopyable
{
public:
    static ExternalTableFactory & instance();

    using Creator = std::function<IExternalTablePtr(ExternalTableSettingsPtr)>;

    IExternalTablePtr getExternalTable(const std::string & type, ExternalTableSettingsPtr settings) const;
    void registerExternalTable(const std::string & type, Creator creator);

private:
    std::unordered_map<std::string, Creator> creators;
};

}
