#pragma once

#include <boost/core/noncopyable.hpp>
#include "Storages/ExternalTable/IExternalTable.h"
#include "Storages/ExternalTable/ExternalTableSettings.h"

namespace DB
{

/// Allows to create an IExternalTable by the name of they type.
class ExternalTableFactory final : private boost::noncopyable
{
public:
    static ExternalTableFactory & instance();

    using Creator = std::function<IExternalTablePtr(const String & /*name*/, ExternalTableSettingsPtr /*settings*/)>;

    IExternalTablePtr getExternalTable(const String & name, ExternalTableSettingsPtr settings) const;
    void registerExternalTable(const String & type, Creator creator);

private:
    ExternalTableFactory();

    std::unordered_map<std::string, Creator> creators;
};

}
