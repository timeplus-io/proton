#pragma once

#include <Interpreters/StorageID.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <boost/core/noncopyable.hpp>


namespace DB
{

/// Allows to create an IExternalTable by the name of they type.
class ExternalTableFactory final : private boost::noncopyable
{
public:
    static ExternalTableFactory & instance();

    using Creator = std::function<StoragePtr(
        const StorageID & table_id,
        const StorageInMemoryMetadata & storage_metadata,
        const ASTCreateQuery & create_query,
        std::unique_ptr<ExternalTableSettings> settings_,
        bool attach,
        ContextPtr context_)>;

    [[nodiscard]] StoragePtr getExternalTable(const StorageFactory::Arguments &) const;
    void registerExternalTable(const String & type, Creator creator);

private:
    ExternalTableFactory();

    std::unordered_map<std::string, Creator> creators;
};

}
