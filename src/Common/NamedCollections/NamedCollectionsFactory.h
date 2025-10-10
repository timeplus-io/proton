#pragma once

#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>
#include <Common/logger_useful.h>
/// #include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <boost/noncopyable.hpp>

namespace DB
{
class ASTCreateNamedCollectionQuery;
class ASTDropNamedCollectionQuery;
class ASTAlterNamedCollectionQuery;

/// proton: starts
/// Rewrite NamedCollectionFactory. Do not keep an internal map to cache the named collections
/// for sync it with MetaStore is complex and costly. Instead, every API call is transferred
/// to metadata request and get latest value from MetaStore.
class NamedCollectionFactory : boost::noncopyable
{
public:
    static NamedCollectionFactory & instance();

    ~NamedCollectionFactory();

    bool exists(const std::string & collection_name) const;

    NamedCollectionPtr get(const std::string & collection_name) const;

    NamedCollectionPtr tryGet(const std::string & collection_name) const;

    NamedCollectionsMap getAll() const;

    void reloadFromConfig(const Poco::Util::AbstractConfiguration & config);

    void reloadFromSQL();

    void createFromSQL(const ASTCreateNamedCollectionQuery & query);

    void removeFromSQL(const ASTDropNamedCollectionQuery & query);

    void updateFromSQL(const ASTAlterNamedCollectionQuery & query);

    bool usesReplicatedStorage();

    void loadIfNot();

    void shutdown();

protected:
    mutable std::mutex mutex;

    const LoggerPtr log = getLogger("NamedCollectionFactory");

    bool loaded = false;
    std::unique_ptr<NamedCollectionsMetadataStorage> metadata_storage;

    bool loadIfNot(std::lock_guard<std::mutex> & lock);
};
/// proton: ends

}
