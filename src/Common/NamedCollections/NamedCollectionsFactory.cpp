#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <base/sleep.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
    extern const int NAMED_COLLECTION_IS_IMMUTABLE;
    extern const int LOGICAL_ERROR;
}

NamedCollectionFactory & NamedCollectionFactory::instance()
{
    static NamedCollectionFactory instance;
    return instance;
}

NamedCollectionFactory::~NamedCollectionFactory()
{
    shutdown();
}

void NamedCollectionFactory::shutdown()
{
    metadata_storage.reset();
}

bool NamedCollectionFactory::exists(const std::string & collection_name) const
{
    /// proton: starts
    try
    {
        return get(collection_name) != nullptr;
    }
    catch (const Exception & ex)
    {
        if (ex.code() == ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST)
            return false;
        throw;
    }
    /// proton: ends
}

NamedCollectionPtr NamedCollectionFactory::get(const std::string & collection_name) const
{
    /// proton: starts
    return metadata_storage->get(collection_name);
    /// proton: ends
}

NamedCollectionPtr NamedCollectionFactory::tryGet(const std::string & collection_name) const
{
    /// proton: starts
    try
    {
        return get(collection_name);
    }
    catch (const Exception & ex)
    {
        if (ex.code() != ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST)
            LOG_ERROR(log, "Failed to get named collection: name={} error={}", collection_name, ex.displayText());
        return nullptr;
    }
    /// proton: ends
}

NamedCollectionsMap NamedCollectionFactory::getAll() const
{
    /// proton: starts
    return metadata_storage->getAll();
    /// proton: ends
}

void NamedCollectionFactory::reloadFromConfig(const Poco::Util::AbstractConfiguration &)
{
    /// proton: starts
    /// Load from configuration is not implemented.
    /// proton: ends
}

void NamedCollectionFactory::createFromSQL(const ASTCreateNamedCollectionQuery & query)
{
    /// proton: starts
    metadata_storage->create(query);
    /// proton: ends
}

void NamedCollectionFactory::removeFromSQL(const ASTDropNamedCollectionQuery & query)
{
    /// proton: starts
    if (query.if_exists)
        metadata_storage->removeIfExists(query.collection_name);
    else
        metadata_storage->remove(query.collection_name);
    /// proton: ends
}

void NamedCollectionFactory::updateFromSQL(const ASTAlterNamedCollectionQuery & query)
{
    /// proton: starts
    auto collection_name = query.collection_name;
    if (!exists(collection_name))
    {
        if (query.if_exists)
            return;

        throw Exception(
            ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
            "Cannot update collection `{}`, because it doesn't exist",
            collection_name);
    }
    metadata_storage->update(query);
    /// proton: ends
}

void NamedCollectionFactory::reloadFromSQL()
{
    /// proton: starts
    /// Do nothing as named collections are not cached
    /// proton: ends
}

bool NamedCollectionFactory::usesReplicatedStorage()
{
    /// proton: starts
    return false;
    /// proton: ends
}

void NamedCollectionFactory::loadIfNot()
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
}

bool NamedCollectionFactory::loadIfNot([[maybe_unused]] std::lock_guard<std::mutex> & lock)
{
    if (loaded)
        return false;

    auto context = Context::getGlobalContextInstance();
    metadata_storage = NamedCollectionsMetadataStorage::create(context);

    loaded = true;
    return true;
}

}
