#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>

/// proton: starts
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/ExistsOperation.h>
#include <Cluster/Protocol/NamedCollectionDescriptor.h>
#include <Cluster/Requests/CreateNamedCollectionRequest.h>
#include <Cluster/Requests/DeleteNamedCollectionRequest.h>
#include <Cluster/Requests/GetNamedCollectionRequest.h>
#include <Cluster/Requests/ListNamedCollectionsRequest.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

#include <memory>


namespace DB
{
namespace ErrorCodes
{
extern const int NAMED_COLLECTION_ALREADY_EXISTS;
extern const int NAMED_COLLECTION_DOESNT_EXIST;
extern const int INVALID_CONFIG_PARAMETER;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int SUPPORT_IS_DISABLED;
}

NamedCollectionsMetadataStorage::NamedCollectionsMetadataStorage(ContextPtr context_)
    : WithContext(context_)
{
}

MutableNamedCollectionPtr NamedCollectionsMetadataStorage::get(const std::string & collection_name) const
{
    auto desc = readDescriptor(collection_name);
    chassert(desc);
    return NamedCollectionFromDescriptor::create(collection_name, desc);
}

NamedCollectionsMap NamedCollectionsMetadataStorage::getAll() const
{
    NamedCollectionsMap result;
    for (const auto & collection_name : listCollections())
    {
        if (result.contains(collection_name))
        {
            throw Exception(ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS, "Found duplicate named collection `{}`", collection_name);
        }
        result.emplace(collection_name, get(collection_name));
    }
    return result;
}

MutableNamedCollectionPtr NamedCollectionsMetadataStorage::create(const ASTCreateNamedCollectionQuery & create_query)
{
    auto collection_ptr = NamedCollectionFromSQL::create(create_query);
    writeCreateQuery(create_query, false);
    return collection_ptr;
}

cluster::protocol::NamedCollectionDescriptorPtr NamedCollectionsMetadataStorage::readDescriptor(const std::string & collection_name) const
{
    auto & metastore = Globals::getMetaStore();
    int64_t timeout_ms = getContext()->getSettingsRef().query_timeout_sec * 1000;

    auto req = std::make_shared<cluster::GetNamedCollectionRequest>(
        collection_name,
        /*versions_requested=*/1,
        metastore.nodeID(),
        /*consistent_read=*/false,
        timeout_ms,
        /*request_version=*/1);

    auto resp = metastore.getNamedCollection(std::move(req));
    const auto & err = resp->error();

    if (err.isNotFound() || resp->data().named_collections.empty())
        throw Exception(ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST, "Named collection {} not found", collection_name);

    if (err.hasError())
        throw Exception(err.error_code, "Failed to get named collection {}: {}", collection_name, err.error_message);

    const auto & collections = resp->data().named_collections;
    if (collections.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected exactly one named collection for '{}', got {}",
            collection_name,
            collections.size());

    return collections[0];
}

void NamedCollectionsMetadataStorage::writeDescriptor(
    const std::string & collection_name, const cluster::protocol::NamedCollectionDescriptorPtr & collection, bool replace)
{
    auto & metastore = Globals::getMetaStore();
    auto timeout_ms = getContext()->getSettingsRef().query_timeout_sec * 1000;

    auto req = std::make_shared<cluster::CreateNamedCollectionRequest>(
        collection_name,
        collection,
        replace ? cluster::protocol::ExistsOperation::Replace : cluster::protocol::ExistsOperation::Throw,
        getContext()->getUserName(),
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/1);

    auto resp = metastore.createNamedCollection(std::move(req));
    if (resp->hasError())
        throw Exception(resp->error().error_code, "Can not write named collection {}: {}", collection_name, resp->error().error_message);
}

void NamedCollectionsMetadataStorage::writeCreateQuery(const ASTCreateNamedCollectionQuery & create_query, bool replace)
{
    auto collection = std::make_shared<cluster::protocol::NamedCollectionDescriptor>();
    collection->entries.reserve(create_query.changes.size());
    for (const auto & change : create_query.changes)
    {
        auto overridability = cluster::protocol::NamedCollectionOverridability::Default;
        if (auto iter = create_query.overridability.find(change.name); iter != create_query.overridability.end())
            overridability = iter->second ? cluster::protocol::NamedCollectionOverridability::Overridable
                                          : cluster::protocol::NamedCollectionOverridability::NotOverridable;
        collection->entries.emplace_back(cluster::protocol::NamedCollectionEntry{
            .key = change.name, .value = convertFieldToString(change.value), .overridability = overridability});
    }

    writeDescriptor(create_query.collection_name, collection, replace);
}

void NamedCollectionsMetadataStorage::remove(const std::string & collection_name)
{
    if (!removeIfExists(collection_name))
    {
        throw Exception(ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST, "Cannot remove `{}`, because it doesn't exist", collection_name);
    }
}

bool NamedCollectionsMetadataStorage::removeIfExists(const std::string & collection_name)
{
    auto & metastore = Globals::getMetaStore();
    auto timeout_ms = getContext()->getSettingsRef().query_timeout_sec * 1000;

    auto req = std::make_shared<cluster::DeleteNamedCollectionRequest>(
        collection_name,
        metastore.nodeID(),
        timeout_ms,
        /*request_version=*/1);

    auto resp = metastore.deleteNamedCollection(std::move(req));
    const auto & err = resp->error();

    if (!err.hasError())
        return true;

    if (err.isNotFound())
        return false;

    throw Exception(err.error_code, "Can not delete named collection {}: {}", collection_name, err.error_message);
}

MutableNamedCollectionPtr NamedCollectionsMetadataStorage::update(const ASTAlterNamedCollectionQuery & query)
{
    const auto & collection_name = query.collection_name;
    auto desc = readDescriptor(collection_name);
    auto collection_ptr = NamedCollectionFromDescriptor::create(collection_name, desc);
    collection_ptr->update(query);

    chassert(collection_ptr->getSourceId() == NamedCollection::SourceId::DESCRIPTOR);
    auto new_desc = std::static_pointer_cast<NamedCollectionFromDescriptor>(collection_ptr)->getDescriptor();
    writeDescriptor(collection_name, new_desc, true);

    return collection_ptr;
}

std::vector<std::string> NamedCollectionsMetadataStorage::listCollections() const
{
    auto & metastore = Globals::getMetaStore();
    auto timeout_ms = getContext()->getSettingsRef().query_timeout_sec * 1000;

    auto req = std::make_shared<cluster::ListNamedCollectionsRequest>(
        metastore.nodeID(),
        /*consistent_read=*/false,
        timeout_ms,
        /*request_version=*/1);

    auto resp = metastore.listNamedCollections(std::move(req));
    const auto & err = resp->error();
    if (err.hasError())
        throw Exception(err.error_code, "Failed to list named collections: {}", err.error_message);

    return resp->data().collection_names;
}

std::unique_ptr<NamedCollectionsMetadataStorage> NamedCollectionsMetadataStorage::create(const ContextPtr & context_)
{
    return std::unique_ptr<NamedCollectionsMetadataStorage>(new NamedCollectionsMetadataStorage(context_));
}

}
/// proton: ends
