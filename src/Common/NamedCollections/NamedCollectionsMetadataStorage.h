#pragma once

#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Interpreters/Context_fwd.h>


/// proton: starts
/// Deprecate the legacy local/config storage and read/write NamedCollectionDescriptor
/// directly via MetaStore. SQL and CONFIG type named collections are no longer
/// persisted locally after the change.
namespace cluster::protocol
{
struct NamedCollectionDescriptor;
using NamedCollectionDescriptorPtr = std::shared_ptr<NamedCollectionDescriptor>;
}

namespace DB
{
class NamedCollectionsMetadataStorage : private WithContext
{
public:
    static std::unique_ptr<NamedCollectionsMetadataStorage> create(const ContextPtr & context);

    NamedCollectionsMap getAll() const;

    MutableNamedCollectionPtr get(const std::string & collection_name) const;

    MutableNamedCollectionPtr create(const ASTCreateNamedCollectionQuery & query);

    void remove(const std::string & collection_name);

    bool removeIfExists(const std::string & collection_name);

    MutableNamedCollectionPtr update(const ASTAlterNamedCollectionQuery & query);

    void shutdown();

private:
    explicit NamedCollectionsMetadataStorage(ContextPtr context_);

    [[nodiscard]] std::vector<std::string> listCollections() const;

    [[nodiscard]] cluster::protocol::NamedCollectionDescriptorPtr readDescriptor(const std::string & collection_name) const;

    void writeDescriptor(
        const std::string & collection_name, const cluster::protocol::NamedCollectionDescriptorPtr & collection, bool replace = false);

    void writeCreateQuery(const ASTCreateNamedCollectionQuery & create_query, bool replace = false);
};
/// proton: ends

}
