#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <Cluster/Protocol/StreamDescriptor.h>

namespace DB
{
class ASTCreateQuery;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

cluster::protocol::StreamDescriptorPtrs getStreamDescriptorsFromMetaStore(const ConstStoragePtr table, size_t versions_requested);

ASTPtr getCreateTableFromStreamDescription(const cluster::protocol::StreamDescriptor & stream_desc, ContextPtr context);

ASTPtr getCreateTableFromMetaStore(const StoragePtr & table, ContextPtr context);

std::vector<StorageMetadataPtr> getMultiVersionMetadataFromMetastore(const ConstStoragePtr & table, ContextPtr context);

/// \brief: Apply metadata changes to CREATE query
void applyMetadataChangesToCreateQuery(const ASTPtr & query, const StorageInMemoryMetadata & metadata);

/// \brief: Update metadata and storage by CREATE query,
/// if \only_update_metadata is true, don't update storage (which used for loading multi versions metadata)
void updateMetadataByCreateQuery(
    const ASTPtr & query, StorageInMemoryMetadata & new_metadata, StoragePtr table, ContextPtr context, bool only_update_metadata);
}
