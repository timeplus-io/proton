#pragma once

#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>

namespace DB
{
/// Base class of StorageExternalStreamImpl
class StorageExternalStreamImpl : public std::enable_shared_from_this<StorageExternalStreamImpl>
{
public:
    virtual ~StorageExternalStreamImpl() = default;

    virtual void startup() = 0;
    virtual void shutdown() = 0;
    virtual bool supportsSubcolumns() const { return false; }
    virtual NamesAndTypesList getVirtuals() const { return {}; }
    virtual ExternalStreamCounterPtr getExternalStreamCounter() const { return nullptr; }

    virtual Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams)
        = 0;

    virtual SinkToStoragePtr write(const ASTPtr & /* query */, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr /* context */)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Ingesting data to this type of external stream is not supported");
    }
};

}
