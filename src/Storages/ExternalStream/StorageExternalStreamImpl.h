#pragma once

#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
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

    std::shared_ptr<ExternalStreamCounter> external_stream_counter;
};

}
