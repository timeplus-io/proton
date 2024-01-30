#pragma once

#include <Storages/IStorage.h>
#include "QueryPipeline/Pipe.h"

namespace DB
{

/// The interface for an External Table implementation to implement.
class IExternalTable
{
public:
    virtual ~IExternalTable() = default;

    virtual void startup() = 0;
    virtual void shutdown() = 0;

    virtual ColumnsDescription getTableStructure() = 0;

    virtual Pipe read(
        const Names &  /*column_names*/,
        const StorageSnapshotPtr &  /*storage_snapshot*/,
        SelectQueryInfo &  /*query_info*/,
        ContextPtr  /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t  /*max_block_size*/,
        size_t /*num_streams*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading data from this type of external table is not supported");
    }

    virtual SinkToStoragePtr write(const ASTPtr & /* query */, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr /* context */)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Ingesting data to this type of external table is not supported");
    }
};

using IExternalTablePtr = std::unique_ptr<IExternalTable>;

}
