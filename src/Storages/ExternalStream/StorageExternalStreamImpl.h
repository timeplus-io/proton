#pragma once

#include <Storages/IStorage.h>

namespace DB
{
/// Base class of StorageExternalStreamImpl
class StorageExternalStreamImpl
{
public:
    virtual ~StorageExternalStreamImpl() = default;

    virtual void startup() = 0;
    virtual void shutdown() = 0;
    virtual bool supportsSubcolumns() const { return false; }
    virtual NamesAndTypesList getVirtuals() const { return {}; }

    virtual Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams)
        = 0;
};

}
