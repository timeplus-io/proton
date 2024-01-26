#pragma once

#include <Storages/IStorage.h>

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

    virtual SinkToStoragePtr write(const ASTPtr & /* query */, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr /* context */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Ingesting data to this type of external stream is not supported");
}
};

using IExternalTablePtr = std::unique_ptr<IExternalTable>;

}
