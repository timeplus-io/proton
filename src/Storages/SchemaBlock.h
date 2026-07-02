#pragma once

#include <Core/Block.h>

#include <memory>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct SchemaBlock
{
    StorageMetadataPtr storage_metadata;
    Block schema_block;
};
using SchemaBlockPtr = std::shared_ptr<SchemaBlock>;

}
