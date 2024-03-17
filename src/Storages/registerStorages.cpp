#include <Storages/registerStorages.h>
#include <Storages/StorageFactory.h>

#include "config.h"

namespace DB
{

void registerStorageMergeTree(StorageFactory & factory);
void registerStorageNull(StorageFactory & factory);
void registerStorageBuffer(StorageFactory & factory);
void registerStorageDistributed(StorageFactory & factory);
void registerStorageMemory(StorageFactory & factory);
void registerStorageFile(StorageFactory & factory);
void registerStorageURL(StorageFactory & factory);
void registerStorageDictionary(StorageFactory & factory);
void registerStorageSet(StorageFactory & factory);
void registerStorageJoin(StorageFactory & factory);
void registerStorageView(StorageFactory & factory);
/// proton: starts.
void registerStorageStream(StorageFactory & factory);
void registerStorageMaterializedView(StorageFactory & factory);
void registerStorageExternalTable(StorageFactory & factory);
void registerStorageExternalStream(StorageFactory & factory);
void registerStorageRandom(StorageFactory & factory);
/// proton: ends.
void registerStorageGenerateRandom(StorageFactory & factory);
void registerStorageExecutable(StorageFactory & factory);

#if USE_AWS_S3
void registerStorageS3(StorageFactory & factory);
void registerStorageCOS(StorageFactory & factory);
#endif

#if USE_ROCKSDB
void registerStorageEmbeddedRocksDB(StorageFactory & factory);
#endif

/// proton: starts
/// #if USE_FILELOG
/// void registerStorageFileLog(StorageFactory & factory);
/// #endif
/// proton: ends

void registerStorages()
{
    auto & factory = StorageFactory::instance();

    registerStorageMergeTree(factory);
    registerStorageNull(factory);
    registerStorageBuffer(factory);
    registerStorageDistributed(factory);
    registerStorageMemory(factory);
    registerStorageFile(factory);
    registerStorageURL(factory);
    registerStorageDictionary(factory);
    registerStorageSet(factory);
    registerStorageJoin(factory);
    registerStorageView(factory);
    /// proton: starts.
    registerStorageStream(factory);
    registerStorageMaterializedView(factory);
    registerStorageExternalTable(factory);
    registerStorageExternalStream(factory);
    registerStorageRandom(factory);
    /// proton: ends.
    registerStorageGenerateRandom(factory);
    registerStorageExecutable(factory);

    #if USE_AWS_S3
    registerStorageS3(factory);
    registerStorageCOS(factory);
    #endif

    /// proton: starts
    /// #if USE_FILELOG
    /// registerStorageFileLog(factory);
    /// #endif
    /// proton: ends

    #if USE_ROCKSDB
    registerStorageEmbeddedRocksDB(factory);
    #endif
}

}
