#include <Cluster/Rocks/mapRocksStatus.h>

#include <rocksdb/status.h>

namespace DB::ErrorCodes
{
extern const int OK;
extern const int RESOURCE_NOT_FOUND;
extern const int UNSUPPORTED;
extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
extern const int ROCKSDB_ERROR;
}

namespace cluster
{
int mapRocksStatus(const rocksdb::Status & status)
{
    if (status.ok())
        return DB::ErrorCodes::OK;
    else if (status.IsNotFound())
        return DB::ErrorCodes::RESOURCE_NOT_FOUND;
    else if (status.IsNotSupported())
        return DB::ErrorCodes::UNSUPPORTED;
    else if (status.IsBusy())
        return DB::ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES;
    else
        return DB::ErrorCodes::ROCKSDB_ERROR;
}
}
