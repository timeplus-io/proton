#include "mapRocksStatus.h"

#include <rocksdb/status.h>

namespace DB::ErrorCodes
{
extern int OK;
extern int RESOURCE_NOT_FOUND;
extern int UNSUPPORTED;
extern int TOO_MANY_SIMULTANEOUS_QUERIES;
extern int INTERNAL_ERROR;
}

namespace nlog
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
        return DB::ErrorCodes::INTERNAL_ERROR;
}
}
