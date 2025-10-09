#pragma once

namespace rocksdb
{
class Status;
}

namespace cluster
{
int mapRocksStatus(const rocksdb::Status & status);
}
