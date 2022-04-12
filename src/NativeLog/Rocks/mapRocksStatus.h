#pragma once

namespace rocksdb
{
class Status;
}

namespace nlog
{
int mapRocksStatus(const rocksdb::Status & status);
}
