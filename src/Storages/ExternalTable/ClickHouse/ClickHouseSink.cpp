#include <Common/logger_useful.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouseSink.h>

namespace DB
{

namespace ExternalTable
{

ClickHouseSink::ClickHouseSink(const Block & header, Poco::Logger * logger_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , logger(logger_)
{
}

void ClickHouseSink::consume(Chunk chunk)
{
    LOG_INFO(logger, "consuming from chunk contains {} rows", chunk.rows());
}

}

}
