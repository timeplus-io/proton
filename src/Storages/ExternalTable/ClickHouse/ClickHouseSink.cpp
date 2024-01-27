#include <Client/ConnectionParameters.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouseSink.h>
#include <Common/logger_useful.h>
#include "IO/WriteBufferFromString.h"
#include <Parsers/ASTInsertQuery.h>

namespace DB
        {

namespace ExternalTable
{

ClickHouseSink::ClickHouseSink(const Block & header, const ConnectionParameters & params_, Poco::Logger * logger_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID), params(params_), logger(logger_)
{
    conn = std::make_unique<Connection>(
        params.host,
        params.port,
        params.default_database,
        params.user,
        params.password,
        params.quota_key,
        "", /*cluster*/
        "", /*cluster_secret*/
        "TimeplusProton",
        params.compression,
        params.security);

    conn->setCompatibleWithClickHouse();
}

void ClickHouseSink::consume(Chunk chunk)
{
    LOG_INFO(logger, "consuming from chunk contains {} rows", chunk.rows());
    ASTInsertQuery query{};
    query.setDatabase("default");
    query.setTable("my_first_table");

    WriteBufferFromOwnString wb {};
    const IAST::FormatSettings fmt_settings {wb, true};
    ASTInsertQuery * query_ptr = &query;
    dynamic_cast<IAST*>(query_ptr)->format(fmt_settings);
    conn->sendQuery(params.timeouts, wb.str(), {}, "", QueryProcessingStage::Complete, nullptr, nullptr, false);
}

}

}
