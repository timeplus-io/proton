#include <Client/ConnectionParameters.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouseSink.h>
#include <Common/logger_useful.h>
#include "Client/LibClient.h"
#include "IO/WriteBufferFromString.h"

namespace DB
        {

namespace ExternalTable
{

ClickHouseSink::ClickHouseSink(
     const Block & header,
     const ConnectionParameters & params_,
     ContextPtr & context_,
     Poco::Logger * logger_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , params(params_)
    , context(context_)
    , logger(logger_)
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
    LibClient client {*conn, params.timeouts, context, logger};

    String query = "INSERT INTO my_first_table (user_id, message, timestamp, metric) VALUES ";
    for (size_t i = 0; i < chunk.rows(); ++i)
    {
        const auto & cols = chunk.getColumns();
        Field f {};
        cols[0]->get(i, f);
        auto user_id = f.get<UInt32>();

        cols[1]->get(i, f);
        auto message = f.get<String>();

        cols[2]->get(i, f);
        auto ts = f.get<UInt32>();

        cols[3]->get(i, f);
        auto metric = f.get<Float32>();

        query.append(fmt::format("({}, {}, {}, {})", user_id, quoteString(message), ts, metric));
    }

    LOG_INFO(logger, "sending query {}", query);
    conn->sendQuery(params.timeouts, query, {}, "", QueryProcessingStage::Complete, nullptr, nullptr, false);
    LOG_INFO(logger, "query sent!");

    client.receiveResult({
        .on_data = [this](Block & block)
        {
            LOG_INFO(logger, "INSERT INTO returns {} columns and {} rows", block.columns(), block.rows());
            if (!block.rows())
                return;

            const auto & cols = block.getColumns();
            for (size_t i = 0; i < block.rows(); ++i)
            {
                for (const auto & col : block.getColumns())
                LOG_INFO(logger, "row {}: col_name = {}", i, col->getName());
            }
        }
    });
    LOG_INFO(logger, "consume done!");
}

}

}
