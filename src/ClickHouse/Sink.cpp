#include <ClickHouse/Sink.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/queryToString.h>

namespace DB
{

namespace ClickHouse
{

namespace
{

ASTPtr createInsertToRemoteTableQuery(const std::string & database, const std::string & table, const Names & column_names)
{
    auto query = std::make_shared<ASTInsertQuery>();
    query->table_id = StorageID(database, table);
    auto columns = std::make_shared<ASTExpressionList>();
    query->columns = columns;
    query->children.push_back(columns);
    for (const auto & column_name : column_names)
        columns->children.push_back(std::make_shared<ASTIdentifier>(column_name));
    return query;
}

}

Sink::Sink(
    const String & database,
    const String & table,
    const Block & header,
    const Names & columns_to_send_,
    std::unique_ptr<Client> client_,
    const ContextPtr & context,
    LoggerPtr logger_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , insert_into(queryToString(createInsertToRemoteTableQuery(database, table, columns_to_send_)))
    , columns_to_send(columns_to_send_.begin(), columns_to_send_.end())
    , client(std::move(client_))
    , batch(context->getSettingsRef().max_insert_block_size, context->getSettingsRef().max_insert_block_bytes)
    , logger(std::move(logger_))
{
    auto timeout = context->getSettingsRef().insert_block_timeout_ms.value;
    if (timeout < 0)
        batch_timeout_ms = std::numeric_limits<UInt64>::max();
    else
        batch_timeout_ms = timeout;

    auto idle_connection_timeout = context->getSettingsRef().client_idle_connection_timeout.value;
    if (idle_connection_timeout <= 0)
        idle_connection_timeout_ms = std::numeric_limits<UInt64>::max();
    else
        idle_connection_timeout_ms = idle_connection_timeout * 1000;

    LOG_INFO(
        logger,
        "Writing to ClickHouse with max_insert_block_size={} max_insert_block_bytes={} insert_block_timeout_ms={}",
        context->getSettingsRef().max_insert_block_size.value,
        context->getSettingsRef().max_insert_block_bytes.value,
        timeout);

    if (columns_to_send.size() != header.columns())
    {
        columns_to_send_positions.reserve(columns_to_send.size());
        for (const auto & name : columns_to_send)
            columns_to_send_positions.push_back(header.getPositionByName(name));
    }
}

void Sink::consume(Chunk chunk)
{
    if (isCancelled())
        return;

    if (chunk.rows() > 0)
        addToBatch(getHeader().cloneWithColumns(chunk.detachColumns()));

    if (batch_timer.elapsedMilliseconds() >= batch_timeout_ms)
        flushBatch();
}

void Sink::addToBatch(Block && block)
{
    std::lock_guard<std::mutex> lock{batch_mutex};

    removeSuperfluousColumns(block);

    auto data = batch.add(block);
    if (data.rows() == 0)
        return;

    idle_connection_timer.restart();
    batch_timer.restart();

    client->executeInsertQuery(insert_into, data);
    client->throwServerExceptionIfAny();
}

void Sink::flushBatch()
{
    if (batch.isEmpty())
    {
        if (idle_connection_timer.elapsedMilliseconds() >= idle_connection_timeout_ms)
        {
            client->releaseConnection();
            idle_connection_timer.restart();
        }

        batch_timer.restart();
        return;
    }

    addToBatch({});
}

void Sink::doCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    flushBatch();
    IProcessor::doCheckpoint(ckpt_ctx);
}

void Sink::removeSuperfluousColumns(Block & block) const
{
    if (columns_to_send_positions.empty())
        return;

    /// It could be an empty block
    if (block.columns() == 0)
        return;

    Block result;
    result.reserve(columns_to_send_positions.size());
    for (auto pos : columns_to_send_positions)
        result.insert(std::move(block.getByPosition(pos)));

    block.swap(result);
}

void Sink::onCancel() noexcept
{
    try
    {
        flushBatch();
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to flush batch");
    }
}

}

}
