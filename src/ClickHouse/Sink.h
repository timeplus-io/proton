#pragma once

#include <ClickHouse/Client.h>
#include <Interpreters/SquashingTransform.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

namespace ClickHouse
{

class Sink final : public SinkToStorage
{
public:
    Sink(
        const String & database,
        const String & table,
        const Block & header,
        const Names & columns_to_send_,
        std::unique_ptr<Client> client_,
        const ContextPtr & context,
        LoggerPtr logger_);

    String getName() const override { return "ClickHouseSink"; }

    void consume(Chunk chunk) override;

protected:
    void onCancel() noexcept override;
    void onFinish() override { flushBatch(); }
    void checkpoint(CheckpointContextPtr) override;

private:
    void removeSuperfluousColumns(Block & block) const;
    void addToBatch(Block && block);
    void flushBatch();

    const String insert_into;
    NameSet columns_to_send;
    std::vector<size_t> columns_to_send_positions;
    std::unique_ptr<Client> client;

    UInt64 idle_connection_timeout_ms{0};
    Stopwatch idle_connection_timer;

    std::mutex batch_mutex;
    SquashingTransform batch;
    UInt64 batch_timeout_ms{0};
    Stopwatch batch_timer;

    LoggerPtr logger;
};

}

}
