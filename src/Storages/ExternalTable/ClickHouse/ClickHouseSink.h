#pragma once

#include <Client/Connection.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

namespace ExternalTable
{

class ClickHouseSink final : public SinkToStorage
{
public:
    ClickHouseSink(const Block & header, const ConnectionParameters & params_, ContextPtr & context_, Poco::Logger * logger_);

    String getName() const override { return "ClickHouseSink"; }

    void consume(Chunk chunk) override;

private:
    const ConnectionParameters & params;
    std::unique_ptr<Connection> conn;

    ContextPtr & context;
    Poco::Logger * logger;
};

}

}
