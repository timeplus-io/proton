#pragma once

#include <Client/Connection.h>
#include <Formats/FormatFactory.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

namespace ExternalTable
{

class ClickHouseSink final : public SinkToStorage
{
public:
    ClickHouseSink(
        const String & table,
        const Block & header,
        const ConnectionParameters & params_,
        ContextPtr & context_,
        Poco::Logger * logger_);

    String getName() const override { return "ClickHouseSink"; }

    void consume(Chunk chunk) override;

private:
    String insert_into;

    const ConnectionParameters & params;
    std::unique_ptr<Connection> conn;

    std::unique_ptr<WriteBufferFromOwnString> buf;
    OutputFormatPtr output_format;

    ContextPtr & context;
    Poco::Logger * logger;
};

}

}
