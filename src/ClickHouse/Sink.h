#pragma once

#include <ClickHouse/Client.h>
#include <Formats/FormatFactory.h>
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
        std::unique_ptr<Client> client_,
        ContextPtr context_,
        Poco::Logger * logger_);

    String getName() const override { return "ClickHouseSink"; }

    void consume(Chunk chunk) override;

private:
    String insert_into;

    std::unique_ptr<Client> client;

    std::unique_ptr<WriteBufferFromOwnString> buf;
    OutputFormatPtr output_format;

    ContextPtr context;
    Poco::Logger * logger;
};

}

}
