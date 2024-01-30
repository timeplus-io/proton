#pragma once

#include <Client/LibClient.h>
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
        ContextPtr context_,
        Poco::Logger * logger_);

    String getName() const override { return "ClickHouseSink"; }

    void consume(Chunk chunk) override;

private:
    String insert_into;

    std::unique_ptr<LibClient> client;

    std::unique_ptr<WriteBufferFromOwnString> buf;
    OutputFormatPtr output_format;

    ContextPtr context;
    Poco::Logger * logger;
};

}

}
