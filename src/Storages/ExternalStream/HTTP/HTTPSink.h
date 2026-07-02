#pragma once

#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/BatchConfiguration.h>
#include <Storages/ExternalStream/HTTP/HTTPConfiguration.h>
#include <Storages/PartitionedSink.h>

namespace DB
{

class MessageQueueFormatExecutor;

namespace ExternalStream
{

class HTTPSink : public SinkToStorage
{
public:
    HTTPSink(
        const HTTPConfiguration & http_config_,
        const Block & sample_block,
        const BatchConfiguration & batch_config_,
        const ContextPtr & context_);

    ~HTTPSink() override;

    std::string getName() const override { return "HTTPSink"; }
    void consume(Chunk chunk) override;
    void onFinish() override;

    void doCheckpoint(CheckpointContextPtr context) override;

private:
    void send(const String & message) const;

    void flush();

    HTTPConfiguration http_config;
    BatchConfiguration batch_config;

    Poco::URI uri;
    String content_type;
    String content_encoding;

    std::unique_ptr<MessageQueueFormatExecutor> format_executor;

    Stopwatch request_timer;

    HTTPSessionPtr session;

    ContextPtr context;
};
;


class PartitionedHTTPSink : public PartitionedSink
{
public:
    PartitionedHTTPSink(
        const ASTPtr & partition_by,
        HTTPConfiguration && http_config_,
        const Block & sample_block_,
        BatchConfiguration && batch_config_,
        const ContextPtr & context_)
        : PartitionedSink(partition_by, context_, sample_block_, ProcessorID::PartitionedHTTPSink)
        , sample_block(sample_block_)
        , http_config(std::move(http_config_))
        , batch_config(std::move(batch_config_))
        , context(context_)
    {
    }

    SinkPtr createSinkForPartition(const String & partition_id) override
    {
        auto partition_path = PartitionedSink::replaceWildcards(http_config.uri, partition_id);
        context->getRemoteHostFilter().checkURL(Poco::URI(partition_path));

        auto config_for_sink = http_config;
        config_for_sink.uri = partition_path;

        return std::make_shared<HTTPSink>(config_for_sink, sample_block, batch_config, context);
    }

private:
    const Block sample_block;

    HTTPConfiguration http_config;
    BatchConfiguration batch_config;

    ContextPtr context;
};

}

}
