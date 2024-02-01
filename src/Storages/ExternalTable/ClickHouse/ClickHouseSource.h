#pragma once

#include <Client/ClickHouseClient.h>
#include <Processors/ISource.h>

namespace DB
{

namespace ExternalTable
{

class ClickHouseSource final : public ISource
{
public:
    ClickHouseSource(
        const String & database,
        const String & table,
        const Block & header,
        std::unique_ptr<ClickHouseClient> client_,
        QueryProcessingStage::Enum processed_stage,
        ContextPtr context_,
        Poco::Logger * logger_);

    String getName() const override { return "ClickHouseSource"; }

protected:
    Chunk generate() override;

private:
    bool started {false};

    std::unique_ptr<ClickHouseClient> client;
    String query;

    ContextPtr context;
    Poco::Logger * logger;
};

}

}
