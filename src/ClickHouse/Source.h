#pragma once

#include <ClickHouse/Client.h>
#include <Processors/ISource.h>

namespace DB
{

namespace ClickHouse
{

class Source final : public ISource
{
public:
    Source(
        const String & database,
        const String & table,
        const Block & header,
        std::unique_ptr<Client> client_,
        ContextPtr context_);

    String getName() const override { return "ClickHouseSource"; }

protected:
    Chunk generate() override;

private:
    bool started {false};

    std::unique_ptr<Client> client;
    String query;

    ContextPtr context;
};

}

}
