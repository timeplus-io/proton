#pragma once

#include "MetadataService.h"

#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>


namespace DB
{
class Context;
class CatalogService;
class PlacementService;
class TaskStatusService;

class DDLService final : public MetadataService
{
public:
    static DDLService & instance(const ContextPtr & global_context_);

    explicit DDLService(const ContextPtr & glboal_context_);
    virtual ~DDLService() override = default;

private:
    void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) override;
    String role() const override { return "ddl"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(10, 200); }

private:
    Int32 sendRequest(const String & payload, const Poco::URI & uri, const String & method, const String & query_id) const;
    Int32 doDDL(const String & payload, const Poco::URI & uri, const String & method, const String & query_id) const;
    void createTable(IDistributedWriteAheadLog::RecordPtr record);
    void mutateTable(IDistributedWriteAheadLog::RecordPtr record, const String & method) const;
    void mutateDatabase(IDistributedWriteAheadLog::RecordPtr record, const String & method) const;
    void commit(Int64 last_sn);

private:
    void updateDDLStatus(
        const String & query_id,
        const String & user,
        const String & status,
        const String & query,
        const String & progress,
        const String & reason) const;

    void progressDDL(const String & query_id, const String & user, const String & query, const String & progress) const;

    void succeedDDL(const String & query_id, const String & user, const String & query = "") const;
    void failDDL(const String & query_id, const String & user, const String & query = "", const String reason = "") const;

    bool validateSchema(const Block & block, const std::vector<String> & col_names) const;

private:
    String http_port;

    CatalogService & catalog;
    PlacementService & placement;
    TaskStatusService & task;
};
}
