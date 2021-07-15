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

    Int32 append(const DWAL::Record & ddl_record) const;

private:
    void processRecords(const DWAL::RecordPtrs & records) override;
    String role() const override { return "ddl"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(10, 200); }

private:
    Int32 doDDL(const String & payload, const Poco::URI & uri, const String & method, const String & query_id, const String & user) const;

    void createTable(DWAL::RecordPtr record);
    void mutateTable(DWAL::RecordPtr record, const String & method) const;
    void mutateDatabase(DWAL::RecordPtr record, const String & method) const;
    void commit(Int64 last_sn);
    void
    createDWAL(const String & database, const String & table, Int32 shards, Int32 replication_factor, const String * url_parameters) const;

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
    std::vector<Poco::URI>
    getTargetURIs(DWAL::RecordPtr record, const String & database, const String & table, const String & method) const;

private:
    CatalogService & catalog;
    PlacementService & placement;
    TaskStatusService & task;
};
}
