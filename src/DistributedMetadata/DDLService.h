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
    static DDLService & instance(const ContextMutablePtr & global_context_);

    explicit DDLService(const ContextMutablePtr & glboal_context_);
    virtual ~DDLService() override = default;

    Int32 append(nlog::Record & ddl_record) const;

private:
    void processRecords(const nlog::RecordPtrs & records) override;
    String role() const override { return "ddl"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(10, 200); }

private:
    using CallBack = std::function<void()>;

    Int32 doDDL(const String & payload, const Poco::URI & uri, const String & method, const String & query_id, const String & user) const;
    void doDDLOnHosts(
        std::vector<Poco::URI> & target_hosts,
        const String & payload,
        const String & method,
        const String & query_id,
        const String & user,
        const String & uuid = "",
        CallBack finished_callback = {}) const;

    void createTable(nlog::Record & record);
    void mutateTable(nlog::Record & record, const String & method, CallBack finished_callback = nullptr) const;
    void mutateDatabase(nlog::Record & record, const String & method) const;
    void executeSystemCommand(nlog::Record & record, const String & method) const;
    /// system command 'REPLACE REPLICA', return false means failed
    bool replaceReplica(const String & replica, const String & target, const String & query_id, const String & user, String & err) const;

    void commit(Int64 last_sn);
    void createDWAL(const String & uuid, Int32 shards, Int32 replication_factor, const String * url_parameters) const;

    void updateDDLStatus(
        const String & query_id,
        const String & user,
        const String & status,
        const String & query,
        const String & progress,
        const String & reason) const;
    void progressDDL(const String & query_id, const String & user, const String & query, const String & progress) const;
    void succeedDDL(const String & query_id, const String & user, const String & query = "") const;
    void failDDL(const String & query_id, const String & user, const String & query = "", const String & reason = "") const;

    bool validateSchema(const Block & block, const std::vector<String> & col_names) const;

    std::vector<Poco::URI> getTargetURIs(nlog::Record & record, const String & database, const String & table, const String & method) const;
    std::vector<Poco::URI>
    getSystemCommandURIs(UInt64 type_, const String & replica, const String & database, const String & table, Int32 shard = -1) const;

private:
    CatalogService & catalog;
    PlacementService & placement;
    TaskStatusService & task;
};
}
