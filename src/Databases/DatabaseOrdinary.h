#pragma once

#include <Databases/DatabaseOnDisk.h>
#include <Common/ThreadPool.h>
/// proton: starts
#include <Cluster/Protocol/AlertDescriptor.h>

#include <unordered_map>
/// proton: ends


namespace DB
{

/** Default engine of databases.
  * It stores tables list in filesystem using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query.
  */
class DatabaseOrdinary : public DatabaseOnDisk
{
public:
    DatabaseOrdinary(const String & name_, const String & metadata_path_, ContextPtr context);
    DatabaseOrdinary(
        const String & name_, const String & metadata_path_, const String & data_path_,
        const String & logger, ContextPtr context_);

    String getEngineName() const override { return "Ordinary"; }

    void loadStoredObjects(ContextMutablePtr context, bool force_restore, bool force_attach, bool skip_startup_tables) override;

    bool supportsLoadingInTopologicalOrder() const override { return true; }

    void loadTablesMetadata(ContextPtr context, ParsedTablesMetadata & metadata, bool is_startup) override;

    void loadTableFromMetadata(ContextMutablePtr local_context, const QualifiedTableName & name, const ParsedTableMetadata & parsed_meta_data, bool force_restore) override;

    void startupTables(ThreadPool & thread_pool, bool force_restore, bool force_attach) override;

    void alterTable(
        ContextPtr context,
        const StorageID & table_id,
        const StorageInMemoryMetadata & metadata,
        String alter_command,
        std::vector<String> alter_command_asts) override;

protected:
    virtual void commitAlterTable(
        const StorageID & table_id,
        const String & table_metadata_tmp_path,
        const String & table_metadata_path,
        const String & statement,
        ContextPtr query_context);

private:
    /// proton : starts
    std::unordered_map<std::string, cluster::protocol::AlertDescriptorPtr> alert_descriptors;

    void loadTablesMetadataFromMetaStore(const ContextPtr & local_context, ParsedTablesMetadata & metadata);

    void loadAlertsMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata);
    void loadAlert(const QualifiedTableName & name, ContextMutablePtr local_context);
    /// proton : ends
};

}
