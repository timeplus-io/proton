#include <Interpreters/MetadataHelper.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

#include <Core/Names.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/Stream/StorageStream.h>
#include <Common/Exception.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int SYNTAX_ERROR;
}

cluster::protocol::StreamDescriptorPtrs getStreamDescriptorsFromMetaStore(const ConstStoragePtr table, size_t versions_requested)
{
    const auto & storage_id = table->getStorageID();

    auto result = Globals::getMetaStore().getStreamLocal(storage_id.database_name, storage_id.table_name, versions_requested);
    if (result.hasError())
        throw Exception(
            result.err.error_code,
            "Failed to get stream '{}' from MetaStore, reason={}",
            storage_id.getFullTableName(),
            result.err.error_message);

    auto & stream_descs = result.result;
    chassert(!stream_descs.empty() && stream_descs.size() <= versions_requested);
    return std::move(stream_descs);
}

/// \brief Retrieve and transform table schema from MetaStore.
ASTPtr getCreateTableFromStreamDescription(const cluster::protocol::StreamDescriptor & stream_desc, ContextPtr context)
{
    /// 1. Retrieve the raw table schema as unique_ptr<StreamDesc>.
    ParserCreateQuery parser;
    const char * pos = stream_desc.sql_def.data();
    std::string error_message;

    /// 2. Attempt to parse this string into AST.
    auto ast = tryParseQuery(
        parser,
        pos,
        pos + stream_desc.sql_def.size(),
        error_message,
        /* hilite = */ false,
        "from metastore",
        /* allow_multi_statements = */ false,
        0,
        context->getSettingsRef().max_parser_depth);

    if (!ast)
    {
        throw Exception::createDeprecated(error_message, ErrorCodes::SYNTAX_ERROR);
    }
    else
    {
        /// 3. Rewrite the AST from 'attach' style to 'create' table style.
        auto & create = ast->as<ASTCreateQuery &>();
        if (create.table && create.uuid != UUIDHelpers::Nil)
        {
            create.attach = false;
            create.setTable(stream_desc.stream.name);
            create.setDatabase(stream_desc.stream.ns);
        }
    }

    return ast;
}

ASTPtr getCreateTableFromMetaStore(const StoragePtr & table, ContextPtr context)
{
    auto stream_descs = getStreamDescriptorsFromMetaStore(table, 1);
    return getCreateTableFromStreamDescription(*stream_descs.front(), context);
}

std::vector<StorageMetadataPtr> getMultiVersionMetadataFromMetastore(const ConstStoragePtr & table, ContextPtr context)
{
    auto stream_descs = getStreamDescriptorsFromMetaStore(table, Globals::getMetaStore().getConfig().metadata_keep_versions);
    std::vector<StorageMetadataPtr> metadata_vec;
    metadata_vec.reserve(stream_descs.size());
    for (auto & stream_desc : stream_descs)
    {
        auto metadata = std::make_shared<StorageInMemoryMetadata>(table->getInMemoryMetadata());
        auto ast = getCreateTableFromStreamDescription(*stream_desc, context);
        updateMetadataByCreateQuery(ast, *metadata, std::const_pointer_cast<IStorage>(table), context, /*only_update_metadata=*/true);
        metadata->setVersion(stream_desc->data_version);
        metadata_vec.emplace_back(metadata);
    }
    return metadata_vec;
}

void applyMetadataChangesToCreateQuery(const ASTPtr & query, const StorageInMemoryMetadata & metadata)
{
    auto & ast_create_query = query->as<ASTCreateQuery &>();

    bool has_structure = ast_create_query.columns_list && ast_create_query.columns_list->columns;

    if (ast_create_query.as_table_function && !has_structure)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot alter stream {} because it was created AS function"
            " and doesn't have structure in metadata",
            backQuote(ast_create_query.getTable()));

    /// proton: starts. External stream / table can be defined without column list
    if (!has_structure && !ast_create_query.isDictionary() && !ast_create_query.isParameterizedView()
        && !ast_create_query.isExternalStream() && !ast_create_query.isExternalTable())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cannot alter stream {} metadata doesn't have structure", backQuote(ast_create_query.getTable()));
    /// proton: ends

    if (!ast_create_query.isDictionary() && !ast_create_query.isParameterizedView() && !ast_create_query.isExternalStream()
        && !ast_create_query.isExternalTable())  /// proton: updates
    {
        ASTPtr new_columns = InterpreterCreateQuery::formatColumns(metadata.columns);
        ASTPtr new_indices = InterpreterCreateQuery::formatIndices(metadata.secondary_indices);
        ASTPtr new_constraints = InterpreterCreateQuery::formatConstraints(metadata.constraints);
        ASTPtr new_projections = InterpreterCreateQuery::formatProjections(metadata.projections);

        ast_create_query.columns_list->replace(ast_create_query.columns_list->columns, new_columns);
        ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->indices, new_indices);
        ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->constraints, new_constraints);
        ast_create_query.columns_list->setOrReplace(ast_create_query.columns_list->projections, new_projections);
    }

    if (metadata.select.inner_query)
    {
        query->replace(ast_create_query.select, metadata.select.inner_query);
    }

    /// MaterializedView, Dictionary are types of CREATE query without storage.
    if (ast_create_query.storage)
    {
        ASTStorage & storage_ast = *ast_create_query.storage;

        bool is_extended_storage_def
            = storage_ast.partition_by || storage_ast.primary_key || storage_ast.order_by || storage_ast.sample_by || storage_ast.settings;

        if (is_extended_storage_def)
        {
            if (metadata.sorting_key.definition_ast)
                storage_ast.set(storage_ast.order_by, metadata.sorting_key.definition_ast);

            if (metadata.primary_key.definition_ast)
                storage_ast.set(storage_ast.primary_key, metadata.primary_key.definition_ast);

            if (metadata.sampling_key.definition_ast)
                storage_ast.set(storage_ast.sample_by, metadata.sampling_key.definition_ast);
            else if (storage_ast.sample_by != nullptr) /// SAMPLE BY was removed
                storage_ast.sample_by = nullptr;

            if (metadata.table_ttl.definition_ast)
                storage_ast.set(storage_ast.ttl_table, metadata.table_ttl.definition_ast);
            else if (storage_ast.ttl_table != nullptr) /// TTL was removed
                storage_ast.ttl_table = nullptr;

            if (metadata.settings_changes)
                storage_ast.set(storage_ast.settings, metadata.settings_changes);
        }
    }

    if (ast_create_query.isMaterializedView())
    {
        if (metadata.settings_changes)
        {
            ast_create_query.set(ast_create_query.storage_settings, metadata.settings_changes);

            /// If there are only read-only settings changes, then no need to display them additionally (e.g., index_granularity)
            if (std::ranges::all_of(ast_create_query.storage_settings->changes, [](const auto & change) {
                    return StreamSettings::isReadonlySetting(change.name);
                }))
                ast_create_query.reset(ast_create_query.storage_settings);
        }

        if (!ast_create_query.storage)
        {
            if (metadata.table_ttl.definition_ast)
                ast_create_query.mv_inner_storage_ttl = metadata.table_ttl.definition_ast;
            else if (ast_create_query.mv_inner_storage_ttl != nullptr) /// TTL was removed
                ast_create_query.mv_inner_storage_ttl = nullptr;
        }
    }

    if (metadata.comment.empty())
        ast_create_query.reset(ast_create_query.comment);
    else
        ast_create_query.set(ast_create_query.comment, std::make_shared<ASTLiteral>(metadata.comment));
}

/// It is the opposite of the function `applyMetadataChangesToCreateQuery()`
void updateMetadataByCreateQuery(
    const ASTPtr & query, StorageInMemoryMetadata & new_metadata, StoragePtr table, ContextPtr context, bool only_update_metadata)
{
    assert(table && context);
    auto & create = query->as<ASTCreateQuery &>();

    /// If db_table->isView() but `table` is a StorageStream, then `table` is an inner storage of a MaterializedView
    /// and it shares the same storage_id of the MaterializedView. Inner storage is not registered against DatabaseCatalog
    /// neither it is in metadb. In this case, we treat `table` as MaterializedView and get the versioned schema from
    /// its parent MaterializedView
    /// https://github.com/timeplus-io/proton-enterprise/issues/7068
    auto db_table = DatabaseCatalog::instance().getTable(table->getStorageID(), context);
    if (auto * mv = db_table->as<StorageMaterializedView>())
    {
        assert(create.select);

        /// 1) Update select query (so far only support modify query settings)
        auto select = SelectQueryDescription::getSelectQueryFromASTForView(create.select->clone(), context);
        new_metadata.setSelectQuery(select);

        /// 2) Update the MV schema (columns) to match the new SELECT query.
        /// Only applicable to MVs with an explicit external target stream (INTO target_stream):
        /// the target's schema is fixed by the user, so we intersect the new SELECT output with
        /// the target columns (dropping any SELECT columns absent from the target) and always
        /// preserve the two mandatory system columns (_tp_time / _tp_sn) from existing metadata.
        /// For MVs without an explicit target the inner storage does not support column changes,
        /// so we leave the MV schema untouched.
        if (mv->hasExternalTarget())
        {
            try
            {
                Block as_select_sample = InterpreterSelectWithUnionQuery::getSampleBlock(
                    select.inner_query->clone(), context, false /* is_subquery */, nullptr /* output_data_stream_semantic */);

                /// Collect column names present in the target stream.
                NameSet target_col_names;
                if (auto target = mv->tryGetTargetTable())
                {
                    for (const auto & col : target->getInMemoryMetadata().getColumns())
                        target_col_names.insert(col.name);
                }

                ColumnsDescription merged_columns;
                NameSet added_col_names;
                for (const auto & col : as_select_sample.getNamesAndTypesList())
                {
                    /// Drop columns absent from the target — they cannot be written there.
                    if (!target_col_names.empty() && !target_col_names.contains(col.name))
                        continue;

                    merged_columns.add(ColumnDescription(col.name, col.type));
                    added_col_names.insert(col.name);
                }

                /// Always preserve the two mandatory system columns from existing metadata.
                for (const auto & sys_col_name : {ProtonConsts::RESERVED_EVENT_TIME, ProtonConsts::RESERVED_EVENT_SEQUENCE_ID})
                {
                    if (!added_col_names.contains(sys_col_name))
                    {
                        if (new_metadata.getColumns().has(sys_col_name))
                            merged_columns.add(new_metadata.getColumns().get(sys_col_name));
                    }
                }

                new_metadata.setColumns(merged_columns);
            }
            catch (...)
            {
                tryLogCurrentException("MetadataHelper", "Failed to update MV schema from new SELECT query, keeping previous schema");
            }
        }

        /// 3) Update inner storage settings and TTL
        if (create.isMaterializedView() && !create.storage)
        {
            auto inner_storage = mv->tryGetTargetTable();
            if (inner_storage && !only_update_metadata)
            {
                auto inner_metadata = inner_storage->getInMemoryMetadata();
                if (create.mv_inner_storage_ttl)
                {
                    inner_metadata.setTableTTLs(TTLTableDescription::getTTLForTableFromAST(
                        create.mv_inner_storage_ttl, inner_metadata.columns, context, inner_metadata.primary_key));
                }
                else if (TTLTableDescription ttl = inner_metadata.getTableTTLs(); ttl.definition_ast)
                {
                    inner_metadata.setTableTTLs(TTLTableDescription{});
                }

                if (create.storage_settings)
                {
                    if (auto * inner_stream = inner_storage->as<StorageStream>())
                    {
                        /// After config a setting, it is not easy to remove it instead we will need
                        /// explicitly reset the setting to a value
                        /// For example, after doing `ALTER STREAM mv MODIFY SETTING logstore_retention_bytes=10000;`
                        /// It is hard to remove this setting. We can do reset though
                        /// `ALTER STREAM mv RESET SETTING logstore_retention_bytes` which is not supported yet
                        auto copy = std::make_unique<MergeTreeSettings>(*inner_stream->getSettings());
                        copy->applyChanges(create.storage_settings->changes);
                        inner_stream->storage_settings.set(std::move(copy));

                        inner_metadata.setSettingsChanges(create.storage_settings->ptr());
                    }
                }

                /// inner_metadata.setVersion(inner_metadata.getVersion() + 1);
                inner_storage->setInMemoryMetadata(inner_metadata);
            }
        }

        /// 4) Update mv TTL
        if (create.mv_inner_storage_ttl)
        {
            new_metadata.setTableTTLs(TTLTableDescription::getTTLForTableFromAST(
                create.mv_inner_storage_ttl, new_metadata.columns, context, new_metadata.primary_key));
        }
        else if (TTLTableDescription ttl = new_metadata.getTableTTLs(); ttl.definition_ast)
        {
            new_metadata.setTableTTLs(TTLTableDescription{});
        }

        /// FIXME: restart pipeline ?
    }
    else if (const auto * stream = table->as<StorageStream>())
    {
        assert(create.storage);
        /// 1) Update settings
        if (create.storage->settings)
        {
            if (!only_update_metadata)
            {
                auto copy = stream->getDefaultSettings();
                copy->applyChanges(create.storage->settings->changes);
                const_cast<StorageStream *>(stream)->storage_settings.set(std::move(copy));
            }

            new_metadata.setSettingsChanges(create.storage->settings->ptr());
        }
        else
        {
            new_metadata.setSettingsChanges(nullptr);
        }

        /// 2) Update columns description
        assert(create.columns_list && create.columns_list->columns); /// already normalized
        new_metadata.setColumns(InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, context, create.attach));

        /// 3) Update TTL
        if (create.storage->ttl_table)
        {
            new_metadata.setTableTTLs(TTLTableDescription::getTTLForTableFromAST(
                create.storage->ttl_table->ptr(), new_metadata.columns, context, new_metadata.primary_key));
        }
        else if (TTLTableDescription ttl = new_metadata.getTableTTLs(); ttl.definition_ast)
        {
            new_metadata.setTableTTLs(TTLTableDescription{});
        }
    }
    else if (auto * external_stream = table->as<StorageExternalStream>(); external_stream != nullptr)
    {
        assert(create.storage);
        /// 1) Update settings
        if (create.storage->settings)
        {
            new_metadata.setSettingsChanges(create.storage->settings->ptr());
        }
        else
        {
            new_metadata.setSettingsChanges(nullptr);
        }
    }
    else if (auto * external_table = table->as<StorageExternalTable>(); external_table != nullptr)
    {
        assert(create.storage);
        /// 1) Update settings
        if (create.storage->settings)
        {
            new_metadata.setSettingsChanges(create.storage->settings->ptr());
        }
        else
        {
            new_metadata.setSettingsChanges(nullptr);
        }
    }
    else
    {
        /// TODO
        return;
    }
}
}
