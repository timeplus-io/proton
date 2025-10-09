#pragma once

#include <Core/NamesAndAliases.h>
#include <Access/Common/AccessRightsElement.h>
#include <Interpreters/IInterpreter.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

class ASTCreateQuery;
class ASTExpressionList;
class ASTConstraintDeclaration;
class ASTStorage;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;


/** Allows to create new table or database,
  *  or create an object for existing table or database.
  */
class InterpreterCreateQuery : public IInterpreter, WithMutableContext
{
public:
    /// proton : starts. There are 2 cases when calling this ctor
    /// 1. Creating system MergeTree tables which are always `local`
    /// 2. Creating MergeTree / StorageStream streams which will go through the
    ///    distributed stream provisioning process
    /// proton : ends
    InterpreterCreateQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    /// This ctor is for load existing streams
    InterpreterCreateQuery(const ASTPtr & query_ptr_, UInt32 schema_version_, ContextMutablePtr context_);

    BlockIO execute() override;

    /// List of columns and their types in AST.
    static ASTPtr formatColumns(const NamesAndTypesList & columns);
    static ASTPtr formatColumns(const NamesAndTypesList & columns, const NamesAndAliases & alias_columns);
    static ASTPtr formatColumns(const ColumnsDescription & columns);
    static ASTPtr formatIndices(const IndicesDescription & indices);
    static ASTPtr formatConstraints(const ConstraintsDescription & constraints);
    static ASTPtr formatProjections(const ProjectionsDescription & projections);

    void setForceRestoreData(bool has_force_restore_data_flag_)
    {
        has_force_restore_data_flag = has_force_restore_data_flag_;
    }

    void setInternal(bool internal_)
    {
        internal = internal_;
    }

    void setForceAttach(bool force_attach_)
    {
        force_attach = force_attach_;
    }

    void setLoadDatabaseWithoutTables(bool load_database_without_tables_)
    {
        load_database_without_tables = load_database_without_tables_;
    }

    /// Obtain information about columns, their types, default values and column comments,
    ///  for case when columns in CREATE query is specified explicitly.
    static ColumnsDescription getColumnsDescription(const ASTExpressionList & columns, ContextPtr context, bool attach);
    static ConstraintsDescription getConstraintsDescription(const ASTExpressionList * constraints);

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr) const override;

    /// proton : starts
    static void completeTableCreationAST(ASTCreateQuery & create, ContextMutablePtr local_context);
    /// proton : ends

private:
    struct TableProperties
    {
        ColumnsDescription columns;
        IndicesDescription indices;
        ConstraintsDescription constraints;
        ProjectionsDescription projections;
    };

    BlockIO createDatabase(ASTCreateQuery & create);
    BlockIO createTable(ASTCreateQuery & create);

    /// proton: start
    void handleExternalStreamCreation(ASTCreateQuery & create);
    BlockIO createDatabaseOnCluster(ASTCreateQuery & create);
    void createDatabaseEngineFromSettings(ASTStorage * storage);
    /// proton: end

    /// Calculate list of columns, constraints, indices, etc... of table. Rewrite query in canonical way.
    TableProperties getTablePropertiesAndNormalizeCreateQuery(ASTCreateQuery & create) const;
    void validateTableStructure(const ASTCreateQuery & create, const TableProperties & properties) const;
    void setEngine(ASTCreateQuery & create) const;
    AccessRightsElements getRequiredAccess() const;

    /// Create IStorage and add it to database. If table already exists and IF NOT EXISTS specified, do nothing and return false.
    bool doCreateTable(ASTCreateQuery & create, const TableProperties & properties);
    /// BlockIO doCreateOrReplaceTable(ASTCreateQuery & create, const InterpreterCreateQuery::TableProperties & properties);
    /// Inserts data in created table if it's CREATE ... SELECT
    BlockIO fillTableIfNeeded(const ASTCreateQuery & create);

    void assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database) const;

    /// Update create query with columns description from storage if query doesn't have it.
    /// It's used to prevent automatic schema inference while table creation on each server startup.
    void addColumnsDescriptionToCreateQueryIfNecessary(ASTCreateQuery & create, const StoragePtr & storage);

    ASTPtr query_ptr;

    /// Skip safety threshold when loading tables.
    bool has_force_restore_data_flag = false;
    /// Is this an internal query - not from the user.
    bool internal = false;
    bool force_attach = false;
    bool load_database_without_tables = false;

    mutable String as_database_saved;
    mutable String as_table_saved;

    /// proton : starts
    UInt32 schema_version = 1;
    LoggerPtr log;
    /// proton : ends
};
}
