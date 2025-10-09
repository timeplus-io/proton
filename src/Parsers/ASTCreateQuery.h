#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTTableOverrides.h>

namespace DB
{

class ASTFunction;
class ASTSetQuery;
class ASTSelectWithUnionQuery;

class ASTStorage : public IAST
{
public:
    ASTFunction * engine = nullptr;
    IAST * partition_by = nullptr;
    IAST * primary_key = nullptr;
    IAST * order_by = nullptr;
    IAST * sample_by = nullptr;
    IAST * ttl_table = nullptr;
    ASTSetQuery * settings = nullptr;

    bool isLocal() const noexcept;
    bool hasLocalInSettings() const noexcept;

    String getID(char) const override { return "Storage definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    bool isExtendedStorageDefinition() const;
};


class ASTExpressionList;

class ASTColumns : public IAST
{
public:
    ASTExpressionList * columns = nullptr;
    ASTExpressionList * indices = nullptr;
    ASTExpressionList * constraints = nullptr;
    ASTExpressionList * projections = nullptr;
    IAST * primary_key = nullptr;

    String getID(char) const override { return "Columns definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    bool empty()
    {
        return (!columns || columns->children.empty()) && (!indices || indices->children.empty())
            && (!constraints || constraints->children.empty()) && (!projections || projections->children.empty());
    }
};


/// CREATE STREAM or ATTACH STREAM query
class ASTCreateQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    bool attach{false}; /// Query ATTACH STREAM, not CREATE STREAM.
    bool if_not_exists{false};
    /// proton: starts.
    bool is_virtual{false};
    bool isLocal() const noexcept;
    /// proton: ends.
    bool replace_view{false}; /// CREATE OR REPLACE VIEW

    /// proton : starts
    enum class Type : uint8_t
    {
        Database = 0,

        Stream = 1,
        VersionedKeyValueStream = 3,
        ChangelogKeyValueStream = 4,
        ChangelogStream = 5,
        RandomStream = 6,
        NullStream = 7,
        ExternalStream = 8,
        ExternalTable = 9,
        Dictionary = 10,

        View = 20,
        MaterializedView = 21,
        ScheduledMaterializedView = 22,
    };
    Type type = Type::Stream;

    ASTSetQuery * storage_settings;
    ASTPtr mv_inner_storage_ttl;
    /// proton : ends

    ASTColumns * columns_list = nullptr;
    ASTExpressionList * tables = nullptr;

    StorageID to_table_id = StorageID::createEmpty(); /// For CREATE MATERIALIZED VIEW mv TO table.
    UUID to_inner_uuid = UUIDHelpers::Nil; /// For materialized view with inner table
    ASTStorage * storage = nullptr;
    ASTPtr watermark_function;
    ASTPtr lateness_function;
    String as_database;
    String as_table;
    ASTPtr as_table_function;
    ASTSelectWithUnionQuery * select = nullptr;
    IAST * comment = nullptr;

    ASTTableOverrideList * table_overrides = nullptr; /// For CREATE DATABASE with engines that automatically create tables

    ASTExpressionList * dictionary_attributes_list = nullptr; /// attributes of
    ASTDictionary * dictionary = nullptr; /// dictionary definition (layout, primary key, etc.)

    bool attach_short_syntax{false};

    std::optional<String> attach_from_path = std::nullopt;

    /// proton : starts, we don't support replace / create or replace table for now
    /// bool replace_table{false};
    /// bool create_or_replace{false};
    /// proton : ends

    /** Get the text that identifies this element. */
    String getID(char delim) const override
    {
        return (attach ? "AttachQuery" : "CreateQuery") + (delim + getDatabase()) + delim + getTable();
    }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTCreateQuery>(clone(), new_database);
    }

    /// proton : starts
    bool isView() const noexcept { return isOrdinaryView() || isMaterializedView(); }
    bool isOrdinaryView() const noexcept { return type == Type::View; }
    bool isMaterializedView() const noexcept { return isOrdinaryMaterializedView() || isScheduledMaterializedView(); }
    bool isOrdinaryMaterializedView() const noexcept { return type == Type::MaterializedView; }
    bool isScheduledMaterializedView() const noexcept { return type == Type::ScheduledMaterializedView; }
    bool isRandomStream() const noexcept { return type == Type::RandomStream; }
    bool isNullStream() const noexcept { return type == Type::NullStream; }
    bool isDatabase() const noexcept { return type == Type::Database; }
    bool isExternal() const noexcept { return isExternalStream() || isExternalTable(); }
    bool isExternalStream() const noexcept { return type == Type::ExternalStream; }
    bool isExternalTable() const noexcept { return type == Type::ExternalTable; }
    bool isDictionary() const noexcept { return type == Type::Dictionary; }
    /// proton : ends

    bool isParameterizedView() const;

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
