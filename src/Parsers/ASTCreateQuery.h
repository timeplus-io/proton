#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTTableOverrides.h>
#include <Interpreters/StorageID.h>

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


    String getID(char) const override { return "Storage definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};


class ASTExpressionList;

class ASTColumns : public IAST
{
public:
    ASTExpressionList * columns = nullptr;
    ASTExpressionList * indices = nullptr;
    ASTExpressionList * constraints = nullptr;
    ASTExpressionList * projections = nullptr;
    IAST              * primary_key = nullptr;

    String getID(char) const override { return "Columns definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    bool empty()
    {
        return (!columns || columns->children.empty()) && (!indices || indices->children.empty()) && (!constraints || constraints->children.empty())
            && (!projections || projections->children.empty());
    }
};


/// CREATE STREAM or ATTACH STREAM query
class ASTCreateQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    bool attach{false};    /// Query ATTACH STREAM, not CREATE STREAM.
    bool if_not_exists{false};
    bool is_ordinary_view{false};
    /// proton: starts.
    bool is_materialized_view{false};
    bool is_virtual{false};
    bool is_random{false};
    /// proton: ends.
    bool replace_view{false}; /// CREATE OR REPLACE VIEW

    /// CREATE EXTERNAL STREAM
    bool is_external = false;

    ASTColumns * columns_list = nullptr;
    ASTExpressionList * tables = nullptr;

    StorageID to_table_id = StorageID::createEmpty();   /// For CREATE MATERIALIZED VIEW mv TO table.
    UUID to_inner_uuid = UUIDHelpers::Nil;      /// For materialized view with inner table
    ASTStorage * storage = nullptr;
    ASTPtr watermark_function;
    ASTPtr lateness_function;
    String as_database;
    String as_table;
    ASTPtr as_table_function;
    ASTSelectWithUnionQuery * select = nullptr;
    IAST * comment = nullptr;

    ASTTableOverrideList * table_overrides = nullptr; /// For CREATE DATABASE with engines that automatically create tables

    bool is_dictionary{false}; /// CREATE DICTIONARY
    ASTExpressionList * dictionary_attributes_list = nullptr; /// attributes of
    ASTDictionary * dictionary = nullptr; /// dictionary definition (layout, primary key, etc.)

    bool attach_short_syntax{false};

    std::optional<String> attach_from_path = std::nullopt;

    bool replace_table{false};
    bool create_or_replace{false};

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return (attach ? "AttachQuery" : "CreateQuery") + (delim + getDatabase()) + delim + getTable(); }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTCreateQuery>(clone(), new_database);
    }

    bool isView() const { return is_ordinary_view || is_materialized_view; }

    /// proton: starts. calculate and set 'is_virtual' for materialized view
    void calculateVirtual() { is_virtual = is_materialized_view && to_inner_uuid != UUIDHelpers::Nil && !attach; }
    /// proton: ends

    virtual QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
