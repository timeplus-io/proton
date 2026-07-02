#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/quoteString.h>
#include <Interpreters/StorageID.h>
#include <IO/Operators.h>


namespace DB
{

/// proton : starts.
bool ASTStorage::hasLocalInSettings() const noexcept
{
    if (settings)
    {
        if (auto * local_f = settings->changes.tryGet("local"); local_f)
        {
            bool is_local = false;
            if (!local_f->tryGet(is_local))
            {
                /// SETTINGS local=1 is uint64_t
                uint64_t is_local_u64 = 0;
                if (local_f->tryGet(is_local_u64))
                    is_local = is_local_u64 > 0;
            }
            return is_local;
        }
    }
    return false;
}

bool ASTStorage::isLocal() const noexcept
{
    if (hasLocalInSettings())
        return true;

    if (engine)
    {
        if (engine->name == "Stream" || engine->name == "ExternalStream" || engine->name == "Random" || engine->name == "Null")
            return false;

        return true;
    }
    return false;
}

const char * mapCreateType(ASTCreateQuery::Type type, bool is_input)
{
    switch (type)
    {
        case ASTCreateQuery::Type::Database:
            return "DATABASE";
        case ASTCreateQuery::Type::Stream:
            return "STREAM";
        case ASTCreateQuery::Type::VersionedKeyValueStream:
            return "VERSIONED_KV STREAM";
        case ASTCreateQuery::Type::ChangelogKeyValueStream:
            return "CHANGELOG_KV STREAM";
        case ASTCreateQuery::Type::ChangelogStream:
            return "CHANGELOG STREAM";
        case ASTCreateQuery::Type::RandomStream:
            return "RANDOM STREAM";
        case ASTCreateQuery::Type::NullStream:
            return "NULL STREAM";
        case ASTCreateQuery::Type::ExternalStream:
            return is_input ? "INPUT" : "EXTERNAL STREAM";
        case ASTCreateQuery::Type::ExternalTable:
            return "EXTERNAL TABLE";
        case ASTCreateQuery::Type::Dictionary:
            return "DICTIONARY";
        case ASTCreateQuery::Type::View:
            return "VIEW";
        case ASTCreateQuery::Type::MaterializedView:
            return "MATERIALIZED VIEW";
        case ASTCreateQuery::Type::ScheduledMaterializedView:
            return "SCHEDULED MATERIALIZED VIEW";
    }
}

/// proton : ends

ASTPtr ASTStorage::clone() const
{
    auto res = std::make_shared<ASTStorage>(*this);
    res->children.clear();

    if (engine)
        res->set(res->engine, engine->clone());
    if (partition_by)
        res->set(res->partition_by, partition_by->clone());
    if (primary_key)
        res->set(res->primary_key, primary_key->clone());
    if (order_by)
        res->set(res->order_by, order_by->clone());
    if (sample_by)
        res->set(res->sample_by, sample_by->clone());
    if (ttl_table)
        res->set(res->ttl_table, ttl_table->clone());

    if (settings)
        res->set(res->settings, settings->clone());

    return res;
}

void ASTStorage::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (engine
        /*proton: starts*/ && engine->name != "ExternalTable" /*proton: ends*/)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ENGINE" << (s.hilite ? hilite_none : "") << " = ";
        engine->formatImpl(s, state, frame);
    }
    if (partition_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PARTITION BY " << (s.hilite ? hilite_none : "");
        partition_by->formatImpl(s, state, frame);
    }
    if (primary_key)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PRIMARY KEY " << (s.hilite ? hilite_none : "");
        primary_key->formatImpl(s, state, frame);
    }
    if (order_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ORDER BY " << (s.hilite ? hilite_none : "");
        order_by->formatImpl(s, state, frame);
    }
    if (sample_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SAMPLE BY " << (s.hilite ? hilite_none : "");
        sample_by->formatImpl(s, state, frame);
    }
    if (ttl_table)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "TTL " << (s.hilite ? hilite_none : "");
        ttl_table->formatImpl(s, state, frame);
    }
    if (settings && !settings->isEmpty()) /// proton: if setting is empty, skip format it. Empty settings cause syntax error
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings->formatImpl(s, state, frame);
    }
}

bool ASTStorage::isExtendedStorageDefinition() const
{
    return partition_by || primary_key || order_by || sample_by || settings;
}


class ASTColumnsElement : public IAST
{
public:
    String prefix;
    IAST * elem;

    String getID(char c) const override { return "ASTColumnsElement for " + elem->getID(c); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

ASTPtr ASTColumnsElement::clone() const
{
    auto res = std::make_shared<ASTColumnsElement>();
    res->prefix = prefix;
    if (elem)
        res->set(res->elem, elem->clone());
    return res;
}

void ASTColumnsElement::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (!elem)
        return;

    if (prefix.empty())
    {
        elem->formatImpl(s, state, frame);
        return;
    }

    s.ostr << (s.hilite ? hilite_keyword : "") << prefix << (s.hilite ? hilite_none : "");
    s.ostr << ' ';
    elem->formatImpl(s, state, frame);
}


ASTPtr ASTColumns::clone() const
{
    auto res = std::make_shared<ASTColumns>();

    if (columns)
        res->set(res->columns, columns->clone());
    if (indices)
        res->set(res->indices, indices->clone());
    if (constraints)
        res->set(res->constraints, constraints->clone());
    if (projections)
        res->set(res->projections, projections->clone());
    if (primary_key)
        res->set(res->primary_key, primary_key->clone());

    return res;
}

void ASTColumns::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    ASTExpressionList list;

    if (columns)
    {
        for (const auto & column : columns->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "";
            elem->set(elem->elem, column->clone());
            list.children.push_back(elem);
        }
    }
    if (indices)
    {
        for (const auto & index : indices->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "INDEX";
            elem->set(elem->elem, index->clone());
            list.children.push_back(elem);
        }
    }
    if (constraints)
    {
        for (const auto & constraint : constraints->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "CONSTRAINT";
            elem->set(elem->elem, constraint->clone());
            list.children.push_back(elem);
        }
    }
    if (projections)
    {
        for (const auto & projection : projections->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "PROJECTION";
            elem->set(elem->elem, projection->clone());
            list.children.push_back(elem);
        }
    }

    if (!list.children.empty())
    {
        if (s.one_line)
            list.formatImpl(s, state, frame);
        else
            list.formatImplMultiline(s, state, frame);
    }
}


ASTPtr ASTCreateQuery::clone() const
{
    auto res = std::make_shared<ASTCreateQuery>(*this);
    res->children.clear();

    if (columns_list)
        res->set(res->columns_list, columns_list->clone());
    if (storage)
        res->set(res->storage, storage->clone());
    if (select)
        res->set(res->select, select->clone());
    if (tables)
        res->set(res->tables, tables->clone());
    if (table_overrides)
        res->set(res->table_overrides, table_overrides->clone());

    if (dictionary)
    {
        assert(isDictionary());
        res->set(res->dictionary_attributes_list, dictionary_attributes_list->clone());
        res->set(res->dictionary, dictionary->clone());
    }

    if (comment)
        res->set(res->comment, comment->clone());

    cloneOutputOptions(*res);
    cloneTableOptions(*res);

    return res;
}

void ASTCreateQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    if (database && !table)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
            << (attach ? "ATTACH DATABASE " : "CREATE DATABASE ")
            << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "")
            << backQuoteIfNeed(getDatabase());

        if (uuid != UUIDHelpers::Nil)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));
        }

        formatOnCluster(settings);

        if (storage)
            storage->formatImpl(settings, state, frame);

        if (table_overrides)
        {
            settings.ostr << settings.nl_or_ws;
            table_overrides->formatImpl(settings, state, frame);
        }

        if (comment)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "COMMENT " << (settings.hilite ? hilite_none : "");
            comment->formatImpl(settings, state, frame);
        }

        return;
    }

    if (!isDictionary())
    {
        String action = "CREATE";
        if (attach)
            action = "ATTACH";
        else if (replace_view)
            action = "CREATE OR REPLACE";
        /// proton : starts
        /// else if (replace_table && create_or_replace)
        ///    action = "CREATE OR REPLACE";
        /// else if (replace_table)
        ///    action = "REPLACE";
        /// proton : ends

        /// proton: starts.
        const auto * what = mapCreateType(type, is_input);

        settings.ostr
            << (settings.hilite ? hilite_keyword : "")
                << action << " "
                << (temporary ? "TEMPORARY " : "")
                << what << " "
                << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "")
            << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

        if (uuid != UUIDHelpers::Nil)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));

        assert(attach || !attach_from_path);
        if (attach_from_path)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "")
                          << quoteString(*attach_from_path);

        formatOnCluster(settings);
    }
    else
    {
        String action = "CREATE";
        if (attach)
            action = "ATTACH";
        /// proton : starts
        /// else if (replace_table && create_or_replace)
        ///    action = "CREATE OR REPLACE";
        /// else if (replace_table)
        ///    action = "REPLACE";
        /// proton : ends

        /// Always DICTIONARY
        settings.ostr << (settings.hilite ? hilite_keyword : "") << action << " DICTIONARY "
                      << (if_not_exists ? "IF NOT EXISTS " : "") << (settings.hilite ? hilite_none : "")
                      << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());
        if (uuid != UUIDHelpers::Nil)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));
        formatOnCluster(settings);
    }

    if (to_table_id)
    {
        assert(isMaterializedView() && to_inner_uuid == UUIDHelpers::Nil);
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << (isMaterializedView() ? " INTO " : " TO ") << (settings.hilite ? hilite_none : "")
            << (!to_table_id.database_name.empty() ? backQuoteIfNeed(to_table_id.database_name) + "." : "")
            << backQuoteIfNeed(to_table_id.table_name);
    }

    if (to_inner_uuid != UUIDHelpers::Nil)
    {
        /// proton: starts.
        assert(isMaterializedView() && !to_table_id);
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (isMaterializedView() ? " INTO INNER UUID " : " TO INNER UUID ") << (settings.hilite ? hilite_none : "")
                      << quoteString(toString(to_inner_uuid));
        /// proton: ends.
    }

    if (!as_table.empty())
    {
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "")
            << (!as_database.empty() ? backQuoteIfNeed(as_database) + "." : "") << backQuoteIfNeed(as_table);
    }

    if (as_table_function)
    {
        if (columns_list && !columns_list->empty())
        {
            frame.expression_list_always_start_on_new_line = true;
            settings.ostr << (settings.one_line ? " (" : "\n(");
            FormatStateStacked frame_nested = frame;
            columns_list->formatImpl(settings, state, frame_nested);
            settings.ostr << (settings.one_line ? ")" : "\n)");
            frame.expression_list_always_start_on_new_line = false; //-V519
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
        as_table_function->formatImpl(settings, state, frame);
    }

    frame.expression_list_always_start_on_new_line = true;

    if (columns_list && !columns_list->empty() && !as_table_function)
    {
        settings.ostr << (settings.one_line ? " (" : "\n(");
        FormatStateStacked frame_nested = frame;
        columns_list->formatImpl(settings, state, frame_nested);
        settings.ostr << (settings.one_line ? ")" : "\n)");
    }

    if (dictionary_attributes_list)
    {
        settings.ostr << (settings.one_line ? " (" : "\n(");
        FormatStateStacked frame_nested = frame;
        if (settings.one_line)
            dictionary_attributes_list->formatImpl(settings, state, frame_nested);
        else
            dictionary_attributes_list->formatImplMultiline(settings, state, frame_nested);
        settings.ostr << (settings.one_line ? ")" : "\n)");
    }

    frame.expression_list_always_start_on_new_line = false; //-V519

    ASTPtr storage_to_format;
    if (storage)
        storage_to_format = storage->clone();

    if (exec_script)
        /// Emit the script verbatim between the $$ markers: adding newlines here makes
        /// the statement grow a blank line inside the body on every parse/format
        /// round trip (e.g. each ALTER STREAM ... MODIFY rewrites the metadata).
        settings.ostr << settings.nl_or_ws << "AS $$" << *exec_script << "$$";

    if (storage_to_format)
        storage_to_format->formatImpl(settings, state, frame);

    if (dictionary)
        dictionary->formatImpl(settings, state, frame);

    if (select)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS"
                      << (comment ? "(" : "")
                      << settings.nl_or_ws << (settings.hilite ? hilite_none : "");
        select->formatImpl(settings, state, frame);
        settings.ostr << (comment ? ")" : "");
    }

    if (tables)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH " << (settings.hilite ? hilite_none : "");
        tables->formatImpl(settings, state, frame);
    }

    /// proton : starts
    if (storage_settings)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "STORAGE_SETTINGS " << (settings.hilite ? hilite_none : "");
        storage_settings->formatImpl(settings, state, frame);
    }

    if (mv_inner_storage_ttl)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "TTL " << (settings.hilite ? hilite_none : "");
        mv_inner_storage_ttl->formatImpl(settings, state, frame);
    }
    /// proton : ends

    if (comment)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "COMMENT " << (settings.hilite ? hilite_none : "");
        comment->formatImpl(settings, state, frame);
    }
}

/// proton : starts
bool ASTCreateQuery::isLocal() const noexcept
{
    if (table)
    {
        return storage && storage->isLocal();
    }
    else
    {
        /// Database with `local` flag is "forced" local
        auto is_local_database = storage && storage->hasLocalInSettings();
        if (is_local_database)
            return true;

        /// `system` database is always local
        if (getDatabase() == "system")
            return true;

        /// For all other databases, they are all distributed
        return false;
    }
}
/// proton : ends

bool ASTCreateQuery::isParameterizedView() const
{
    if (isOrdinaryView() && select && select->hasQueryParameters())
        return true;
    return false;
}

}
