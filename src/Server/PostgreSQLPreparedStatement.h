#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <base/types.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct PreparedStatement
{
    String query;
    std::vector<Int32> param_oids;
    size_t param_count = 0;
};

struct Portal
{
    String statement_name;
    String bound_query;
    std::vector<Int16> result_formats;
};

/// Manages named and unnamed prepared statements and portals for a single
/// PostgreSQL connection. Each connection gets its own manager instance.
class PreparedStatementManager
{
public:
    PreparedStatementManager() = default;

    void parseStatement(const String & name, const String & query, const std::vector<Int32> & param_oids)
    {
        PreparedStatement stmt;
        stmt.query = query;
        stmt.param_oids = param_oids;
        stmt.param_count = countParameters(query);

        const size_t pc = stmt.param_count;
        const String log_query = query;

        if (name.empty())
            unnamed_statement = std::move(stmt);
        else
            named_statements[name] = std::move(stmt);

        LOG_DEBUG(log, "Parsed statement '{}': {} params, query: {}",
                  name.empty() ? "<unnamed>" : name, pc, log_query);
    }

    void bindPortal(
        const String & portal_name,
        const String & stmt_name,
        const std::vector<std::optional<String>> & param_values,
        const std::vector<Int16> & result_formats)
    {
        const auto * stmt = getStatement(stmt_name);
        if (!stmt)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Prepared statement '{}' does not exist", stmt_name);

        Portal portal;
        portal.statement_name = stmt_name;
        portal.bound_query = substituteParams(stmt->query, param_values);
        portal.result_formats = result_formats;

        const String log_bound_query = portal.bound_query;

        if (portal_name.empty())
            unnamed_portal = std::move(portal);
        else
            named_portals[portal_name] = std::move(portal);

        LOG_DEBUG(log, "Bound portal '{}' from statement '{}': {}",
                  portal_name.empty() ? "<unnamed>" : portal_name,
                  stmt_name.empty() ? "<unnamed>" : stmt_name,
                  log_bound_query);
    }

    const PreparedStatement * getStatement(const String & name) const
    {
        if (name.empty())
            return unnamed_statement ? &*unnamed_statement : nullptr;

        auto it = named_statements.find(name);
        return it != named_statements.end() ? &it->second : nullptr;
    }

    const Portal * getPortal(const String & name) const
    {
        if (name.empty())
            return unnamed_portal ? &*unnamed_portal : nullptr;

        auto it = named_portals.find(name);
        return it != named_portals.end() ? &it->second : nullptr;
    }

    void closeStatement(const String & name)
    {
        if (name.empty())
            unnamed_statement.reset();
        else
            named_statements.erase(name);
    }

    void closePortal(const String & name)
    {
        if (name.empty())
            unnamed_portal.reset();
        else
            named_portals.erase(name);
    }

    void clearAll()
    {
        named_statements.clear();
        named_portals.clear();
        unnamed_statement.reset();
        unnamed_portal.reset();
    }

private:
    LoggerPtr log = getLogger("PreparedStatementManager");

    std::optional<PreparedStatement> unnamed_statement;
    std::optional<Portal> unnamed_portal;
    std::unordered_map<String, PreparedStatement> named_statements;
    std::unordered_map<String, Portal> named_portals;

    static size_t countParameters(const String & query)
    {
        size_t max_param = 0;
        for (size_t i = 0; i < query.size(); ++i)
        {
            if (query[i] == '$' && i + 1 < query.size() && std::isdigit(query[i + 1]))
            {
                size_t num = 0;
                size_t j = i + 1;
                while (j < query.size() && std::isdigit(query[j]))
                {
                    num = num * 10 + (query[j] - '0');
                    ++j;
                }
                if (num > max_param)
                    max_param = num;
            }
        }
        return max_param;
    }

    static String substituteParams(const String & query, const std::vector<std::optional<String>> & values)
    {
        String result;
        result.reserve(query.size() * 2);

        bool in_string_literal = false;

        for (size_t i = 0; i < query.size(); ++i)
        {
            if (query[i] == '\'' && !in_string_literal)
            {
                in_string_literal = true;
                result += query[i];
                continue;
            }
            if (query[i] == '\'' && in_string_literal)
            {
                if (i + 1 < query.size() && query[i + 1] == '\'')
                {
                    result += "''";
                    ++i;
                    continue;
                }
                in_string_literal = false;
                result += query[i];
                continue;
            }

            if (!in_string_literal && query[i] == '$' && i + 1 < query.size() && std::isdigit(query[i + 1]))
            {
                size_t num = 0;
                size_t j = i + 1;
                while (j < query.size() && std::isdigit(query[j]))
                {
                    num = num * 10 + (query[j] - '0');
                    ++j;
                }

                if (num >= 1 && num <= values.size())
                {
                    const auto & val = values[num - 1];
                    if (!val.has_value())
                    {
                        result += "NULL";
                    }
                    else
                    {
                        result += '\'';
                        for (char c : *val)
                        {
                            if (c == '\'')
                                result += "''";
                            else if (c == '\\')
                                result += "\\\\";
                            else
                                result += c;
                        }
                        result += '\'';
                    }
                    i = j - 1;  // skip past the $N
                    continue;
                }
            }
            result += query[i];
        }
        return result;
    }
};

}
