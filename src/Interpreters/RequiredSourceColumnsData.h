#pragma once

#include <ostream>
#include <optional>

#include <Core/Names.h>
#include <base/types.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTIdentifier;

/// Information about table and column names extracted from ASTSelectQuery block. Do not include info from subselects.
struct RequiredSourceColumnsData
{
    struct NameInfo
    {
        std::set<String> aliases;
        size_t appears = 0;

        void addInclusion(const String & alias)
        {
            if (!alias.empty())
                aliases.insert(alias);
            ++appears;
        }
    };

    RequiredSourceColumnsData() { }

    /// proton: starts
    bool streaming = true;
    /// proton: ends

    std::unordered_map<String, NameInfo> required_names;
    NameSet private_aliases; /// lambda aliases that should not be interpreted as required columns
    NameSet complex_aliases; /// aliases to functions results: they are not required cause calculated by query itself
    NameSet masked_columns;  /// columns names masked by function aliases: we still need them in required columns
    NameSet array_join_columns; /// Tech debt: we exclude ArrayJoin columns from general logic cause they have own logic outside

    bool has_table_join = false;
    bool has_array_join = false;

    bool addColumnAliasIfAny(const IAST & ast);
    void addColumnIdentifier(const ASTIdentifier & node);
    bool addArrayJoinAliasIfAny(const IAST & ast);
    void addArrayJoinIdentifier(const ASTIdentifier & node);

    NameSet requiredColumns() const;
    size_t nameInclusion(const String & name) const;

    /// proton: starts
    bool needsScanAgain() const
    {
        if (complex_aliases.empty())
            return false;

        for (const auto & alias : complex_aliases)
        {
            if (!masked_columns.contains(alias) && required_names.contains(alias))
                return true;
        }
        return false;
    }
    /// proton: ends
};

std::ostream & operator << (std::ostream & os, const RequiredSourceColumnsData & cols);

}
