#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{
/// DELETE FROM [db.]name WHERE ...
class ASTDeleteQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    String getID(char delim) const final;
    ASTPtr clone() const final;
    ASTPtr getRewrittenASTWithoutOnCluster(const String & params) const override
    {
        return removeOnCluster<ASTDeleteQuery>(clone(), params);
    }

    ASTPtr predicate;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
