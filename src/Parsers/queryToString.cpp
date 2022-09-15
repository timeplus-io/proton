#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>

namespace DB
{
    String queryToString(const ASTPtr & query, bool one_line)
    {
        return queryToString(*query, one_line);
    }

    String queryToString(const IAST & query, bool one_line)
    {
        return serializeAST(query, one_line);
    }
}
