#pragma once

#include <Parsers/IAST.h>

namespace DB
{
    String queryToString(const ASTPtr & query, bool one_line=false);
    String queryToString(const IAST & query, bool one_line=false);
}
