#include "config_core.h"

#include <Interpreters/InterpreterExternalDDLQuery.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExternalDDLQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
}

InterpreterExternalDDLQuery::InterpreterExternalDDLQuery(const ASTPtr & query_, ContextMutablePtr context_)
    : WithMutableContext(context_), query(query_)
{
}

BlockIO InterpreterExternalDDLQuery::execute()
{
    return BlockIO();
}

}
