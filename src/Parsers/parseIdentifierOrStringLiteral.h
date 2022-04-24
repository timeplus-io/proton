#pragma once

#include <Core/Types.h>
#include <Parsers/IParser.h>

namespace DB
{

/** Parses a name of an object which could be written in 3 forms:
  * name, `name` or 'name' */
bool parseIdentifierOrStringLiteral(IParser::Pos & pos, Expected & expected, String & result, [[ maybe_unused ]] bool hint=true);

/** Parse a list of identifiers or string literals. */
bool parseIdentifiersOrStringLiterals(IParser::Pos & pos, Expected & expected, Strings & result, [[ maybe_unused ]] bool hint=true);

}
