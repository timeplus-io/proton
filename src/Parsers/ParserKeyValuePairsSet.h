#pragma once

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc99-extensions"
#endif

namespace DB
{

/// Parser for list of key-value pairs.
class ParserKeyValuePairsSet : public IParserBase
{
protected:
    bool allow_duplicate = false;

    ParserPtr elem_parser = std::make_unique<ParserKeyValuePair>();
    ParserPtr separator_parser = std::make_unique<ParserNothing>();
    bool allow_empty = true;
    char result_separator = '\0';
    const char * getName() const override { return "set of pairs"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}

