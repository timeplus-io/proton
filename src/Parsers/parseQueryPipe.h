#pragma once

#include <Parsers/IParser.h>

namespace DB
{
std::pair<std::string, ASTPtr> rewriteQueryPipeAndParse(
    IParser & parser,
    const char * pos,
    const char * end,
    std::string & error_message,
    bool hilite,
    size_t max_query_size,
    size_t max_parser_depth);

ASTPtr tryParseQueryPipe(
    IParser & parser,
    const char * pos,
    const char * end,
    std::string & error_message,
    bool hilite,
    size_t max_query_size,
    size_t max_parser_depth);

ASTPtr parseQueryPipe(
    IParser & parser,
    const char * pos,
    const char * end,
    size_t max_query_size,
    size_t max_parser_depth);
}
