#include <Parsers/TokenIterator.h>


namespace DB
{

UnmatchedParentheses checkUnmatchedParentheses(TokenIterator begin)
{
    /// We have just two kind of parentheses: () and [].
    UnmatchedParentheses stack;

    /// proton: starts.
    TokenIterator prev_it = begin;
    std::optional<TokenIterator> inside_seesion_it_opt;
    /// proton: ends.

    /// We have to iterate through all tokens until the end to avoid false positive "Unmatched parentheses" error
    /// when parser failed in the middle of the query.
    for (TokenIterator it = begin; it.isValid(); prev_it = it, ++it)
    {
        if (it->type == TokenType::OpeningRoundBracket || it->type == TokenType::OpeningSquareBracket)
        {
            stack.push_back(*it);

            /// proton: starts. Add mark for special case `session(...)`
            if (std::string(prev_it->begin, prev_it->end) == "session" && it->type == TokenType::OpeningRoundBracket)
                inside_seesion_it_opt = it;
            /// proton: ends.
        }
        else if (it->type == TokenType::ClosingRoundBracket || it->type == TokenType::ClosingSquareBracket)
        {
            if (stack.empty())
            {
                /// Excessive closing bracket.
                stack.push_back(*it);
                return stack;
            }
            else if ((stack.back().type == TokenType::OpeningRoundBracket && it->type == TokenType::ClosingRoundBracket)
                || (stack.back().type == TokenType::OpeningSquareBracket && it->type == TokenType::ClosingSquareBracket))
            {
                /// Valid match.
                stack.pop_back();
            }
            else
            {
                /// proton: starts. Allow special case `[...)` or `(...]` in table function session
                if (inside_seesion_it_opt.has_value() && stack.size() >= 2
                    && (*inside_seesion_it_opt)->begin == stack[stack.size() - 2].begin)
                {
                    stack.pop_back();
                    continue;
                }
                /// proton: ends.

                /// Closing bracket type doesn't match opening bracket type.
                stack.push_back(*it);
                return stack;
            }
        }
    }

    /// If stack is not empty, we have unclosed brackets.
    return stack;
}

}
