#include <Parsers/ParserKeyValuePairsSet.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/ErrorCodes.h>
#include <unordered_map>



namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_KEY;
}

bool ParserKeyValuePairsSet::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ASTs elements;
    std::unordered_map<String, size_t> exists_keys;

    auto parse_element = [&]
    {
        ASTPtr element;
        if (!elem_parser->parse(pos, element, expected))
            return false;
        auto key_value = element->as<ASTPair>();
        if (exists_keys.find(key_value->first) == exists_keys.end())
        {
            exists_keys[key_value->first] = elements.size();
            elements.push_back(element);
        }
        else
        {
            if (allow_duplicate)
            {
                size_t index = exists_keys[key_value->first];
                elements.erase(elements.begin() + index);
                exists_keys[key_value->first] = elements.size();
                elements.push_back(element);
            }
            else
            {
                throw Exception("Duplicate key \"" + key_value->first + "\" has existed previously", ErrorCodes::DUPLICATE_KEY);
            }
        }
        
        return true;
    };

    if (!ParserList::parseUtil(pos, expected, parse_element, *separator_parser, allow_empty))
        return false;

    auto list = std::make_shared<ASTExpressionList>(result_separator);
    list->children = std::move(elements);
    node = list;

    return true;
}

}

