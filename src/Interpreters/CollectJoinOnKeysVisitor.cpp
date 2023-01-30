#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryToString.h>

#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/TableJoin.h>

/// proton : starts
#include <Parsers/ASTLiteral.h>
/// proton : ends

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int AMBIGUOUS_COLUMN_NAME;
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int BAD_QUERY_PARAMETER;
}

namespace
{

bool isLeftIdentifier(JoinIdentifierPos pos)
{
    /// Unknown identifiers  considered as left, we will try to process it on later stages
    /// Usually such identifiers came from `ARRAY JOIN ... AS ...`
    return pos == JoinIdentifierPos::Left || pos == JoinIdentifierPos::Unknown;
}

bool isRightIdentifier(JoinIdentifierPos pos)
{
    return pos == JoinIdentifierPos::Right;
}

bool isSupportedRangeAsofJoinFunctions(const String & func_name)
{
    return (func_name == "minus") || (func_name == "date_diff");
}
}

void CollectJoinOnKeysMatcher::Data::addJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast, JoinIdentifierPosPair table_pos)
{
    ASTPtr left = left_ast->clone();
    ASTPtr right = right_ast->clone();

    if (isLeftIdentifier(table_pos.first) && isRightIdentifier(table_pos.second))
        analyzed_join.addOnKeys(left, right);
    else if (isRightIdentifier(table_pos.first) && isLeftIdentifier(table_pos.second))
        analyzed_join.addOnKeys(right, left);
    else
        throw Exception("Cannot detect left and right JOIN keys. JOIN ON section is ambiguous.",
                        ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
}

void CollectJoinOnKeysMatcher::Data::addAsofJoinKeys(const ASTPtr & left_ast, const ASTPtr & right_ast,
                                                     JoinIdentifierPosPair table_pos, const ASOFJoinInequality & inequality)
{
    if (isLeftIdentifier(table_pos.first) && isRightIdentifier(table_pos.second))
    {
        asof_left_key = left_ast->clone();
        asof_right_key = right_ast->clone();
        analyzed_join.setAsofInequality(inequality);
    }
    else if (isRightIdentifier(table_pos.first) && isLeftIdentifier(table_pos.second))
    {
        asof_left_key = right_ast->clone();
        asof_right_key = left_ast->clone();
        analyzed_join.setAsofInequality(reverseASOFJoinInequality(inequality));
    }
    else
    {
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                        "Expressions {} and {} are from the same stream but from different arguments of equal function in ASOF JOIN",
                        queryToString(left_ast), queryToString(right_ast));
    }
}

void CollectJoinOnKeysMatcher::Data::asofToJoinKeys()
{
    if (!asof_left_key || !asof_right_key)
        throw Exception("No inequality in ASOF JOIN ON section.", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
    addJoinKeys(asof_left_key, asof_right_key, {JoinIdentifierPos::Left, JoinIdentifierPos::Right});
}

void CollectJoinOnKeysMatcher::visit(const ASTIdentifier & ident, const ASTPtr & ast, CollectJoinOnKeysMatcher::Data & data)
{
    if (auto expr_from_table = getTableForIdentifiers(ast, false, data); expr_from_table != JoinIdentifierPos::Unknown)
        data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(expr_from_table));
    else
        throw Exception("Unexpected identifier '" + ident.name() + "' in JOIN ON section",
                        ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
}

void CollectJoinOnKeysMatcher::visit(const ASTFunction & func, const ASTPtr & ast, Data & data)
{
    if (func.name == "and")
        return; /// go into children

    ASOFJoinInequality inequality = getASOFJoinInequality(func.name);

    /// proton : starts
    if (inequality == ASOFJoinInequality::RangeBetween)
    {
        handleRangeBetweenAsOfJoin(func, ast, data);
        return;
    }
    /// proton : ends

    if (func.name == "equals" || inequality != ASOFJoinInequality::None)
    {
        if (func.arguments->children.size() != 2)
            throw Exception("Function " + func.name + " takes two arguments, got '" + func.formatForErrorMessage() + "' instead",
                            ErrorCodes::SYNTAX_ERROR);
    }

    if (func.name == "equals")
    {
        ASTPtr left = func.arguments->children.at(0);
        ASTPtr right = func.arguments->children.at(1);
        auto table_numbers = getTableNumbers(left, right, data);
        if (table_numbers.first == table_numbers.second)
        {
            if (table_numbers.first == JoinIdentifierPos::Unknown)
                throw Exception("Ambiguous column in expression '" + queryToString(ast) + "' in JOIN ON section",
                                ErrorCodes::AMBIGUOUS_COLUMN_NAME);
            data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(table_numbers.first));
            return;
        }

        if (table_numbers.first != JoinIdentifierPos::NotApplicable && table_numbers.second != JoinIdentifierPos::NotApplicable)
        {
            data.addJoinKeys(left, right, table_numbers);
            return;
        }
    }

    /// proton : starts
    /// Deals with `filter on join clause` cases:
    /// SELECT ... FROM left JOIN right ON left.i = right.ii AND left.s = 10 => Non-asof
    /// SELECT ... FROM left JOIN right ON left.i = right.ii AND left.s > 10 => Non-asof
    /// SELECT ... FROM left JOIN right ON left.i = right.ii AND right.ss > 10 => Non-asof
    if (auto expr_from_table = getTableForIdentifiers(ast, false, data); expr_from_table != JoinIdentifierPos::Unknown)
    {
        data.analyzed_join.addJoinCondition(ast, isLeftIdentifier(expr_from_table));
        return;
    }

    /// When code reaches here, the function is either an inequality function like >, >=, <, <= etc
    /// or it is a normal function like only `minus` and `date_diff` are supported in range asof JOIN ON clause
    /// Examples :
    /// left.col - right.col between 0 and 10
    /// left.col - right.col between -10s and 10s
    /// date_diff('seconds', left.col, right.col) between -10 and 10
    if (isSupportedRangeAsofJoinFunctions(func.name))
        return;

    /// When logic reaches here, it means there are 2 columns from left / right table in the in the inequality join clause
    /// Support RangeBetween asof and other asof. We don't require end user explicitly specify `ASOF`

    /// What about both left and right are literals `ON 3 > 1`
    if (handleRangeBetweenAsOfJoinGeneral(func, data))
        return;

    /// For normal asof join, left argument and right argument shall neither be constant
    /// The following code handle case : SELECT ... FROM left ASOF JOIN right ON left.i = right.ii AND left.s > right.ss => Greater asof
    if (data.is_asof)
    {
        /// Normal asof join or malformed range asof join
        if (data.asof_left_key || data.asof_right_key)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF JOIN expects exactly one inequality in ON section. Unexpected '{}'",
                            queryToString(ast));

        ASTPtr left = func.arguments->children.at(0);
        ASTPtr right = func.arguments->children.at(1);
        auto table_numbers = getTableNumbers(left, right, data);

        data.addAsofJoinKeys(left, right, table_numbers, inequality);
        return;
    }
    /// proton : ends

    throw Exception("Unsupported JOIN ON conditions. Unexpected '" + queryToString(ast) + "'",
                    ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
}

void CollectJoinOnKeysMatcher::getIdentifiers(const ASTPtr & ast, std::vector<const ASTIdentifier *> & out)
{
    if (const auto * func = ast->as<ASTFunction>())
    {
        if (func->name == "array_join")
            throw Exception("Not allowed function in JOIN ON. Unexpected '" + queryToString(ast) + "'",
                            ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
    }
    else if (const auto * ident = ast->as<ASTIdentifier>())
    {
        if (IdentifierSemantic::getColumnName(*ident))
            out.push_back(ident);
        return;
    }

    for (const auto & child : ast->children)
        getIdentifiers(child, out);
}

JoinIdentifierPosPair CollectJoinOnKeysMatcher::getTableNumbers(const ASTPtr & left_ast, const ASTPtr & right_ast, Data & data)
{
    auto left_idents_table = getTableForIdentifiers(left_ast, true, data);
    auto right_idents_table = getTableForIdentifiers(right_ast, true, data);

    return std::make_pair(left_idents_table, right_idents_table);
}

const ASTIdentifier * CollectJoinOnKeysMatcher::unrollAliases(const ASTIdentifier * identifier, const Aliases & aliases)
{
    if (identifier->supposedToBeCompound())
        return identifier;

    UInt32 max_attempts = 100;
    for (auto it = aliases.find(identifier->name()); it != aliases.end();)
    {
        const ASTIdentifier * parent = identifier;
        identifier = it->second->as<ASTIdentifier>();
        if (!identifier)
            break; /// not a column alias
        if (identifier == parent)
            break; /// alias to itself with the same name: 'a as a'
        if (identifier->supposedToBeCompound())
            break; /// not an alias. Break to prevent cycle through short names: 'a as b, t1.b as a'

        it = aliases.find(identifier->name());
        if (!max_attempts--)
            throw Exception("Cannot unroll aliases for '" + identifier->name() + "'", ErrorCodes::LOGICAL_ERROR);
    }

    return identifier;
}

/// @returns Left or right table identifiers belongs to.
/// Place detected identifier into identifiers[0] if any.
JoinIdentifierPos CollectJoinOnKeysMatcher::getTableForIdentifiers(const ASTPtr & ast, bool throw_on_table_mix, const Data & data)
{
    std::vector<const ASTIdentifier *> identifiers;
    getIdentifiers(ast, identifiers);
    if (identifiers.empty())
        return JoinIdentifierPos::NotApplicable;

    JoinIdentifierPos table_number = JoinIdentifierPos::Unknown;

    for (auto & ident : identifiers)
    {
        const ASTIdentifier * identifier = unrollAliases(ident, data.aliases);
        if (!identifier)
            continue;

        /// Column name could be cropped to a short form in TranslateQualifiedNamesVisitor.
        /// In this case it saves membership in IdentifierSemantic.
        JoinIdentifierPos membership = JoinIdentifierPos::Unknown;
        if (auto opt = IdentifierSemantic::getMembership(*identifier); opt.has_value())
        {
            if (*opt == 0)
                membership = JoinIdentifierPos::Left;
            else if (*opt == 1)
                membership = JoinIdentifierPos::Right;
            else
                throw DB::Exception(ErrorCodes::AMBIGUOUS_COLUMN_NAME,
                                    "Position of identifier {} can't be deteminated.",
                                    identifier->name());
        }

        if (membership == JoinIdentifierPos::Unknown)
        {
            const String & name = identifier->name();
            bool in_left_table = data.left_table.hasColumn(name);
            bool in_right_table = data.right_table.hasColumn(name);

            if (in_left_table && in_right_table)
            {
                /// Relax ambiguous check for multiple JOINs
                if (auto original_name = IdentifierSemantic::uncover(*identifier))
                {
                    auto match = IdentifierSemantic::canReferColumnToTable(*original_name, data.right_table.table);
                    if (match == IdentifierSemantic::ColumnMatch::NoMatch)
                        in_right_table = false;
                    in_left_table = !in_right_table;
                }
                else
                    throw Exception("Column '" + name + "' is ambiguous", ErrorCodes::AMBIGUOUS_COLUMN_NAME);
            }

            if (in_left_table)
                membership = JoinIdentifierPos::Left;
            if (in_right_table)
                membership = JoinIdentifierPos::Right;
        }

        if (membership != JoinIdentifierPos::Unknown && table_number == JoinIdentifierPos::Unknown)
        {
            table_number = membership;
            std::swap(ident, identifiers[0]); /// move first detected identifier to the first position
        }

        if (membership != JoinIdentifierPos::Unknown && membership != table_number)
        {
            if (throw_on_table_mix)
                throw Exception("Invalid columns in JOIN ON section. Columns "
                            + identifiers[0]->getAliasOrColumnName() + " and " + ident->getAliasOrColumnName()
                            + " are from different tables.", ErrorCodes::INVALID_JOIN_ON_EXPRESSION);
            return JoinIdentifierPos::Unknown;
        }
    }

    return table_number;
}

/// proton : starts
void CollectJoinOnKeysMatcher::handleRangeBetweenAsOfJoin(const ASTFunction & func, const ASTPtr & ast, Data & data)
{
    assert(func.name == "date_diff_within");

    if (func.arguments->children.size() != 3)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Function {} takes 3 arguments, but got '{}' instead", func.name, func.formatForErrorMessage());

    /// date_diff_within(2m, col1, col2)
    ASTPtr range_interval = func.arguments->children.at(0);
    ASTPtr left = func.arguments->children.at(1);
    ASTPtr right = func.arguments->children.at(2);

    Int64 range = 0;

    if (auto * interval_func = range_interval->as<ASTFunction>())
    {
        if (interval_func->name == "to_interval_second")
            range = 1;
        else if (interval_func->name == "to_interval_minute")
            range = 60;
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "First argument of function {} only supports second or minute time interval (examples, 1s, 2m etc), but got '{}' instead",
                func.name, func.formatForErrorMessage());

        auto * interval = interval_func->arguments->children.at(0)->as<ASTLiteral>();
        assert(interval);
        range *= interval->value.get<Int64>();
        if (range <= 0)
            throw Exception(
                ErrorCodes::BAD_QUERY_PARAMETER,
                "First argument of function {} shall be positive second or minute time interval (examples, 1s, 2m etc), but got '{}' instead",
                func.name, func.formatForErrorMessage());
    }
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "First argument of function {} shall be a time interval (examples, 1s, 2m etc), but got '{}' instead", func.name, func.formatForErrorMessage());

    if (data.asof_left_key || data.asof_right_key)
        throw Exception(
            ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Multiple range joins are detected in ON section. Unexpected '{}'", queryToString(ast));

    auto table_numbers = getTableNumbers(left, right, data);
    data.addAsofJoinKeys(left, right, table_numbers, ASOFJoinInequality::RangeBetween);

    data.analyzed_join.setRangeAsofLowerBound(-range);
    data.analyzed_join.setRangeAsofUpperBound(range);
    data.analyzed_join.setRangeType(Streaming::RangeType::Interval);
    data.analyzed_join.setRangeAsofLeftInequality(ASOFJoinInequality::GreaterOrEquals);
    data.analyzed_join.setRangeAsofRightInequality(ASOFJoinInequality::LessOrEquals);

    data.range_analyze_finished = true;
}

bool CollectJoinOnKeysMatcher::handleRangeBetweenAsOfJoinGeneral(const ASTFunction & func, Data & data)
{
    auto & left = func.arguments->children.at(0);
    auto & right = func.arguments->children.at(1);

    auto * left_literal = left->as<ASTLiteral>();
    auto * right_literal = right->as<ASTLiteral>();

    /// For RangeBetween asof join, either left or right shall be a constant
    if ((!left_literal && !right_literal) || (left_literal && right_literal))
        return false;

    /// SELECT ... FROM left JOIN right ON left.i = right.ii AND date_diff(...) BETWEEN -10 AND 10 => RangeBetween asof
    /// SELECT ... FROM left JOIN right ON left.i = right.ii AND left.k - right.kk BETWEEN -10 AND 10 => RangeBetween asof, this seems difficult
    /// SELECT ... FROM left JOIN right ON left.i = right.ii AND (left.k - right.kk) > -10 AND (left.k - right.kk) < 10 => RangeBetween asof, this seems difficult

    ASOFJoinInequality inequality = getASOFJoinInequality(func.name);

    /// RangeBetween asof join. We don't support these kinds of range : x > y + 5 and x <= y + 10
    /// User needs rewrite it to `x - y > 5 and x - y < 10`. There are 2 parts of inequality in range asof join
    if (!data.asof_left_key)
    {
        /// The first part
        if (left_literal)
        {
            data.is_first_arg_left_identifier_of_range_func
                = handleLeftLiteralArgumentForRangeBetweenAsofJoin(left_literal, right, inequality, data);
            data.range_func = right;
        }
        else
        {
            data.is_first_arg_left_identifier_of_range_func
                = handleRightLiteralArgumentForRangeBetweenAsofJoin(right_literal, left, inequality, data);
            data.range_func = left;
        }
    }
    else
    {
        if (data.range_analyze_finished)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Found more than 2 unequal expressions in `ON` join clause for range join.");

        /// The second part of range asof join clause
        assert(data.asof_right_key);
        assert(data.range_func);

        if (left_literal)
        {
            /// The range asof function shall be the same
            if (data.range_func->getTreeHash() != right->getTreeHash())
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR,
                    "Range join requires same function expression in ON clause, but got two different expressions: '{}', '{}'",
                    queryToString(data.range_func),
                    queryToString(right));

            handleLeftLiteralArgumentForRangeBetweenAsofJoin(left_literal, right, inequality, data);
        }
        else
        {
            /// The range asof function shall be the same
            if (data.range_func->getTreeHash() != left->getTreeHash())
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR,
                    "Range join requires same function expression in ON clause, but got two different expressions: '{}', '{}'",
                    queryToString(data.range_func),
                    queryToString(left));

            handleRightLiteralArgumentForRangeBetweenAsofJoin(right_literal, left, inequality, data);
        }

        data.range_analyze_finished = true;
    }

    return true;
}

std::pair<Int64, bool> CollectJoinOnKeysMatcher::handleLeftAndRightArgumentsForRangeBetweenAsOfJoin(const ASTLiteral * literal_arg, ASTPtr non_literal_arg, Data & data)
{
    /// FIXME: we don't support this format for now: left._tp_time - right._tp_time between -10s and 10s
    /// User needs do `date_diff('second', left._tp_time, right._tp_time) between -10 and 10
    if (!isInt64OrUInt64FieldType(literal_arg->value.getType()))
        throw Exception(ErrorCodes::BAD_QUERY_PARAMETER, "Range join only supports integral range, but got {}", literal_arg->value.getTypeName());

    Int64 range = literal_arg->value.get<Int64>();
    if (data.is_first_arg_left_identifier_of_range_func.has_value())
        /// We already analyze the function, don't analyze again
        return {range * data.range_factor, data.is_first_arg_left_identifier_of_range_func.value()};

    auto * arg_func = non_literal_arg->as<ASTFunction>();
    if (!arg_func)
        throw Exception(ErrorCodes::BAD_QUERY_PARAMETER, "Bad range join");


    /// Analyze non-literal function argument
    if (arg_func->name == "minus")
    {
        if (arg_func->arguments->children.size() != 2)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "Function {} requires 2 arguments function, got '{}'",
                arg_func->name,
                arg_func->formatForErrorMessage());

        auto table_numbers = getTableNumbers(arg_func->arguments->children[0], arg_func->arguments->children[1], data);
        data.addAsofJoinKeys(arg_func->arguments->children[0], arg_func->arguments->children[1], table_numbers, ASOFJoinInequality::RangeBetween);
        data.analyzed_join.setRangeType(Streaming::RangeType::Integer);
        return {range, isLeftIdentifier(table_numbers.first)};
    }
    else if (arg_func->name == "date_diff")
    {
        if (arg_func->arguments->children.size() != 3)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "Function {} requires 3 arguments, got '{}'",
                arg_func->name,
                arg_func->formatForErrorMessage());


        /// We will need parse the scale
        if (const auto * scale = arg_func->arguments->children[0]->as<ASTLiteral>())
        {
            if (scale->value.getType() != Field::Types::String)
                throw Exception(
                    ErrorCodes::SYNTAX_ERROR,
                    "Function {} requires first argument as unit string : second, minute etc, got '{}'",
                    arg_func->name,
                    arg_func->formatForErrorMessage());

            const auto & unit = scale->value.get<String>();
            if (unit == "second")
            {
            }
            else if (unit == "minute")
            {
                data.range_factor = 60;
                range *= data.range_factor;
            }
            else
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "First argument of function {} only supports second or minute, but got '{}' instead",
                    arg_func->name, arg_func->formatForErrorMessage());
        }
        else
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "Function {} requires first argument as unit string : second, minute etc, got '{}'",
                arg_func->name,
                arg_func->formatForErrorMessage());

        auto table_numbers = getTableNumbers(arg_func->arguments->children[1], arg_func->arguments->children[2], data);
        data.addAsofJoinKeys(arg_func->arguments->children[1], arg_func->arguments->children[2], table_numbers, ASOFJoinInequality::RangeBetween);
        data.analyzed_join.setRangeType(Streaming::RangeType::Interval);

        return {range, isLeftIdentifier(table_numbers.first)};
    }
    else
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Function {} is not supported in stream to stream join clause",
            arg_func->name);
}

bool CollectJoinOnKeysMatcher::handleLeftLiteralArgumentForRangeBetweenAsofJoin(const ASTLiteral * left_literal_arg, ASTPtr right_arg, ASOFJoinInequality inequality, Data & data)
{
    auto [range_value, is_first_arg_left_identifier] = handleLeftAndRightArgumentsForRangeBetweenAsOfJoin(left_literal_arg, right_arg, data);

    if (is_first_arg_left_identifier)
    {
        if (inequality == ASOFJoinInequality::Greater || inequality == ASOFJoinInequality::GreaterOrEquals)
        {
            /// The expression is in this `10 >= left.column - right.column or 10 >= date_diff('second', left.column, right.column)` form
            /// Change it to `left.column - right.column <= 10 or date_diff('second', left.column, right.column) <= 10` form
            data.analyzed_join.setRangeAsofRightInequality(reverseASOFJoinInequality(inequality));
            data.analyzed_join.setRangeAsofUpperBound(range_value);
        }
        else
        {
            /// The expression is in this `5 < left.column - right.column or 5 < date_diff('second', left.column, right.column)` form
            /// Change it to `left.column - right.column >= 5 or date_diff('second', left.column, right.column) >= 5` form
            data.analyzed_join.setRangeAsofLeftInequality(reverseASOFJoinInequality(inequality));
            data.analyzed_join.setRangeAsofLowerBound(range_value);
        }
    }
    else
    {
        if (inequality == ASOFJoinInequality::Greater || inequality == ASOFJoinInequality::GreaterOrEquals)
        {
            /// The expression is in this `10 >= right.column - left.column or 10 >= date_diff('second', right.column, left.column)` form
            /// Change it to `left.column - right.column >= -10 or date_diff('second', left.column, right.column) >= -10` form
            data.analyzed_join.setRangeAsofLeftInequality(inequality);
            data.analyzed_join.setRangeAsofLowerBound(-range_value);
        }
        else
        {
            /// The expression is in this `5 < right.column - left.column or 5 < date_diff('second', right.column, left.column)` form
            /// Change it to `left.column - right.column < -5 or date_diff('second', left.column, right.column) < -5` form
            data.analyzed_join.setRangeAsofRightInequality(inequality);
            data.analyzed_join.setRangeAsofUpperBound(-range_value);
        }
    }

    return is_first_arg_left_identifier;
}

bool CollectJoinOnKeysMatcher::handleRightLiteralArgumentForRangeBetweenAsofJoin(const ASTLiteral * right_literal_arg, ASTPtr left_arg, ASOFJoinInequality inequality, Data & data)
{
    auto [range_value, is_first_arg_left_identifier] = handleLeftAndRightArgumentsForRangeBetweenAsOfJoin(right_literal_arg, left_arg, data);

    if (is_first_arg_left_identifier)
    {
        if (inequality == ASOFJoinInequality::Greater || inequality == ASOFJoinInequality::GreaterOrEquals)
        {
            /// The expression is in this `left.column - right.column >= 10 or date_diff('second', left.column, right.column) >= 10` form
            /// Don't need change form
            data.analyzed_join.setRangeAsofLeftInequality(inequality);
            data.analyzed_join.setRangeAsofLowerBound(range_value);
        }
        else
        {
            /// The expression is in this `left.column - right.column < 10 or date_diff('second', right.column, left.column) < 10` form
            /// don't need change form
            data.analyzed_join.setRangeAsofRightInequality(inequality);
            data.analyzed_join.setRangeAsofUpperBound(range_value);
        }
    }
    else
    {
        if (inequality == ASOFJoinInequality::Greater || inequality == ASOFJoinInequality::GreaterOrEquals)
        {
            /// The expression is in this `right.column - left.column >= 10 or date_diff('second', right.column, left.column)` >= 10 form
            /// Change it to `left.column - right.column <= -10 or date_diff('second', left.column, right.column) <= -10` form
            data.analyzed_join.setRangeAsofRightInequality(reverseASOFJoinInequality(inequality));
            data.analyzed_join.setRangeAsofUpperBound(-range_value);
        }
        else
        {
            /// The expression is in this `right.column - left.column < 10 or date_diff('second', right.column, left.column)` < 10 form
            /// Change it to `left.column - right.column > -10 or date_diff('second', left.column, right.column) > -10` form
            data.analyzed_join.setRangeAsofLeftInequality(reverseASOFJoinInequality(inequality));
            data.analyzed_join.setRangeAsofLowerBound(-range_value);
        }
    }
    return is_first_arg_left_identifier;
}
/// proton : ends

}
