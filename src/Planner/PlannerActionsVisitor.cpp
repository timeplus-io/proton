#include <Planner/PlannerActionsVisitor.h>

#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeSet.h>

#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeTuple.h>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnConst.h>

#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionFactory.h>
#include <Functions/indexHint.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class ActionNodeNameHelper
{
public:
    static String calculateConstantActionNodeName(const Field & constant_literal, const DataTypePtr & constant_type)
    {
        auto constant_name = applyVisitor(FieldVisitorToString(), constant_literal);
        return constant_name + "_" + constant_type->getName();
    }

    static String calculateConstantActionNodeName(const Field & constant_literal)
    {
        return calculateConstantActionNodeName(constant_literal, applyVisitor(FieldToDataType(), constant_literal));
    }
};

}

String calculateConstantActionNodeName(const Field & constant_literal, const DataTypePtr & constant_type)
{
    return ActionNodeNameHelper::calculateConstantActionNodeName(constant_literal, constant_type);
}

String calculateConstantActionNodeName(const Field & constant_literal)
{
    return ActionNodeNameHelper::calculateConstantActionNodeName(constant_literal);
}

}
