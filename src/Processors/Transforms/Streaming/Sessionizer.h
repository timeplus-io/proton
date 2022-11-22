#pragma once

#include <Interpreters/ExpressionActions.h>

namespace DB
{
class Chunk;

namespace Streaming
{
class Sessionizer final
{
public:
    Sessionizer(
        const Block & input_header_,
        const Block & output_header_,
        ExpressionActionsPtr start_actions_,
        ExpressionActionsPtr end_actions_);

    void sessionize(Chunk & chunk);

private:
    void addPredicateColumns(Chunk & chunk);

private:
    Block input_header;

    ExpressionActionsPtr start_actions;
    ExpressionActionsPtr end_actions;

    std::vector<size_t> start_required_pos;
    std::vector<size_t> end_required_pos;
};
}
}
