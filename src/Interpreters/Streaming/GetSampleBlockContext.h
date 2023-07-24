#pragma once

#include <Core/Joins.h>
#include <Core/Streaming/DataStreamSemantic.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
struct SelectQueryOptions;

namespace Streaming
{
struct GetSampleBlockContext
{
    DataStreamSemantic output_data_stream_semantic = DataStreamSemantic::Append;

    bool parent_select_has_aggregates = false;
    std::optional<JoinStrictness> parent_select_join_strictness;

    void mergeSelectOptions(SelectQueryOptions & select_options);

    ASTPtr rewritten_query;
};

}
}
