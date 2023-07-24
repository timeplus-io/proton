#pragma once

#include <Core/Joins.h>
#include <Core/Streaming/DataStreamSemantic.h>

namespace DB
{
struct SelectQueryOptions;
struct SelectQueryInfo;

namespace Streaming
{

bool isJoinResultChangelog(DataStreamSemantic left_data_stream_semantic, DataStreamSemantic right_data_stream_semantic);

/// Calculate output data stream semantic according its inputs data stream semantic and
/// the properties of the current layer of SELECT
struct DataStreamSemanticPair
{
    DataStreamSemantic effective_input_data_stream_semantic = DataStreamSemantic::Append;
    DataStreamSemantic output_data_stream_semantic = DataStreamSemantic::Append;

    bool isChangelogInput() const noexcept { return isChangelogDataStream(effective_input_data_stream_semantic); }

    bool isChangelogOutput() const noexcept { return isChangelogDataStream(output_data_stream_semantic); }
};

DataStreamSemanticPair calculateDataStreamSemantic(
    DataStreamSemantic left_input_data_stream_semantic,
    std::optional<DataStreamSemantic> right_input_data_stream_semantic,
    std::optional<JoinStrictness> strictness,
    bool current_select_has_aggregates,
    const SelectQueryOptions & options,
    SelectQueryInfo & query_info);
}
}
