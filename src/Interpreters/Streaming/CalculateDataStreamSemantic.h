#pragma once

#include <Core/Joins.h>
#include <Core/Streaming/DataStreamSemantic.h>

namespace DB
{
struct SelectQueryOptions;
struct SelectQueryInfo;

namespace Streaming
{
/// @brief Return whether current semantic can tracking changes:
/// 1) Storage (VersionedKV/ChangelogKV/Changelog)
/// 2) Or flat transformation / filtering data stream over 1) Storages
/// 3) Or changelog data stream
bool canTrackChangesFromInput(DataStreamSemanticEx input_data_stream_semantic);

/// Used for <stream> join <stream>
bool isJoinResultChangelog(DataStreamSemanticEx left_data_stream_semantic, DataStreamSemanticEx right_data_stream_semantic);

/// Used for <stream/table> join <stream/table>
bool isJoinResultChangelog(
    DataStreamSemanticEx left_data_stream_semantic,
    bool left_is_streaming,
    DataStreamSemanticEx right_data_stream_semantic,
    bool right_is_streaming);

/// Calculate output data stream semantic according its inputs data stream semantic and
/// the properties of the current layer of SELECT
struct DataStreamSemanticPair
{
    DataStreamSemanticEx effective_input_data_stream_semantic;
    DataStreamSemanticEx output_data_stream_semantic;

    bool isChangelogInput() const noexcept { return isChangelogDataStream(effective_input_data_stream_semantic); }

    bool isChangelogOutput() const noexcept { return isChangelogDataStream(output_data_stream_semantic); }
};

DataStreamSemanticPair calculateDataStreamSemantic(
    DataStreamSemanticEx left_input_data_stream_semantic,
    std::optional<DataStreamSemanticEx> right_input_data_stream_semantic,
    std::optional<std::pair<JoinKind, JoinStrictness>> kind_and_strictness,
    std::optional<bool> right_input_is_streaming,
    bool current_select_has_aggregates,
    SelectQueryInfo & query_info);
}
}
