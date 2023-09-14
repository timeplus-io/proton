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
bool canTrackChangesFromStorage(DataStreamSemanticEx input_data_stream_semantic);

bool isJoinResultChangelog(DataStreamSemanticEx left_data_stream_semantic, DataStreamSemanticEx right_data_stream_semantic);

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
    bool current_select_has_aggregates,
    SelectQueryInfo & query_info);
}
}
