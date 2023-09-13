#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/Streaming/CalculateDataStreamSemantic.h>
#include <Interpreters/Streaming/HashJoin.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{
namespace Streaming
{
namespace
{
DataStreamSemanticEx calculateDataStreamSemanticForJoin(
    DataStreamSemanticEx left_input_data_stream_semantic,
    DataStreamSemanticEx right_input_data_stream_semantic,
    std::pair<JoinKind, JoinStrictness> kind_and_strictness,
    SelectQueryInfo & query_info)
{
    HashJoin::validate(
        {left_input_data_stream_semantic.toStorageSemantic(),
         kind_and_strictness.first,
         kind_and_strictness.second,
         right_input_data_stream_semantic.toStorageSemantic()});

    /// Speical handling for asof / any join
    if (kind_and_strictness.second == JoinStrictness::Asof || kind_and_strictness.second == JoinStrictness::Any)
    {
        /// Left stream semantic (Append or VersionedKV)
        query_info.left_storage_tracking_changes = false;
        if (unlikely(isChangelogDataStream(left_input_data_stream_semantic)))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The left data stream semantic of {} join is expected to be 'Append'",
                kind_and_strictness.second);

        /// Right stream semantic (Append or VersionedKV)
        query_info.right_storage_tracking_changes = false;
        if (unlikely(isChangelogDataStream(right_input_data_stream_semantic)))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The right data stream semantic of {} join is expected to be 'Append'",
                kind_and_strictness.second);
    }
    else
    {
        /// Left stream semantic: tracking changelog for storage
        if (canTrackChangesFromStorage(left_input_data_stream_semantic))
        {
            query_info.left_storage_tracking_changes = true;
            left_input_data_stream_semantic = DataStreamSemantic::Changelog;
        }
        else
            query_info.left_storage_tracking_changes = false;

        /// Right stream semantic: tracking changelog for storage
        if (canTrackChangesFromStorage(right_input_data_stream_semantic))
        {
            query_info.right_storage_tracking_changes = true;
            right_input_data_stream_semantic = DataStreamSemantic::Changelog;
        }
        else
            query_info.right_storage_tracking_changes = false;
    }

    if (isJoinResultChangelog(left_input_data_stream_semantic, right_input_data_stream_semantic))
        return DataStreamSemantic::Changelog;
    else
        return DataStreamSemantic::Append;
}
}

bool canTrackChangesFromStorage(DataStreamSemanticEx input_data_stream_semantic)
{
    /// 1) Storages: VerionsedKV, ChangelogKV, Changelog
    /// 2) Or flat transformation / filtering data stream over 1) Storages
    return isVersionedKeyedStorage(input_data_stream_semantic) || isChangelogKeyedStorage(input_data_stream_semantic)
        || isChangelogKeyedStorage(input_data_stream_semantic);
}

bool isJoinResultChangelog(DataStreamSemanticEx left_data_stream_semantic, DataStreamSemanticEx right_data_stream_semantic)
{
    if (!isAppendDataStream(left_data_stream_semantic) && !isAppendDataStream(right_data_stream_semantic))
        return true;

    return false;
}

DataStreamSemanticPair calculateDataStreamSemantic(
    DataStreamSemanticEx left_input_data_stream_semantic,
    std::optional<DataStreamSemanticEx> right_input_data_stream_semantic, /// Imply join
    std::optional<std::pair<JoinKind, JoinStrictness>> kind_and_strictness,
    bool current_select_has_aggregates,
    SelectQueryInfo & query_info)
{
    DataStreamSemanticPair semantic_pair;

    /// First, look at what the current layer does

    /// When the current layer has join or aggregates, we calculate the output data semantic locally and its inputs data stream semantic.
    /// Otherwise we will also need look at the parent SELECT
    if (current_select_has_aggregates)
    {
        /// If current layer of select has aggregates, the output stream for now will be an append only stream
        /// TODO, in future, we can emit changelog when `EMIT changelog` clause is supported
        if (query_info.force_emit_changelog)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for aggregates emit changelog");

        semantic_pair.output_data_stream_semantic = DataStreamSemantic::Append;

        if (right_input_data_stream_semantic)
        {
            /// JOIN + aggregates
            semantic_pair.effective_input_data_stream_semantic = calculateDataStreamSemanticForJoin(
                left_input_data_stream_semantic, *right_input_data_stream_semantic, *kind_and_strictness, query_info);
        }
        else
        {
            /// Single stream.
            /// Only has aggregates. Calculate if tracking changes is required
            /// For changelog-kv / changelog stream / changelog(...) they are tracking changes natively, it just means read `_tp_delta` column for them.
            /// For versioned-kv stream, we will ask the source to add changelog transform to track the changes.
            if (canTrackChangesFromStorage(left_input_data_stream_semantic))
            {
                query_info.left_storage_tracking_changes = true;
                semantic_pair.effective_input_data_stream_semantic = DataStreamSemantic::Changelog;
            }
            else
                semantic_pair.effective_input_data_stream_semantic = left_input_data_stream_semantic;
        }
    }
    else if (right_input_data_stream_semantic)
    {
        /// JOIN only
        semantic_pair.effective_input_data_stream_semantic = calculateDataStreamSemanticForJoin(
            left_input_data_stream_semantic, *right_input_data_stream_semantic, *kind_and_strictness, query_info);

        if (query_info.force_emit_changelog && !Streaming::isChangelogDataStream(semantic_pair.effective_input_data_stream_semantic))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for emit changelog from non-changelog join results");

        semantic_pair.output_data_stream_semantic = semantic_pair.effective_input_data_stream_semantic;
    }
    else
    {
        /// Single stream. Flat transformation / filtering
        if (query_info.force_emit_changelog)
        {
            if (canTrackChangesFromStorage(left_input_data_stream_semantic))
            {
                query_info.left_storage_tracking_changes = true;
                semantic_pair.effective_input_data_stream_semantic = DataStreamSemantic::Changelog;
                semantic_pair.output_data_stream_semantic = DataStreamSemantic::Changelog;
            }
            else if (isAppendDataStream(left_input_data_stream_semantic))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for emit changelog from append stream");
        }
        else
            semantic_pair.output_data_stream_semantic = semantic_pair.effective_input_data_stream_semantic = left_input_data_stream_semantic;
    }

    return semantic_pair;
}

}
}
