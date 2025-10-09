#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/Streaming/CalculateDataStreamSemantic.h>
#include <Interpreters/Streaming/HashJoin/MemoryHashJoin/MemoryHashJoin.h>
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
    SelectQueryInfo & query_info,
    bool allow_emit_changelog_join_result)
{
    assert(left_input_data_stream_semantic.streaming);

    /// Speical handling
    /// 1) for <stream> join <table>, the right inputs don't support changelog semantic
    if (!right_input_data_stream_semantic.streaming)
    {
        /// Left stream semantic (Keep the original semantic)
        query_info.left_input_tracking_changes = isChangelogDataStream(left_input_data_stream_semantic);

        /// Right stream semantic (Append or VersionedKV or ChangelogKV)
        query_info.right_input_tracking_changes = false;
        if (unlikely(isChangelogDataStream(right_input_data_stream_semantic)))
        {
            /// For table(changelog_kv), we can convert it to append, since its _tp_delta always is `+1`
            if (isChangelogKVStorage(right_input_data_stream_semantic))
                right_input_data_stream_semantic = DataStreamSemantic::Append;
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The filled join data doesn't support changelog processing");
        }

        if (query_info.force_emit_changelog && !isChangelogDataStream(left_input_data_stream_semantic))
        {
            if (canTrackChangesFromInput(left_input_data_stream_semantic))
            {
                query_info.left_input_tracking_changes = true;
                return DataStreamSemantic::Changelog;
            }
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for emit changelog from append stream join table result");
        }

        return left_input_data_stream_semantic;
    }

    /// 2) for asof / any join or cross join, the left and right inputs don't support changelog
    if (kind_and_strictness.second == JoinStrictness::Asof || kind_and_strictness.second == JoinStrictness::Any
        || isCrossOrComma(kind_and_strictness.first))
    {
        /// Left stream semantic (Append or VersionedKV)
        query_info.left_input_tracking_changes = false;
        if (unlikely(isChangelogDataStream(left_input_data_stream_semantic)))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The left data stream semantic of {} {} JOIN is expected to be 'Append', but got 'Changelog'",
                kind_and_strictness.first,
                kind_and_strictness.second);

        /// Right stream semantic (Append or VersionedKV)
        query_info.right_input_tracking_changes = false;
        if (unlikely(isChangelogDataStream(right_input_data_stream_semantic)))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "The right data stream semantic of {} {} JOIN is expected to be 'Append', but got 'Changelog'",
                kind_and_strictness.first,
                kind_and_strictness.second);

        return DataStreamSemantic::Append;
    }

    /// Left stream semantic
    if (canTrackChangesFromInput(left_input_data_stream_semantic))
    {
        query_info.left_input_tracking_changes = true;
        left_input_data_stream_semantic = DataStreamSemantic::Changelog;
    }
    else
    {
        query_info.left_input_tracking_changes = false;
        left_input_data_stream_semantic = DataStreamSemantic::Append;
    }

    /// Right stream semantic
    if (canTrackChangesFromInput(right_input_data_stream_semantic))
    {
        query_info.right_input_tracking_changes = true;
        right_input_data_stream_semantic = DataStreamSemantic::Changelog;
    }
    else
    {
        query_info.right_input_tracking_changes = false;
        right_input_data_stream_semantic = DataStreamSemantic::Append;
    }

    if (isJoinResultChangelog(left_input_data_stream_semantic, right_input_data_stream_semantic))
    {
        if (allow_emit_changelog_join_result)
            return DataStreamSemantic::Changelog;
        else
            /// NOTE: If the current layer doesn't need joined result emit changelog,
            /// we shall emit append-only stream with mutable semantic(MutableStream).
            /// In this way, the outer layer can still track changes if needed.
            /// For example: select count() from (select * from kv join kv2 on kv.key = kv2.key)
            return DataStreamSemanticEx{StorageSemantic::NativeKV};
    }
    else
        return DataStreamSemantic::Append;
}
}

bool canTrackChangesFromInput(DataStreamSemanticEx input_data_stream_semantic)
{
    return isChangelogDataStream(input_data_stream_semantic) || isKeyedStorage(input_data_stream_semantic);
}

bool isJoinResultChangelog(DataStreamSemanticEx left_data_stream_semantic, DataStreamSemanticEx right_data_stream_semantic)
{
    if (!left_data_stream_semantic.streaming)
        return false;

    /// <stream> join <table>
    if (!right_data_stream_semantic.streaming)
        return isChangelogDataStream(left_data_stream_semantic);

    /// <stream> join <stream>
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
    /// By default, the joined result stream semantic always is append-only unless the current layer has aggregates
    /// or the query forces to emit changelog
    bool allow_emit_changelog_join_result = current_select_has_aggregates || query_info.force_emit_changelog;
    /// First, look at what the current layer does

    /// When the current layer has join or aggregates, we calculate the output data semantic locally and its inputs data stream semantic.
    /// Otherwise we will also need look at the parent SELECT
    if (current_select_has_aggregates)
    {
        if (right_input_data_stream_semantic)
        {
            /// JOIN + aggregates
            semantic_pair.effective_input_data_stream_semantic = calculateDataStreamSemanticForJoin(
                left_input_data_stream_semantic,
                *right_input_data_stream_semantic,
                *kind_and_strictness,
                query_info,
                allow_emit_changelog_join_result);
        }
        else
        {
            /// Single stream.
            /// Only has aggregates. Calculate if tracking changes is required
            /// For changelog-kv / changelog stream / changelog(...) they are tracking changes natively, it just means read `_tp_delta` column for them.
            /// For versioned-kv stream, we will ask the source to add changelog transform to track the changes.
            semantic_pair.effective_input_data_stream_semantic = left_input_data_stream_semantic;
        }

        if (canTrackChangesFromInput(semantic_pair.effective_input_data_stream_semantic))
        {
            query_info.left_input_tracking_changes = true;
            semantic_pair.effective_input_data_stream_semantic = DataStreamSemantic::Changelog;
        }

        semantic_pair.output_data_stream_semantic
            = query_info.force_emit_changelog ? DataStreamSemantic::Changelog : DataStreamSemantic::Append;
    }
    else if (right_input_data_stream_semantic)
    {
        /// JOIN only
        semantic_pair.effective_input_data_stream_semantic = calculateDataStreamSemanticForJoin(
            left_input_data_stream_semantic,
            *right_input_data_stream_semantic,
            *kind_and_strictness,
            query_info,
            allow_emit_changelog_join_result);

        if (query_info.force_emit_changelog && !Streaming::isChangelogDataStream(semantic_pair.effective_input_data_stream_semantic))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for emit changelog from non-changelog join results");

        semantic_pair.output_data_stream_semantic = semantic_pair.effective_input_data_stream_semantic;
    }
    else
    {
        /// Single stream. Flat transformation / filtering
        semantic_pair.output_data_stream_semantic = semantic_pair.effective_input_data_stream_semantic = left_input_data_stream_semantic;
        if (query_info.force_emit_changelog)
        {
            if (canTrackChangesFromInput(left_input_data_stream_semantic))
            {
                query_info.left_input_tracking_changes = true;
                semantic_pair.effective_input_data_stream_semantic = DataStreamSemantic::Changelog;
                semantic_pair.output_data_stream_semantic = DataStreamSemantic::Changelog;
            }
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for emit changelog from append stream");
        }
    }

    return semantic_pair;
}

}
}
