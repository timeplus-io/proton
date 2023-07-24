#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/Streaming/CalculateDataStreamSemantic.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{
namespace Streaming
{
namespace
{

DataStreamSemantic calculateDataStreamSemanticForJoin(
    DataStreamSemantic left_input_data_stream_semantic,
    DataStreamSemantic right_input_data_stream_semantic,
    JoinStrictness strictness,
    SelectQueryInfo & query_info)
{
    auto join_semantic_strictness = std::tuple(left_input_data_stream_semantic, strictness, right_input_data_stream_semantic);
    auto join_semantic = std::tuple(left_input_data_stream_semantic, right_input_data_stream_semantic);

    if (join_semantic == std::tuple(DataStreamSemantic::VersionedKV, DataStreamSemantic::VersionedKV))
    {
        query_info.versioned_kv_tracking_changes = true;
        return DataStreamSemantic::Changelog;
    }
    else if (join_semantic == std::tuple(DataStreamSemantic::ChangelogKV, DataStreamSemantic::ChangelogKV))
    {
        query_info.changelog_tracking_changes = true;
        return DataStreamSemantic::Changelog;
    }
    else if (join_semantic == std::tuple(DataStreamSemantic::Changelog, DataStreamSemantic::Changelog))
    {
        query_info.changelog_tracking_changes = true;
        return DataStreamSemantic::Changelog;
    }
    else if (join_semantic_strictness == std::tuple(DataStreamSemantic::Append, JoinStrictness::All, DataStreamSemantic::VersionedKV))
    {
        query_info.versioned_kv_tracking_changes = true;
        return DataStreamSemantic::Append;
    }
    else if (join_semantic_strictness == std::tuple(DataStreamSemantic::Append, JoinStrictness::Asof, DataStreamSemantic::VersionedKV))
    {
        query_info.versioned_kv_tracking_changes = false;
        return DataStreamSemantic::Append;
    }
    else if (join_semantic_strictness == std::tuple(DataStreamSemantic::Append, JoinStrictness::Any, DataStreamSemantic::VersionedKV))
    {
        query_info.versioned_kv_tracking_changes = false;
        return DataStreamSemantic::Append;
    }
    else if (join_semantic_strictness == std::tuple(DataStreamSemantic::ChangelogKV, JoinStrictness::All, DataStreamSemantic::VersionedKV))
    {
        query_info.changelog_tracking_changes = true;
        query_info.versioned_kv_tracking_changes = true;
        return DataStreamSemantic::Changelog;
    }
    else if (join_semantic_strictness == std::tuple(DataStreamSemantic::ChangelogKV, JoinStrictness::Any, DataStreamSemantic::VersionedKV))
    {
        query_info.changelog_tracking_changes = true;
        query_info.versioned_kv_tracking_changes = false;
        return DataStreamSemantic::Changelog;
    }
    else if (join_semantic_strictness == std::tuple(DataStreamSemantic::ChangelogKV, JoinStrictness::Asof, DataStreamSemantic::VersionedKV))
    {
        query_info.changelog_tracking_changes = true;
        query_info.versioned_kv_tracking_changes = false;
        return DataStreamSemantic::Changelog;
    }
    else if (join_semantic_strictness == std::tuple(DataStreamSemantic::Changelog, JoinStrictness::All, DataStreamSemantic::VersionedKV))
    {
        query_info.changelog_tracking_changes = true;
        query_info.versioned_kv_tracking_changes = true;
        return DataStreamSemantic::Changelog;
    }
    else if (join_semantic_strictness == std::tuple(DataStreamSemantic::Changelog, JoinStrictness::Any, DataStreamSemantic::VersionedKV))
    {
        query_info.changelog_tracking_changes = true;
        query_info.versioned_kv_tracking_changes = false;
        return DataStreamSemantic::Changelog;
    }
    else if (join_semantic_strictness == std::tuple(DataStreamSemantic::Changelog, JoinStrictness::Asof, DataStreamSemantic::VersionedKV))
    {
        query_info.changelog_tracking_changes = true;
        query_info.versioned_kv_tracking_changes = false;
        return DataStreamSemantic::Changelog;
    }

    return DataStreamSemantic::Append;
}

DataStreamSemantic calculateOutputDataStreamSemanticForParentJoin(
    DataStreamSemantic left_input_data_stream_semantic, const SelectQueryOptions & options, SelectQueryInfo & query_info)
{
    assert(options.parent_select_join_strictness);

    if ((left_input_data_stream_semantic == DataStreamSemantic::ChangelogKV)
        || (left_input_data_stream_semantic == DataStreamSemantic::Changelog))
    {
        query_info.changelog_tracking_changes = true;
        return DataStreamSemantic::Changelog;
    }
    else if (left_input_data_stream_semantic == DataStreamSemantic::VersionedKV)
    {
        if ((*options.parent_select_join_strictness == JoinStrictness::Asof)
            || (*options.parent_select_join_strictness == JoinStrictness::Any))
        {
            query_info.versioned_kv_tracking_changes = false;
            return DataStreamSemantic::VersionedKV;
        }
        else
        {
            query_info.versioned_kv_tracking_changes = true;
            return DataStreamSemantic::Changelog;
        }
    }

    return DataStreamSemantic::Append;
}
}

bool isJoinResultChangelog(DataStreamSemantic left_data_stream_semantic, DataStreamSemantic right_data_stream_semantic)
{
    auto join_semantic = std::pair(left_data_stream_semantic, right_data_stream_semantic);

    if ((join_semantic == std::pair(DataStreamSemantic::VersionedKV, DataStreamSemantic::VersionedKV))
        || (join_semantic == std::pair(DataStreamSemantic::ChangelogKV, DataStreamSemantic::ChangelogKV))
        || (join_semantic == std::pair(DataStreamSemantic::Changelog, DataStreamSemantic::Changelog))
        || (join_semantic == std::pair(DataStreamSemantic::ChangelogKV, DataStreamSemantic::VersionedKV))
        || (join_semantic == std::pair(DataStreamSemantic::Changelog, DataStreamSemantic::VersionedKV)))
    {
        return true;
    }

    return false;
}

DataStreamSemanticPair calculateDataStreamSemantic(
    DataStreamSemantic left_input_data_stream_semantic,
    std::optional<DataStreamSemantic> right_input_data_stream_semantic, /// Imply join
    std::optional<JoinStrictness> strictness,
    bool current_select_has_aggregates,
    const SelectQueryOptions & options,
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
        semantic_pair.output_data_stream_semantic = DataStreamSemantic::Append;

        if (right_input_data_stream_semantic)
        {
            /// JOIN + aggregates
            semantic_pair.effective_input_data_stream_semantic = calculateDataStreamSemanticForJoin(
                left_input_data_stream_semantic, *right_input_data_stream_semantic, *strictness, query_info);
        }
        else
        {
            /// Single stream.
            /// Only has aggregates. Calculate if tracking changes is required
            /// For changelog-kv / changelog stream / changelog(...) they are tracking changes natively, it just means read `_tp_delta` column for them.
            /// For versioned-kv stream, we will ask the source to add changelog transform to track the changes.
            if ((left_input_data_stream_semantic == DataStreamSemantic::ChangelogKV)
                || (left_input_data_stream_semantic == DataStreamSemantic::Changelog))
            {
                query_info.changelog_tracking_changes = true;
                semantic_pair.effective_input_data_stream_semantic = DataStreamSemantic::Changelog;
            }
            else if (left_input_data_stream_semantic == DataStreamSemantic::VersionedKV)
            {
                query_info.versioned_kv_tracking_changes = true;
                semantic_pair.effective_input_data_stream_semantic = DataStreamSemantic::Changelog;
            }
        }
    }
    else if (right_input_data_stream_semantic)
    {
        /// JOIN only
        semantic_pair.effective_input_data_stream_semantic = calculateDataStreamSemanticForJoin(
            left_input_data_stream_semantic, *right_input_data_stream_semantic, *strictness, query_info);

        /// Calculate its output data stream semantic
        if (isJoinResultChangelog(left_input_data_stream_semantic, *right_input_data_stream_semantic))
            semantic_pair.output_data_stream_semantic = DataStreamSemantic::Changelog;
    }
    else
    {
        /// Single stream. Flat transformation / filtering, we will need look at its parent select
        if (options.parent_select_has_aggregates)
        {
            if (options.parent_select_join_strictness)
            {
                /// Parent has aggregates + join
                semantic_pair.effective_input_data_stream_semantic = semantic_pair.output_data_stream_semantic
                    = calculateOutputDataStreamSemanticForParentJoin(left_input_data_stream_semantic, options, query_info);
            }
            else
            {
                /// Parent only has aggregates
                if ((left_input_data_stream_semantic == DataStreamSemantic::ChangelogKV)
                    || (left_input_data_stream_semantic == DataStreamSemantic::Changelog))
                {
                    query_info.changelog_tracking_changes = true;
                    semantic_pair.effective_input_data_stream_semantic = semantic_pair.output_data_stream_semantic
                        = DataStreamSemantic::Changelog;
                }
                else if (left_input_data_stream_semantic == DataStreamSemantic::VersionedKV)
                {
                    query_info.versioned_kv_tracking_changes = true;
                    semantic_pair.effective_input_data_stream_semantic = semantic_pair.output_data_stream_semantic
                        = DataStreamSemantic::Changelog;
                }
            }
        }
        else if (options.parent_select_join_strictness)
        {
            /// Parent only has join
            semantic_pair.effective_input_data_stream_semantic = semantic_pair.output_data_stream_semantic
                = calculateOutputDataStreamSemanticForParentJoin(left_input_data_stream_semantic, options, query_info);
        }
        else
        {
            /// Parent only has flat transformation, do nothing
        }
    }

    return semantic_pair;
}

}
}
