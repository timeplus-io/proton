#pragma once

namespace DB::Streaming
{
enum class DataStreamSemantic
{
    Append = 0,
    ChangelogKV = 1,
    VersionedKV = 2,
    Changelog = 3,
};

inline bool isVersionedKeyedDataStream(DataStreamSemantic data_stream_semantic)
{
    return data_stream_semantic == DataStreamSemantic::VersionedKV;
}

inline bool isChangelogKeyedDataStream(DataStreamSemantic data_stream_semantic)
{
    return data_stream_semantic == DataStreamSemantic::ChangelogKV;
}

inline bool isKeyedDataStream(DataStreamSemantic data_stream_semantic)
{
    return data_stream_semantic == DataStreamSemantic::VersionedKV || data_stream_semantic == DataStreamSemantic::ChangelogKV;
}

inline bool isChangelogDataStream(DataStreamSemantic data_stream_semantic)
{
    return data_stream_semantic == DataStreamSemantic::Changelog;
}

inline bool isAppendDataStream(DataStreamSemantic data_stream_semantic)
{
    return data_stream_semantic == DataStreamSemantic::Append;
}

}
