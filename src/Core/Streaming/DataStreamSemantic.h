#pragma once

namespace DB::Streaming
{
enum class StorageSemantic : uint8_t
{
    Append = 0,
    Changelog = 1,
    ChangelogKV = 2,
    VersionedKV = 3,
};

enum class DataStreamSemantic : uint8_t
{
    Append = 0,
    Changelog = 1
};

struct DataStreamSemanticEx
{
    DataStreamSemantic semantic = DataStreamSemantic::Append;
    std::optional<StorageSemantic> storage_semantic; /// extra elem is used for join

    DataStreamSemanticEx() = default;
    DataStreamSemanticEx(const DataStreamSemanticEx &) = default;
    DataStreamSemanticEx(DataStreamSemanticEx &&) = default;
    DataStreamSemanticEx & operator=(DataStreamSemanticEx &&) = default;
    DataStreamSemanticEx & operator=(const DataStreamSemanticEx &) = default;

    DataStreamSemanticEx(DataStreamSemantic semantic_) : semantic(semantic_) { }
    DataStreamSemanticEx & operator=(const DataStreamSemantic & semantic_)
    {
        semantic = semantic_;
        return *this;
    }

    bool operator==(const DataStreamSemanticEx & rhs) const { return semantic == rhs.semantic; }
    bool operator==(DataStreamSemantic semantic_) const { return semantic == semantic_; }
    bool operator==(StorageSemantic) const = delete;

    /// Only used for validate join combination
    StorageSemantic toStorageSemantic() const { return storage_semantic.value_or(static_cast<StorageSemantic>(semantic)); }

    /// Only used for flat transformation / filtering layer of storage
    DataStreamSemanticEx(StorageSemantic storage_semantic_) { attachStorageSemantic(storage_semantic_); }

    void attachStorageSemantic(StorageSemantic storage_semantic_)
    {
        storage_semantic = storage_semantic_;
        if (storage_semantic_ == StorageSemantic::Changelog || storage_semantic_ == StorageSemantic::ChangelogKV)
            semantic = DataStreamSemantic::Changelog;
        else
            semantic = DataStreamSemantic::Append;
    }

    bool hasStorageSemantic(StorageSemantic storage_semantic_) const
    {
        return storage_semantic.has_value() && storage_semantic.value() == storage_semantic_;
    }
};

inline bool isVersionedKeyedStorage(DataStreamSemanticEx data_stream_semantic)
{
    return data_stream_semantic.hasStorageSemantic(StorageSemantic::VersionedKV);
}

inline bool isChangelogKeyedStorage(DataStreamSemanticEx data_stream_semantic)
{
    return data_stream_semantic.hasStorageSemantic(StorageSemantic::ChangelogKV);
}

inline bool isChangelogStorage(DataStreamSemanticEx data_stream_semantic)
{
    return data_stream_semantic.hasStorageSemantic(StorageSemantic::Changelog);
}

inline bool isAppendStorage(DataStreamSemanticEx data_stream_semantic)
{
    return data_stream_semantic.hasStorageSemantic(StorageSemantic::Append);
}

inline bool isKeyedStorage(DataStreamSemanticEx data_stream_semantic)
{
    return isVersionedKeyedStorage(data_stream_semantic) || isChangelogKeyedStorage(data_stream_semantic);
}

inline bool isChangelogDataStream(DataStreamSemanticEx data_stream_semantic)
{
    return data_stream_semantic == DataStreamSemantic::Changelog;
}

inline bool isAppendDataStream(DataStreamSemanticEx data_stream_semantic)
{
    return data_stream_semantic == DataStreamSemantic::Append;
}
}
