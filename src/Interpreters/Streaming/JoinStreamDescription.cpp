#include <Interpreters/Streaming/JoinStreamDescription.h>

#include <Storages/IStorage.h>

namespace DB::Streaming
{
DataStreamSemantic getDataStreamSemantic(StoragePtr storage)
{
    if (!storage)
        return Streaming::DataStreamSemantic::Append;

    if (storage->isVersionedKvMode())
        return Streaming::DataStreamSemantic::VersionedKV;
    else if (storage->isChangelogKvMode())
        return Streaming::DataStreamSemantic::ChangeLogKV;
    else
        return Streaming::DataStreamSemantic::Append;

}
}
