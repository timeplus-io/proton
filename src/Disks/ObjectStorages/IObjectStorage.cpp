#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

IAsynchronousReader & IObjectStorage::getThreadPoolReader()
{
    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");

    return context->getThreadPoolReader(Context::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
}

ThreadPool & IObjectStorage::getThreadPoolWriter()
{
    auto context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");

    return context->getThreadPoolWriter();
}

void IObjectStorage::copyObjectToAnotherObjectStorage( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    IObjectStorage & object_storage_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    if (&object_storage_to == this)
        copyObject(object_from, object_to, object_to_attributes);

    auto in = readObject(object_from);
    auto out = object_storage_to.writeObject(object_to, WriteMode::Rewrite);
    copyData(*in, *out);
    out->finalize();
}

std::string IObjectStorage::getCacheBasePath() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getCacheBasePath() is not implemented for {}", getName());
}

}
