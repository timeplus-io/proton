#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/copyData.h>

namespace DB
{

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

std::string IObjectStorage::getCacheBasePath() const
{
    return cache ? cache->getBasePath() : "";
}

void IObjectStorage::removeFromCache(const std::string & path)
{
    if (cache)
    {
        auto key = cache->hash(path);
        cache->remove(key);
    }
}

void IObjectStorage::copyObjectToAnotherObjectStorage(const std::string & object_from, const std::string & object_to, IObjectStorage & object_storage_to, std::optional<ObjectAttributes> object_to_attributes) // NOLINT
{
    if (&object_storage_to == this)
        copyObject(object_from, object_to, object_to_attributes);

    auto in = readObject(object_from);
    auto out = object_storage_to.writeObject(object_to, WriteMode::Rewrite);
    copyData(*in, *out);
    out->finalize();
}

}
