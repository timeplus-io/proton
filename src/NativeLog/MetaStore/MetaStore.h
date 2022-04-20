#pragma once

#include "MetaStoreConfig.h"

#include <NativeLog/Requests/CreateStreamRequest.h>
#include <NativeLog/Requests/CreateStreamResponse.h>
#include <NativeLog/Requests/DeleteStreamRequest.h>
#include <NativeLog/Requests/DeleteStreamResponse.h>
#include <NativeLog/Requests/ListStreamsRequest.h>
#include <NativeLog/Requests/ListStreamsResponse.h>
#include <NativeLog/Requests/RenameStreamRequest.h>
#include <NativeLog/Requests/RenameStreamResponse.h>

#include <boost/noncopyable.hpp>

#include <vector>

namespace Poco
{
class Logger;
}

namespace rocksdb
{
class DB;
}

namespace nlog
{
class MetaStore final : private boost::noncopyable
{
public:
    explicit MetaStore(const MetaStoreConfig & config_);
    ~MetaStore();

    void startup();
    void shutdown();

    //// `createStream` creates a stream according to the request and persists the metadata
    /// information in MetaStore
    /// return nothing on success, otherwise throw an exception
    CreateStreamResponse createStream(const std::string & ns, const CreateStreamRequest & req);

    //// `deleteStream` deletes a stream according to the request and clears the metadata
    /// information from MetaStore
    /// return nothing on success, otherwise throw an exception
    DeleteStreamResponse deleteStream(const std::string & ns, const DeleteStreamRequest & req);

    /// `renameStream` rename a stream name to a different one
    RenameStreamResponse renameStream(const std::string & ns, const RenameStreamRequest & req);

    /// Collect stream metadata from metastore
    /// If stream name and namespace is not empty, return stream metadata only for that stream
    /// If namespace is not empty, return streams metadata only for that namespace
    /// Otherwise return all streams metadata
    ListStreamsResponse listStreams(const std::string & ns, const ListStreamsRequest & req) const;

private:
    ListStreamsResponse getStream(const std::string & ns, const ListStreamsRequest & req) const;
    ListStreamsResponse listStreamsInNamespace(const std::string & ns) const;
    ListStreamsResponse listAllStreams() const;

private:
    MetaStoreConfig config;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    mutable std::mutex mlock;
    std::unique_ptr<rocksdb::DB> metadb;

    Poco::Logger * logger;
};
}
