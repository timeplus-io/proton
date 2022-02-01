#pragma once

#include "MetaStoreConfig.h"

#include <NativeLog/Requests/CreateTopicRequest.h>
#include <NativeLog/Requests/CreateTopicResponse.h>
#include <NativeLog/Requests/DeleteTopicRequest.h>
#include <NativeLog/Requests/DeleteTopicResponse.h>
#include <NativeLog/Requests/ListTopicsRequest.h>
#include <NativeLog/Requests/ListTopicsResponse.h>

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

    //// `createTopic` creates a topic according to the request and persists the metadata
    /// information in MetaStore
    /// return nothing on success, otherwise throw an exception
    CreateTopicResponse createTopic(const std::string & ns, const CreateTopicRequest & req);

    //// `deleteTopic` deletes a topic according to the request and clears the metadata
    /// information from MetaStore
    /// return nothing on success, otherwise throw an exception
    DeleteTopicResponse deleteTopic(const std::string & ns, const DeleteTopicRequest & req);

    /// Collect topic metadata from metastore
    /// If topic name and namespace is not empty, return topic metadata only for that topic
    /// If namespace is not empty, return topics metadata only for that namespace
    /// Otherwise return all topics metadata
    ListTopicsResponse listTopics(const std::string & ns, const ListTopicsRequest & req) const;

private:
    MetaStoreConfig config;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    std::mutex mlock;
    std::unique_ptr<rocksdb::DB> metadb;

    Poco::Logger * logger;
};
}
