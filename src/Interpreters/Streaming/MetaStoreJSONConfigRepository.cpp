#include "MetaStoreJSONConfigRepository.h"

#include <Coordination/MetaStoreDispatcher.h>
#include <Interpreters/Streaming/ASTToJSONUtils.h>
#include <Common/ProtonCommon.h>

#include <Poco/Glob.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Util/JSONConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int INCORRECT_DATA;
extern const int NOT_A_LEADER;
extern const int TIMEOUT_EXCEEDED;
}

namespace Streaming
{
namespace
{
constexpr auto FUNC_PREFIX = "CFG";
constexpr auto STATUS_PREFIX = "STATUS";
}

MetaStoreJSONConfigRepository::MetaStoreJSONConfigRepository(
    const std::shared_ptr<MetaStoreDispatcher> & metastore_dispatcher_, const std::string & namespace_)
    : metastore_dispatcher(metastore_dispatcher_), ns(namespace_)
{
}

uint32_t MetaStoreJSONConfigRepository::getVersion(const std::string & definition_entity_name)
{
    try
    {
        Poco::JSON::Object::Ptr status = readConfigKey(fmt::format("{}/{}", STATUS_PREFIX, definition_entity_name));
        if (status->has("version"))
            return status->getValue<uint32_t>("version");

        return 0;
    }
    catch (...)
    {
        return 0;
    }
}

Poco::Timestamp MetaStoreJSONConfigRepository::getUpdateTime(const std::string & definition_entity_name)
{
    Poco::JSON::Object::Ptr status = readConfigKey(fmt::format("{}/{}", STATUS_PREFIX, definition_entity_name));
    return Poco::Timestamp(Poco::Timestamp::TimeVal(status->getValue<Int64>("update_time")));
}

std::set<std::string> MetaStoreJSONConfigRepository::getAllLoadablesDefinitionNames()
{
    if (metastore_dispatcher->hasServer() && !metastore_dispatcher->hasLeader())
        throw Exception(ErrorCodes::NOT_A_LEADER, "Ignoring request, because no alive raft leader exist");

    auto kv_pairs = metastore_dispatcher->localRangeGetByNamespace(FUNC_PREFIX, ns);
    std::set<std::string> keys;
    for (const auto & [key, value] : kv_pairs)
        keys.emplace(key.substr(4));

    return keys;
}

bool MetaStoreJSONConfigRepository::exists(const std::string & definition_entity_name)
{
    if (metastore_dispatcher->hasServer() && !metastore_dispatcher->hasLeader())
        throw Exception(ErrorCodes::NOT_A_LEADER, "No alive raft leader exists");

    String json_cfg;
    try
    {
        json_cfg = metastore_dispatcher->localGetByKey(fmt::format("{}/{}", FUNC_PREFIX, definition_entity_name), ns);
        return true;
    }
    catch (...)
    {
        return false;
    }
}

Poco::AutoPtr<Poco::Util::AbstractConfiguration> MetaStoreJSONConfigRepository::load(const std::string & key)
{
    Poco::JSON::Object::Ptr object = get(key);
    return Poco::AutoPtr<Poco::Util::AbstractConfiguration>(
    new Poco::Util::JSONConfiguration(object));
}

Poco::JSON::Object::Ptr MetaStoreJSONConfigRepository::get(const std::string & key) const
{
    if (metastore_dispatcher->hasServer() && !metastore_dispatcher->hasLeader())
        throw Exception(ErrorCodes::NOT_A_LEADER, "No alive raft leader exists");

    return readConfigKey(fmt::format("{}/{}", FUNC_PREFIX, key));
}

Poco::JSON::Object::Ptr MetaStoreJSONConfigRepository::readConfigKey(const std::string & key) const
{
    auto json_cfg = metastore_dispatcher->localGetByKey(key, ns);
    Poco::JSON::Parser parser;
    return parser.parse(json_cfg).extract<Poco::JSON::Object::Ptr>();
}

void MetaStoreJSONConfigRepository::save(const std::string & key, const Poco::JSON::Object::Ptr & config)
{
    std::vector<std::pair<String, String>> kv_pairs;
    String cfg_json = jsonToString(*config);
    kv_pairs.emplace_back(fmt::format("{}/{}", FUNC_PREFIX, key), cfg_json);

    /// create status
    Poco::JSON::Object status;
    uint32_t version = ProtonConsts::UDF_VERSION;

    status.set("version", version);
    status.set("update_time", Poco::Timestamp().epochMicroseconds());
    std::stringstream status_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    status.stringify(status_str_stream, 0);
    kv_pairs.emplace_back(fmt::format("{}/{}", STATUS_PREFIX, key), status_str_stream.str());

    auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIPUT);
    request->as<Coordination::KVMultiPutRequest>()->kv_pairs = kv_pairs;

    auto response = metastore_dispatcher->putRequest(request, ns);
    if (response->code != ErrorCodes::OK)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Save config item {} failed with code {}, msg {}", key, response->code, response->msg);
}

void MetaStoreJSONConfigRepository::remove(const std::string & config_file)
{
    auto request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIDELETE);
    request->as<Coordination::KVMultiDeleteRequest>()->keys
        = {fmt::format("{}/{}", FUNC_PREFIX, config_file), fmt::format("{}/{}", STATUS_PREFIX, config_file)};

    auto response = metastore_dispatcher->putRequest(request, ns);
    if (response->code != ErrorCodes::OK)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Remove config item {} failed with code {}, msg {}",
            config_file,
            response->code,
            response->msg);
}

Poco::JSON::Array::Ptr MetaStoreJSONConfigRepository::list() const
{
    if (metastore_dispatcher->hasServer() && !metastore_dispatcher->hasLeader())
    {
        LOG_ERROR(&Poco::Logger::get("MetaStoreJSONConfigRepository"), "No alive raft leader");
        throw Exception(ErrorCodes::NOT_A_LEADER, "Fail to list configuration");
    }

    Poco::JSON::Array::Ptr functions(new Poco::JSON::Array());
    auto kv_pairs = metastore_dispatcher->localRangeGetByNamespace(FUNC_PREFIX, ns);
    for (const auto & [key, value] : kv_pairs)
    {
        Poco::JSON::Parser parser;
        try
        {
            const auto func = parser.parse(value).extract<Poco::JSON::Object::Ptr>();
            functions->add(func->getObject("function"));
        }
        catch (...)
        {
            /// ignore parse error
            continue;
        }
    }
    return functions;
}
}
}
