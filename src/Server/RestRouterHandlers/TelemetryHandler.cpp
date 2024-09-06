#include "TelemetryHandler.h"
#include "SchemaValidator.h"

#include <Interpreters/TelemetryCollector.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_REQUEST_PARAMETER;
extern const int UDF_INVALID_NAME;
}

std::pair<String, Int32> TelemetryHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    Poco::JSON::Object resp;
    resp.set("request_id", query_context->getCurrentQueryId());

    auto & telemetry_collector = TelemetryCollector::instance(query_context);

    resp.set("collect", telemetry_collector.isEnabled());
    resp.set("collect_interval_ms", telemetry_collector.getCollectIntervalMilliseconds());

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return {resp_str_stream.str(), HTTPResponse::HTTP_OK};
}

std::map<String, std::map<String, String>> TelemetryHandler::update_schema
    = {{"required", {{"collect", "bool"}}}, {"optional", {{"collect_interval_ms", "int"}}}};

bool TelemetryHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(update_schema, payload, error_msg);
}

std::pair<String, Int32> TelemetryHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    auto & telemetry_collector = TelemetryCollector::instance(query_context);

    bool collect = payload->get("collect").convert<bool>();

    if (collect)
        telemetry_collector.enable();
    else
        telemetry_collector.disable();

    if (payload->has("collect_interval_ms"))
    {
        UInt64 collect_interval_ms = payload->get("collect_interval_ms").convert<UInt64>();
        telemetry_collector.setCollectIntervalMilliseconds(collect_interval_ms);
    }

    Poco::JSON::Object resp;
    resp.set("request_id", query_context->getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return {resp_str_stream.str(), HTTPResponse::HTTP_OK};
}
}
