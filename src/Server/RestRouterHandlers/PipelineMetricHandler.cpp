#include "PipelineMetricHandler.h"

#include "SchemaValidator.h"

#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
}

namespace
{
    const std::map<String, std::map<String, String>> PIPELINE_METRIC_SCHEMA = {{"required", {{"query_id", "string"}}}};
}

std::pair<String, Int32> PipelineMetricHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    String error;
    if (!validatePost(payload, error))
        return { jsonErrorResponse(error, ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST };

    const auto & query_id = payload->get("query_id").toString();
    String pipeline_metric = query_context->getProcessList().getPipelineMetric(query_id, query_context->getUserName());
    if (pipeline_metric.empty())
        return { jsonErrorResponse("Query Not Found", ErrorCodes::BAD_REQUEST_PARAMETER, query_id), HTTPResponse::HTTP_NOT_FOUND };
    return { pipeline_metric, HTTPResponse::HTTP_OK };
}

bool PipelineMetricHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(PIPELINE_METRIC_SCHEMA, payload, error_msg);
}

}
