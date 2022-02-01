#pragma once

#include "CommonResponse.h"

#include "CreateTopicRequest.h"
#include "TopicInfo.h"

namespace nlog
{
struct CreateTopicResponse
{
    static CreateTopicResponse from(const CreateTopicRequest & request)
    {
        CreateTopicResponse response;
        response.topic_info.name = request.name;
        response.topic_info.partitions = request.partitions;
        response.topic_info.replicas = request.replicas;
        response.topic_info.compacted = request.compacted;

        return response;
    }

    TopicInfo topic_info;

    CommonResponse error;

    std::string string() const { return topic_info.string(); }
};

}
