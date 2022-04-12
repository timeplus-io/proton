#pragma once

#include "CommonResponse.h"
#include "StreamDescription.h"

#include <vector>

namespace nlog
{
struct ListStreamsResponse final : public CommonResponse
{
    using CommonResponse::CommonResponse;

    std::vector<StreamDescription> streams;
};
}
