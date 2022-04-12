#pragma once

#include "CommonResponse.h"

namespace nlog
{
struct TruncateResponse final : public CommonResponse
{
    using CommonResponse::CommonResponse;
};
}
