#pragma once

#include <Core/Types.h>

namespace DB
{

enum class ApacheIcebergStorageType : uint8_t
{
    S3,
    Azure,
    Local,
    HDFS,
};

}
