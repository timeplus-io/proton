#pragma once

#include <librdkafka/rdkafka.h>

namespace DB
{

namespace Kafka
{

int32_t mapErrorCode(rd_kafka_resp_err_t err, bool retriable = false);

}

}
