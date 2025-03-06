#pragma once

#include <memory>

namespace DB::Kafka
{

class Handle;
class ConsumerHandle;
using ConsumerHandlePtr = std::shared_ptr<ConsumerHandle>;
class ProducerHandle;
using ProducerHandlePtr = std::shared_ptr<ProducerHandle>;

}
