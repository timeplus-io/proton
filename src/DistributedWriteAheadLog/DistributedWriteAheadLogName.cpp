#include "DistributedWriteAheadLogName.h"

namespace DB
{
std::string escapeDWalName(const std::string & namespace_, const std::string & name_)
{
    /// FIXME : normalize name according to Kafka topic name "[a-zA-Z0-9\\._\\-]"
    return namespace_ + "." + name_;
}
}
