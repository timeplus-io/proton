#include "Name.h"

namespace DB
{
namespace DWAL
{
std::string escapeDWalName(const std::string & namespace_, const std::string & name_)
{
    /// FIXME : normalize name according to Kafka topic name "[a-zA-Z0-9\\._\\-]"
    return namespace_ + "." + name_;
}
}
}
