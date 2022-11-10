#include "ExecuteMode.h"

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTING_ENUM(
    ExecuteMode,
    ErrorCodes::BAD_ARGUMENTS,
    {{"normal", ExecuteMode::NORMAL},
     {"subscribe", ExecuteMode::SUBSCRIBE},
     {"recover", ExecuteMode::RECOVER},
     {"unsubscribe", ExecuteMode::UNSUBSCRIBE}})

}
