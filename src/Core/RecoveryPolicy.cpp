#include <Core/RecoveryPolicy.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTING_ENUM(
    RecoveryPolicy,
    ErrorCodes::BAD_ARGUMENTS,
    {{"strict", RecoveryPolicy::Strict},
     {"best_effort", RecoveryPolicy::BestEffort}})

}
