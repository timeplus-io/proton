#include <Core/ExecuteMode.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTING_ENUM(
    ExecuteMode,
    ErrorCodes::BAD_ARGUMENTS,
    {{"normal", ExecuteMode::Normal},
     {"subscribe", ExecuteMode::Subscribe},
     {"recover", ExecuteMode::Recover}})

}
