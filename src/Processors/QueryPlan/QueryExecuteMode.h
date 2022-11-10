#pragma once

#include <Core/ExecuteMode.h>

namespace DB
{
struct Settings;
ExecuteMode queryExecuteMode(bool is_streaming, const Settings & settings);
}
