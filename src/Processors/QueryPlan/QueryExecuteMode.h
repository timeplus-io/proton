#pragma once

#include <Core/ExecuteMode.h>

namespace DB
{
struct Settings;
ExecuteMode queryExecuteMode(bool is_streaming, bool is_subquery, const Settings & settings);
}
