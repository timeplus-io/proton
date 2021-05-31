#pragma once

#include <string>

namespace DB
{
namespace DWAL
{
/// Escape `namespace_` and `name_` to restrict char set
std::string escapeDWalName(const std::string & namespace_, const std::string & name_);
}
}
