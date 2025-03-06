#pragma once

#include <string>

namespace DB
{

std::string base64Encode(const std::string & decoded, bool url_encoding = false, bool no_padding = false); /// proton: added no_padding

std::string base64Decode(const std::string & encoded, bool url_encoding = false);

}
