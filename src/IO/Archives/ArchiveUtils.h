#pragma once

#include <optional>
#include <string_view>
#include <string>

namespace DB
{

bool hasSupportedTarExtension(std::string_view path);
bool hasSupportedZipExtension(std::string_view path);
bool hasSupported7zExtension(std::string_view path);

bool hasSupportedArchiveExtension(std::string_view path);

std::pair<std::string, std::optional<std::string>> getURIAndArchivePattern(const std::string & source);

}
