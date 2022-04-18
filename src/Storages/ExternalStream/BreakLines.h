#pragma once

namespace re2
{
class RE2;
}

namespace DB
{
/// Break `data` into pieces according to line breaker regex. If we can't find line breaker in data and max line length reaches
/// force breaking the whole data into a single piece
std::vector<std::string_view> breakLines(const char * data, size_t & length, const re2::RE2 & line_breaker, size_t max_line_length);
}
