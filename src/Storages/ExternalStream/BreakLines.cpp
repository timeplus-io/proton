#include "BreakLines.h"

#include <re2/re2.h>

#include <cassert>

namespace DB
{
/// line_breaker shall always have capture group
std::vector<std::string_view> breakLines(const char * data, size_t & length, const re2::RE2 & line_breaker, size_t max_line_length)
{
    assert(line_breaker.ok() && line_breaker.NumberOfCapturingGroups() == 1);

    re2::StringPiece input{data, length};

    re2::StringPiece breaker;
    re2::StringPiece last_breaker;

    std::vector<std::string_view> lines;
    lines.reserve(100);


    while (re2::RE2::FindAndConsume(&input, line_breaker, &breaker))
    {
        /// re2 moves input to next starting position beyond the current match automatically
        /// [.....breaker][....
        /// ^             ^
        /// |             |
        /// start       next start
        if (breaker.data() != data)
        {
            /// Not match at the very beginning
            if (!last_breaker.empty())
            {
                /// [last_breaker + data in the middle][breaker][next start...
                /// last breaker + data in the middle are composed into a line
                lines.emplace_back(last_breaker.data(), static_cast<size_t>(breaker.data() - last_breaker.data()));
                last_breaker = breaker;
            }
            else
            {
                /// [data in the middle][breaker][next start...][breaker][next start...
                /// data in the middle + breaker are composed into a line
                /// Force the prefix information into a line
                /// example:
                ///     Processing configuration file 'config.xml'.
                ///     There is no file 'config.xml', will use embedded config.
                ///     Env variable is not set: TELEMETRY_ENABLED
                ///     2024.06.03 14:19:48.818674 [ 374515 ] {} <Information> SentryWriter: Sending crash reports is disabled
                lines.emplace_back(data, static_cast<size_t>(breaker.data() - data));
                last_breaker = breaker;
            }
        }
        else
        {
            /// matched at the very beginning
            /// do nothing
            last_breaker = breaker;
        }
    }

    /// update remaining
    if (!lines.empty())
    {
        length -= (breaker.data() - data);
    }

    /// if remaining length is greater than max_line_length
    /// force the remaining data to a line
    if (length >= max_line_length)
    {
        lines.emplace_back(breaker.data(), length);
        length = 0;
    }

    return lines;
}
}
