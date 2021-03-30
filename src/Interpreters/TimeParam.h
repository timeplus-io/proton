#pragma once

#include <string>

namespace DB
{

struct TimeParam
{
private:
    String start;
    String end;

public:
    TimeParam() = default;
    const String & getStart() const { return start; }
    const String & getEnd() const { return end; }

    void setStart(const String & start_) { start = start_; }
    void setEnd(const String & end_) { end = end_; }
    bool empty() const { return start.empty() && end.empty(); }
};

}
