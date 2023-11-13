#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Common/logger_useful.h>
#include <Common/getResource.h>

namespace DB
{
/// Format examples in file 'grok-patterns'
class ExternalGrokPatterns
{
private:
    Poco::Logger * log;
    String file_name;
    std::unique_ptr<std::unordered_map<String, String>> patterns;
    mutable std::shared_mutex patterns_mutex;

    BackgroundSchedulePool & pool;
    BackgroundSchedulePoolTaskHolder reload_task;
    int64_t last_updated = -1;

    static constexpr auto initial_reload_task_delay_ms = 30000;
    static constexpr auto reload_interval_ms = 2000;

public:
    static ExternalGrokPatterns & instance(ContextPtr context_)
    {
        static ExternalGrokPatterns inst(context_);
        return inst;
    }

    ~ExternalGrokPatterns();
    void shutdown();

    std::optional<String> tryGetPattern(const String & pattern_name) const;

private:
    ExternalGrokPatterns(ContextPtr context_);
    void reloadPatternsFromFile();
    void loadPatternsFromFile();
    void loadPatternsFromStream(std::istream& stream);

private:
    std::atomic_flag is_shutdown;
};
}
