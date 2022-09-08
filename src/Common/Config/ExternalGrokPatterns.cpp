#include "ExternalGrokPatterns.h"

#include <Interpreters/Context.h>

#include <fstream>
#include <sys/stat.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB::ErrorCodes
{
extern const int CANNOT_STAT;
}

namespace
{
int64_t lastModifiedTimeMS(const String & file_name)
{
    struct stat buf;
    int res = ::stat(file_name.c_str(), &buf);
    if (-1 == res)
        DB::throwFromErrnoWithPath("Cannot execute stat ", file_name, DB::ErrorCodes::CANNOT_STAT);

#if defined(OS_DARWIN)
    return buf.st_mtimespec.tv_sec * 1000 + buf.st_mtimespec.tv_nsec / 1000000;
#else
    return buf.st_mtim.tv_sec * 1000 + buf.st_mtim.tv_nsec / 1000000;
#endif
}
}

namespace DB
{
ExternalGrokPatterns::ExternalGrokPatterns(ContextPtr context_)
    : log(&Poco::Logger::get("ExternalGrokPatterns")), pool(context_->getSchedulePool())
{
    const auto & config = context_->getConfigRef();
    if (config.has("grok_patterns_file"))
        file_name = config.getString("grok_patterns_file");

    if (!file_name.empty())
        loadPatternsFromFile();

    reload_task = pool.createTask("ExternalGrokPatternsReload", [this]() { this->reloadPatternsFromFile(); });
    reload_task->activate();
    reload_task->scheduleAfter(initial_reload_task_delay_ms);
}

ExternalGrokPatterns::~ExternalGrokPatterns()
{
    shutdown();
}

void ExternalGrokPatterns::shutdown()
{
    if (!is_shutdown.test_and_set())
        reload_task->deactivate();
}

std::optional<String> ExternalGrokPatterns::tryGetPattern(const String & pattern_name) const
{
    std::shared_lock lock(patterns_mutex);
    auto iter = patterns->find(pattern_name);
    if (iter != patterns->end())
        return iter->second;

    return {};
}

void ExternalGrokPatterns::reloadPatternsFromFile()
{
    if (!file_name.empty())
    {
        auto last_modified = lastModifiedTimeMS(file_name);
        if (last_updated != last_modified)
        {
            loadPatternsFromFile();
            last_updated = last_modified;
        }

        reload_task->scheduleAfter(reload_interval_ms);
    }
}

void ExternalGrokPatterns::loadPatternsFromFile()
{
    std::ifstream ifs(file_name);
    if (!ifs)
    {
        LOG_WARNING(log, "External grok patterns file '{}' is not exist", file_name);
        patterns = std::make_unique<std::unordered_map<String, String>>();
        return;
    }

    int line_num = 0;
    String line;

    auto new_patterns = std::make_unique<std::unordered_map<String, String>>();
    while (std::getline(ifs, line))
    {
        Poco::trimInPlace(line);
        ++line_num;

        /// Skip empty or comments
        if (line.empty() || line.starts_with('#') || line.starts_with("//"))
            continue;

        /// <PATTERN_NAME> + ' ' + <pattern_regex>
        /// <PATTERN_NAME> + '\t' + <pattern_regex>
        auto pos = line.find_first_of(" \t");
        if (pos == String::npos)
        {
            LOG_WARNING(log, "Error format in pattern file line {}, expected format is PATTERN_NAME + ' ' or '\\t' + regex", line_num);
            continue;
        }

        auto pattern_name = line.substr(0, pos);
        auto [_, inserted] = new_patterns->emplace(pattern_name, line.substr(pos + 1));
        if (!inserted)
            LOG_WARNING(
                log,
                "There are duplicate pattern name '{}' in line {} of the extern grok patterns file '{}'.",
                pattern_name,
                line_num,
                file_name);
    }

    /// Update grok pattern
    std::lock_guard lock(patterns_mutex);
    patterns = std::move(new_patterns);
}
}
