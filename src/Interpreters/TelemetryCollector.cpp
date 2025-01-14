#include "TelemetryCollector.h"


//#include <filesystem>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <base/ClockUtils.h>
#include <base/getMemoryAmount.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/DateLUT.h>
#include <Common/getNumberOfPhysicalCPUCores.h>

namespace fs = std::filesystem;

namespace ProfileEvents
{
extern const Event SelectQuery;
extern const Event StreamingSelectQuery;
extern const Event HistoricalSelectQuery;
}

namespace DB
{
namespace
{
constexpr auto DEFAULT_INTERVAL_MS = 5 * 60 * 1000;
constexpr auto TELEMETRY_QUEUE_SIZE = 1000;
}

TelemetryCollector::TelemetryCollector(ContextPtr context_)
    : pool(context_->getSchedulePool())
    , started_on_in_minutes(UTCMinutes::now())
    , queue(TELEMETRY_QUEUE_SIZE)
    , logger(&Poco::Logger::get("TelemetryCollector"))
{
    const auto & config = context_->getConfigRef();

    try
    {
        is_enable = config.getBool("telemetry_enabled", true);
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(logger, "Failed to parse telemetry_enabled, use default settings: true. {}", e.displayText());
        is_enable = true;
    }

    try
    {
        collect_interval_ms = config.getUInt("telemetry_interval_ms", DEFAULT_INTERVAL_MS);
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(logger, "Failed to parse collect_interval_ms, use default settings: {}. {}", DEFAULT_INTERVAL_MS, e.displayText());
        collect_interval_ms = DEFAULT_INTERVAL_MS;
    }

    WriteBufferFromOwnString wb;
    writeDateTimeTextISO(UTCMilliseconds::now(), 3, wb, DateLUT::instance("UTC"));
    started_on = wb.str();
}

TelemetryCollector::~TelemetryCollector()
{
    shutdown();
    LOG_INFO(logger, "stopped");
}

void TelemetryCollector::startup()
{
    collector_task = pool.createTask("TelemetryCollector", [this]() { this->collect(); });
    collector_task->activate();
    collector_task->schedule();

    if (!upload_thread.joinable())
        upload_thread = ThreadFromGlobalPool(&TelemetryCollector::upload, this);
}

void TelemetryCollector::shutdown()
{
    if (is_shutdown.test_and_set())
        return;

    if (collector_task)
    {
        LOG_INFO(logger, "Stopped");
        collector_task->deactivate();
    }

    if (!upload_thread.joinable())
        return;

    auto temp_thread = std::move(upload_thread);
    temp_thread.join();
}

void TelemetryCollector::enable()
{
    LOG_WARNING(
        logger,
        "Please note that telemetry is enabled. "
        "This is used to collect the version and runtime environment information to Timeplus, Inc. "
        "You can disable it by setting telemetry_enabled to false in config.yaml");
    is_enable = true;
}

void TelemetryCollector::disable()
{
    LOG_WARNING(logger, "Please note that telemetry is disabled.");
    is_enable = false;
}

void TelemetryCollector::add(std::shared_ptr<TelemetryElement> element)
{
    if (is_shutdown.test())
        return;

    if (!queue.add(std::move(element), /*timeout_ms=*/5))
        LOG_WARNING(logger, "Telemetry queue is full, dropping telemetry.");
}

void TelemetryCollector::collect()
{
    SCOPE_EXIT({ collector_task->scheduleAfter(getCollectIntervalMilliseconds()); });

    if (!isEnabled())
        return;

    try
    {
        /// https://stackoverflow.com/questions/20010199/how-to-determine-if-a-process-runs-inside-lxc-docker
        bool in_docker = fs::exists("/.dockerenv");

        auto load_counter = [](const auto & event) {
            assert(event < ProfileEvents::end());
            return static_cast<Int64>(ProfileEvents::global_counters[event].load(std::memory_order_relaxed));
        };

        const auto total_select_query = load_counter(ProfileEvents::SelectQuery);
        const auto streaming_select_query = load_counter(ProfileEvents::StreamingSelectQuery);
        const auto historical_select_query = load_counter(ProfileEvents::HistoricalSelectQuery);

        const auto delta_total_select_query = total_select_query - prev_total_select_query;
        prev_total_select_query = total_select_query;

        const auto delta_streaming_select_query = streaming_select_query - prev_streaming_select_query;
        prev_streaming_select_query = streaming_select_query;

        const auto delta_historical_select_query = historical_select_query - prev_historical_select_query;
        prev_historical_select_query = historical_select_query;

        add<TelemetryStatsElementBuilder>([&](auto & builder) {
            builder.useCPU(getNumberOfPhysicalCPUCores())
                .useMemoryInGB(getMemoryAmount() / 1024 / 1024 / 1024)
                .isNewSession(new_session)
                .startedOn(started_on)
                .during(UTCMinutes::now() - started_on_in_minutes)
                .isInDocker(in_docker)
                .setTotalSelectQuery(total_select_query)
                .setHistoricalSelectQuery(historical_select_query)
                .setStreamingSelectQuery(streaming_select_query)
                .setDeltaTotalSelectQuery(delta_total_select_query)
                .setDeltaHistoricalSelectQuery(delta_historical_select_query)
                .setDeltaStreamingSelectQuery(delta_streaming_select_query);
        });

        new_session = false;
    }
    catch (Poco::Exception & ex)
    {
        LOG_WARNING(logger, "Failed to collect telemetry: {}.", ex.displayText());
    }
}

void TelemetryCollector::upload()
{
    constexpr auto jitsu_url = "https://data.timeplus.com/api/s/s2s/track";
    constexpr auto jitsu_token = "U7qmIGzuZvvkp16iPaYLeBR4IHfKBY6P:Cc6EUDRmEHG9TCO7DX8x23xWrdFg8pBU";

    Poco::URI uri(jitsu_url);
    Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());
    session.setKeepAlive(true);

    while (!is_shutdown.test())
    {
        auto q = queue.drain(/*timeout_ms=*/10'000);

        /// drop all telemetry if disabled
        if (!is_enable)
            continue;

        while (!q.empty())
        {
            auto element = q.front();
            q.pop();
            try
            {
                std::string data = element->toString();

                Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery());

                LOG_TRACE(logger, "Sending telemetry: {}.", data);

                request.setContentLength(data.length());
                request.setContentType("application/json");
                request.add("X-Write-Key", jitsu_token);

                auto & requestStream = session.sendRequest(request);
                requestStream << data;

                Poco::Net::HTTPResponse response;

                auto & responseStream = session.receiveResponse(response);

                if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
                {
                    std::stringstream ss;
                    ss << responseStream.rdbuf();
                    LOG_WARNING(logger, "Failed to send telemetry: {}.", ss.str());
                    return;
                }

                LOG_INFO(logger, "Telemetry sent successfully.");
            }
            catch (Poco::Exception & ex)
            {
                LOG_WARNING(logger, "Failed to send telemetry: {}.", ex.displayText());
            }
        }
    }
}
}
