#include "TelemetryCollector.h"
#include "config_version.h"

#include <filesystem>
#include <Core/ServerUUID.h>
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
}

TelemetryCollector::TelemetryCollector(ContextPtr context_)
    : log(&Poco::Logger::get("TelemetryCollector")), pool(context_->getSchedulePool()), started_on_in_minutes(UTCMinutes::now())
{
    const auto & config = context_->getConfigRef();

    if (config.getBool("telemetry_enabled", true))
        is_enable.test_and_set();

    collect_interval_ms = config.getUInt("telemetry_interval_ms", DEFAULT_INTERVAL_MS);

    WriteBufferFromOwnString wb;
    writeDateTimeTextISO(UTCMilliseconds::now(), 3, wb, DateLUT::instance("UTC"));
    started_on = wb.str();
}

TelemetryCollector::~TelemetryCollector()
{
    shutdown();
    LOG_INFO(log, "stopped");
}

void TelemetryCollector::startup()
{
    collector_task = pool.createTask("TelemetryCollector", [this]() { this->collect(); });
    collector_task->activate();
    collector_task->schedule();
}

void TelemetryCollector::shutdown()
{
    if (!is_shutdown.test_and_set() && collector_task)
    {
        LOG_INFO(log, "Stopped");
        collector_task->deactivate();
    }
}

void TelemetryCollector::enable()
{
    LOG_WARNING(
        log,
        "Please note that telemetry is enabled. "
        "This is used to collect the version and runtime environment information to Timeplus, Inc. "
        "You can disable it by setting telemetry_enabled to false in config.yaml");
    is_enable.test_and_set();
}

void TelemetryCollector::disable()
{
    LOG_WARNING(log, "Please note that telemetry is disabled.");
    is_enable.clear();
}

void TelemetryCollector::collect()
{
    SCOPE_EXIT({ collector_task->scheduleAfter(getCollectIntervalMilliseconds()); });

    if (!isEnabled())
        return;

    constexpr auto jitsu_url = "https://data.timeplus.com/api/s/s2s/track";
    constexpr auto jitsu_token = "U7qmIGzuZvvkp16iPaYLeBR4IHfKBY6P:Cc6EUDRmEHG9TCO7DX8x23xWrdFg8pBU";

    try
    {
        Poco::URI uri(jitsu_url);
        Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery());

        auto memory_in_gb = getMemoryAmount() / 1024 / 1024 / 1024;
        auto cpu = getNumberOfPhysicalCPUCores();

        Int64 duration_in_minute = UTCMinutes::now() - started_on_in_minutes;

        DB::UUID server_uuid = DB::ServerUUID::get();
        std::string server_uuid_str = server_uuid != DB::UUIDHelpers::Nil ? DB::toString(server_uuid) : "Unknown";

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

        std::string data = fmt::format(
            "{{"
            "\"type\": \"track\","
            "\"event\": \"proton_ping\","
            "\"properties\": {{"
            "    \"cpu\": \"{}\","
            "    \"memory_in_gb\": \"{}\","
            "    \"edition\": \"{}\","
            "    \"version\": \"{}\","
            "    \"new_session\": \"{}\","
            "    \"started_on\": \"{}\","
            "    \"duration_in_minute\": \"{}\","
            "    \"server_id\": \"{}\","
            "    \"docker\": \"{}\","
            "    \"total_select_query\": \"{}\","
            "    \"historical_select_query\": \"{}\","
            "    \"streaming_select_query\": \"{}\","
            "    \"delta_total_select_query\": \"{}\","
            "    \"delta_historical_select_query\": \"{}\","
            "    \"delta_streaming_select_query\": \"{}\""
            "}}"
            "}}",
            cpu,
            memory_in_gb,
            EDITION,
            VERSION_STRING,
            new_session,
            started_on,
            duration_in_minute,
            server_uuid_str,
            in_docker,
            total_select_query,
            historical_select_query,
            streaming_select_query,
            delta_total_select_query,
            delta_historical_select_query,
            delta_streaming_select_query);

        LOG_TRACE(log, "Sending telemetry: {}.", data);

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
            LOG_WARNING(log, "Failed to send telemetry: {}.", ss.str());
            return;
        }

        new_session = false;
        LOG_INFO(log, "Telemetry sent successfully.");
    }
    catch (Poco::Exception & ex)
    {
        LOG_WARNING(log, "Failed to send telemetry: {}.", ex.displayText());
    }
}

}
