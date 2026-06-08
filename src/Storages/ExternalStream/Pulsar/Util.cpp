#include <Storages/ExternalStream/Pulsar/Util.h>

#if USE_PULSAR
#include <base/scope_guard.h>
#include <base/sleep.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <thread>

namespace DB::ExternalStream
{
namespace
{
/// Copied from contrib/pulsar-cmake/pulsar-client-cpp/lib/ResultUtils.h
ALWAYS_INLINE bool isResultRetryable(pulsar::Result result)
{
    chassert(result != pulsar::ResultOk);
    if (result == pulsar::ResultRetryable || result == pulsar::ResultDisconnected)
        return true;

    static const std::unordered_set<int> unretriable_errors{
        pulsar::ResultConnectError,
        pulsar::ResultTimeout,
        pulsar::ResultAuthenticationError,
        pulsar::ResultAuthorizationError,
        pulsar::ResultInvalidUrl,
        pulsar::ResultInvalidConfiguration,
        pulsar::ResultIncompatibleSchema,
        pulsar::ResultTopicNotFound,
        pulsar::ResultOperationNotSupported,
        pulsar::ResultNotAllowedError,
        pulsar::ResultChecksumError,
        pulsar::ResultCryptoError,
        pulsar::ResultConsumerAssignError,
        pulsar::ResultProducerBusy,
        pulsar::ResultConsumerBusy,
        pulsar::ResultLookupError,
        pulsar::ResultTooManyLookupRequestException,
        pulsar::ResultProducerBlockedQuotaExceededException,
        pulsar::ResultProducerBlockedQuotaExceededError};
    return !unretriable_errors.contains(static_cast<int>(result));
}
}

pulsar::Result executeWithRetry(std::function<pulsar::Result()> op, std::string_view op_name, size_t timeout_ms, LoggerPtr logger)
{
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_DEBUG(logger, "Took {}ms to execute pulsar operation '{}'", stopwatch.elapsedMilliseconds(), op_name); });

    while (true)
    {
        auto res = op();
        if (res == pulsar::ResultOk || !isResultRetryable(res) || stopwatch.elapsedMilliseconds() >= timeout_ms)
            return res;

        /// Retry if the result is retryable
        LOG_INFO(logger, "Failed to execute pulsar operation '{}': {}, retrying ...", op_name, pulsar::strResult(res));

        sleepForMilliseconds(100);
    }
}
}
#endif
