#include <IO/ConnectionTimeouts.h>

namespace DB
{

class SendReceiveTimeoutsForFirstAttempt
{
private:
    static constexpr size_t known_methods_count = 6;
    using KnownMethodsArray = std::array<String, known_methods_count>;
    static const KnownMethodsArray known_methods;

    /// HTTP_POST is used for CompleteMultipartUpload requests. Its latency could be high.
    /// These requests need longer timeout, especially when minio is used.
    /// The same assumption are made for HTTP_DELETE, HTTP_PATCH
    /// That requests are more heavy that HTTP_GET, HTTP_HEAD, HTTP_PUT

    static constexpr Poco::Timestamp::TimeDiff first_byte_ms[known_methods_count][2] =
        {
            /* GET */ {200, 200},
            /* POST */ {200, 200},
            /* DELETE */ {200, 200},
            /* PUT */ {200, 200},
            /* HEAD */ {200, 200},
            /* PATCH */ {200, 200},
    };

    static constexpr Poco::Timestamp::TimeDiff rest_bytes_ms[known_methods_count][2] =
        {
            /* GET */ {500, 500},
            /* POST */ {1000, 30000},
            /* DELETE */ {1000, 10000},
            /* PUT */ {1000, 3000},
            /* HEAD */ {500, 500},
            /* PATCH */ {1000, 10000},
    };

    static_assert(sizeof(first_byte_ms) == sizeof(rest_bytes_ms));
    static_assert(sizeof(first_byte_ms) == known_methods_count * sizeof(Poco::Timestamp::TimeDiff) * 2);

    static size_t getMethodIndex(const String & method)
    {
        KnownMethodsArray::const_iterator it = std::find(known_methods.begin(), known_methods.end(), method);
        chassert(it != known_methods.end());
        if (it == known_methods.end())
            return 0;
        return std::distance(known_methods.begin(), it);
    }

public:
    static std::pair<Poco::Timespan, Poco::Timespan> getSendReceiveTimeout(const String & method, bool first_byte)
    {
        auto idx = getMethodIndex(method);

        if (first_byte)
            return std::make_pair(
                Poco::Timespan(first_byte_ms[idx][0] * 1000),
                Poco::Timespan(first_byte_ms[idx][1] * 1000)
            );

        return std::make_pair(
            Poco::Timespan(rest_bytes_ms[idx][0] * 1000),
            Poco::Timespan(rest_bytes_ms[idx][1] * 1000)
        );
    }
};

const SendReceiveTimeoutsForFirstAttempt::KnownMethodsArray SendReceiveTimeoutsForFirstAttempt::known_methods =
    {
        "GET", "POST", "DELETE", "PUT", "HEAD", "PATCH"
};

ConnectionTimeouts ConnectionTimeouts::getAdaptiveTimeouts(const String & method, bool first_attempt, bool first_byte) const
{
    if (!first_attempt)
        return *this;

    auto [send, recv] = SendReceiveTimeoutsForFirstAttempt::getSendReceiveTimeout(method, first_byte);

    auto aggressive = *this;
    aggressive.send_timeout = saturate(send, send_timeout);
    aggressive.receive_timeout = saturate(recv, receive_timeout);

    return aggressive;
}

}
