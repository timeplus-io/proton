#include "sendRequest.h"

#include <IO/HTTPCommon.h>
#include <Common/Exception.h>

#include <Poco/Net/HTTPRequest.h>

#include <sstream>

namespace DB
{

namespace
{
    int toHTTPCode(const Poco::Exception & e)
    {
        /// FIXME, more HTTP code mapping
        auto code = e.code();
        if (code == 1000 || code == 422)
        {
            /// Unretriable failure
            return -1;
        }
        return Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
    }
}

std::pair<String, Int32> sendRequest(
    const Poco::URI & uri,
    const String & method,
    const String & query_id,
    const String & user,
    const String & password,
    const String & payload,
    Poco::Logger * log)
{
    /// One second for connect/send/receive
    ConnectionTimeouts timeouts({2, 0}, {5, 0}, {10, 0});

    PooledHTTPSessionPtr session;
    try
    {
        session = makePooledHTTPSession(uri, timeouts, 1);
        Poco::Net::HTTPRequest request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        request.setHost(uri.getHost());
        request.setContentLength(payload.size());
        request.setContentType("application/json");
        /// FIXME : query ID chain. Change the query ID to avoid same query ID issue
        request.add("x-daisy-query-id", "from-" + query_id);
        request.add("x-daisy-user", user);

        if (!password.empty())
        {
            request.add("x-daisy-key", password);
        }

        auto & ostr = session->sendRequest(request);
        ostr << payload;
        if (!ostr.good())
        {
            LOG_ERROR(log, "Failed to send request payload={} to uri={} query_id={}", payload, uri.toString(), query_id);
            return {"Internal server error", Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
        }

        Poco::Net::HTTPResponse response;
        auto & istr = session->receiveResponse(response);

        std::stringstream response_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        response_stream << istr.rdbuf();

        String response_body{response_stream.str()};

        auto status = response.getStatus();
        if (status == Poco::Net::HTTPResponse::HTTP_OK)
        {
            LOG_INFO(
                log,
                "Successfully executed request to uri={} successfully, method={} payload={} query_id={}",
                uri.toString(),
                method,
                payload,
                query_id);
        }
        else
        {
            LOG_ERROR(
                log,
                "Failed to execute request to uri={} method={} payload={} query_id={}, error={}",
                uri.toString(),
                method,
                payload,
                query_id,
                response_body);
        }

        return {response_body, status};
    }
    catch (const Poco::Exception & e)
    {
        if (!session.isNull())
        {
            session->attachSessionData(e.message());
        }

        LOG_ERROR(
            log,
            "Failed to send request to uri={} method={} payload={} query_id={} error={} exception={}",
            uri.toString(),
            method,
            payload,
            query_id,
            e.message(),
            getCurrentExceptionMessage(true, true));

        return {"Unkown Exception", toHTTPCode(e)};
    }
    catch (...)
    {
        LOG_ERROR(
            log,
            "Failed to send request to uri={} method={} payload={} query_id={} exception={}",
            uri.toString(),
            method,
            payload,
            query_id,
            getCurrentExceptionMessage(true, true));
    }

    return {"Internal server error", Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
}
}
