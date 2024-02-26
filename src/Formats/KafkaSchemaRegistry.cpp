#include <Formats/KafkaSchemaRegistry.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <format>

#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

KafkaSchemaRegistry::KafkaSchemaRegistry(const String & base_url_, const String & credentials_)
    : base_url(base_url_)
    , logger(&Poco::Logger::get("KafkaSchemaRegistry"))
{
    assert(!base_url.empty());

    if (auto pos = credentials_.find(':'); pos == credentials_.npos)
        credentials.setUsername(credentials_);
    else
    {
        credentials.setUsername(credentials_.substr(0, pos));
        credentials.setPassword(credentials_.substr(pos + 1));
    }
}

String KafkaSchemaRegistry::fetchSchema(UInt32 id)
{
    try
    {
        try
        {
            Poco::URI url(base_url, std::format("/schemas/ids/{}", id));
            LOG_TRACE(logger, "Fetching schema id = {}", id);

            /// One second for connect/send/receive. Just in case.
            ConnectionTimeouts timeouts({1, 0}, {1, 0}, {1, 0});

            Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, url.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1);
            request.setHost(url.getHost());

            if (!credentials.empty())
                credentials.authenticate(request);

            auto session = makePooledHTTPSession(url, timeouts, 1);
            std::istream * response_body{};
            try
            {
                session->sendRequest(request);

                Poco::Net::HTTPResponse response;
                response_body = receiveResponse(*session, request, response, false);
            }
            catch (const Poco::Exception & e)
            {
                /// We use session data storage as storage for exception text
                /// Depend on it we can deduce to reconnect session or reresolve session host
                session->attachSessionData(e.message());
                throw;
            }
            Poco::JSON::Parser parser;
            auto json_body = parser.parse(*response_body).extract<Poco::JSON::Object::Ptr>();
            auto schema = json_body->getValue<std::string>("schema");
            LOG_TRACE(logger, "Successfully fetched schema id = {}\n{}", id, schema);
            return schema;
        }
        catch (const Exception &)
        {
            throw;
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(Exception::CreateFromPocoTag{}, e);
        }
    }
    catch (Exception & e)
    {
        e.addMessage(std::format("while fetching schema with id {}", id));
        throw;
    }
}

UInt32 KafkaSchemaRegistry::readSchemaId(ReadBuffer & in)
{
    uint8_t magic;
    uint32_t schema_id;

    try
    {
        readBinaryBigEndian(magic, in);
        readBinaryBigEndian(schema_id, in);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
            /// empty or incomplete message without magic byte or schema id
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing magic byte or schema identifier.");
        else
            throw;
    }

    if (magic != 0x00)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid magic byte before schema identifier."
            " Must be zero byte, found 0x{:x} instead", magic);

    return schema_id;
}

}
