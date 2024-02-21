#include <Formats/KafkaSchemaRegistry.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int TYPE_MISMATCH;
}

KafkaSchemaRegistry::KafkaSchemaRegistry(const String & base_url_, const String & credentials_): base_url(base_url_)
{
    if (credentials_.empty())
        return;

    auto pos = credentials_.find(':');
    if (pos == credentials_.npos)
        credentials.setUsername(credentials_);
    else
    {
        credentials.setUsername(credentials_.substr(0, pos));
        credentials.setPassword(credentials_.substr(pos));
    }
}

String KafkaSchemaRegistry::fetchSchema(UInt32 id, const String & expected_schema_type)
{
    assert(!base_url.empty());

    try
    {
        try
        {
            Poco::URI url(base_url, "/schemas/ids/" + std::to_string(id));
            LOG_TRACE((&Poco::Logger::get("KafkaSchemaRegistry")), "Fetching schema id = {}", id);

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

            if (!expected_schema_type.empty())
            {
                auto schema_type = json_body->getValue<std::string>("type");
                if (boost::iequals(schema_type, expected_schema_type))
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected schema type {}, got {}", expected_schema_type, schema_type);
            }
            auto schema = json_body->getValue<std::string>("schema");
            LOG_TRACE((&Poco::Logger::get("KafkaSchemaRegistry")), "Successfully fetched schema id = {}\n{}", id, schema);
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
        e.addMessage("while fetching schema id = " + std::to_string(id));
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
        {
            /// empty or incomplete message without magic byte or schema id
            throw Exception("Missing magic byte or schema identifier.", ErrorCodes::INCORRECT_DATA);
        }
        else
            throw;
    }

    if (magic != 0x00)
    {
        throw Exception("Invalid magic byte before schema identifier."
            " Must be zero byte, found " + std::to_string(int(magic)) + " instead", ErrorCodes::INCORRECT_DATA);
    }

    return schema_id;
}

}
