#include <gtest/gtest.h>

#include "config.h"

#if USE_AWS_S3

#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <utility>

#include <Common/RemoteHostFilter.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/S3/Client.h>
#include <IO/SessionAwareIOStream.h>
#include <Poco/Net/HTTPBasicStreamBuf.h>

class CountedSession
{
public:
    CountedSession() { ++total; }
    CountedSession(const CountedSession &) { ++total; }
    ~CountedSession() { --total; }

    static int outstandingObjects() { return total; }

private:
    static int total;
};

int CountedSession::total = 0;

using CountedSessionPtr = std::shared_ptr<CountedSession>;

class StringHTTPBasicStreamBuf : public Poco::Net::HTTPBasicStreamBuf
{
public:
    explicit StringHTTPBasicStreamBuf(std::string body)
        : BasicBufferedStreamBuf(body.size(), IOS::in), body_stream(std::stringstream(std::move(body)))
    {}

private:
    std::stringstream body_stream;

    int readFromDevice(char_type * buf, std::streamsize n) override
    {
        body_stream.read(buf, n);
        return static_cast<int>(body_stream.gcount());
    }
};

/// proton: starts.
/// Throws Poco::TimeoutException on the first readFromDevice call, then behaves like
/// StringHTTPBasicStreamBuf. Mirrors the failure mode in issue 12036 where the SSL socket
/// recv inside Poco fires SO_RCVTIMEO mid-body.
class TimeoutOnceThenStringStreamBuf : public Poco::Net::HTTPBasicStreamBuf
{
public:
    explicit TimeoutOnceThenStringStreamBuf(std::string body)
        : BasicBufferedStreamBuf(body.size(), IOS::in), body_stream(std::stringstream(std::move(body)))
    {}

private:
    std::stringstream body_stream;
    int call_count = 0;

    int readFromDevice(char_type * buf, std::streamsize n) override
    {
        if (call_count++ == 0)
            throw Poco::TimeoutException("simulated S3 socket read timeout");
        body_stream.read(buf, n);
        return static_cast<int>(body_stream.gcount());
    }
};
/// proton: ends.

using GetObjectFn = std::function<Aws::S3::Model::GetObjectOutcome(const Aws::S3::Model::GetObjectRequest & request)>;

struct ClientFake : DB::S3::Client
{
    explicit ClientFake()
        : DB::S3::Client(
              1,
              DB::S3::ServerSideEncryptionKMSConfig(),
              std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>("test_access_key", "test_secret"),
              DB::S3::ClientFactory::instance().createClientConfiguration(
                  "test_region", DB::RemoteHostFilter(), 1, false, false, {}, {}, "http"),
              Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              DB::S3::ClientSettings(),
              nullptr)
    {}

    std::optional<GetObjectFn> getObjectImpl;

    void setGetObjectSuccess(const std::shared_ptr<CountedSession> & session, std::streambuf * sb)
    {
        std::weak_ptr weak_session_ptr = session;
        getObjectImpl
            = [weak_session_ptr, sb]([[maybe_unused]] const Aws::S3::Model::GetObjectRequest & request) -> Aws::S3::Model::GetObjectOutcome
        {
            auto responseStream = Aws::Utils::Stream::ResponseStream(
                Aws::New<DB::SessionAwareIOStream<CountedSessionPtr>>("test response stream", weak_session_ptr.lock(), sb));
            Aws::AmazonWebServiceResult awsStream(std::move(responseStream), Aws::Http::HeaderValueCollection());
            DB::S3::Model::GetObjectResult getObjectResult(std::move(awsStream));
            return DB::S3::Model::GetObjectOutcome(std::move(getObjectResult));
        };
    }

    Aws::S3::Model::GetObjectOutcome GetObject([[maybe_unused]] const Aws::S3::Model::GetObjectRequest & request) const override
    {
        assert(getObjectImpl);
        return (*getObjectImpl)(request);
    }
};

static void readAndAssert(DB::ReadBuffer & buf, const char * str)
{
    size_t n = strlen(str);
    std::vector<char> tmp(n);
    buf.readStrict(tmp.data(), n);
    ASSERT_EQ(strncmp(tmp.data(), str, n), 0);
}

TEST(ReadBufferFromS3Test, RetainsSessionWhenPending)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings readSettings;
    readSettings.remote_fs_buffer_size = 2;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3Settings::RequestSettings(), readSettings);

    auto session = std::make_shared<CountedSession>();
    auto streamBuf = std::make_shared<StringHTTPBasicStreamBuf>("123456789");
    client->setGetObjectSuccess(session, streamBuf.get());

    readAndAssert(subject, "123");

    session.reset();
    ASSERT_EQ(CountedSession::outstandingObjects(), 1);

    readAndAssert(subject, "45");
    ASSERT_EQ(CountedSession::outstandingObjects(), 1);
}

TEST(ReadBufferFromS3Test, ReleaseSessionWhenStreamEof)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings readSettings;
    readSettings.remote_fs_buffer_size = 10;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3Settings::RequestSettings(), readSettings);

    auto session = std::make_shared<CountedSession>();
    const auto streamBuf = std::make_shared<StringHTTPBasicStreamBuf>("1234");
    client->setGetObjectSuccess(session, streamBuf.get());

    readAndAssert(subject, "1234");

    session.reset();
    ASSERT_EQ(CountedSession::outstandingObjects(), 0);

    ASSERT_TRUE(subject.eof());
    ASSERT_FALSE(subject.nextImpl());
}

/// proton: starts.
/// Regression test for issue 12036 / upstream ClickHouse 896cbb6a756.
/// The SSL socket inside Poco can throw a raw Poco::TimeoutException mid-body when
/// SO_RCVTIMEO fires. Before the fix, ReadBufferFromS3::nextImpl() only caught
/// DB::Exception and let Poco::TimeoutException escape, killing merges/queries.
/// After the fix it catches Poco::Exception and the retry loop recovers.
TEST(ReadBufferFromS3Test, RetriesOnPocoTimeoutException)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings readSettings;
    readSettings.remote_fs_buffer_size = 10;

    DB::S3Settings::RequestSettings request_settings;
    request_settings.max_single_read_retries = 3;

    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", request_settings, readSettings);

    auto session = std::make_shared<CountedSession>();
    const auto streamBuf = std::make_shared<TimeoutOnceThenStringStreamBuf>("1234");
    client->setGetObjectSuccess(session, streamBuf.get());

    readAndAssert(subject, "1234");
}
/// proton: ends.

TEST(ReadBufferFromS3Test, ReleaseSessionWhenReadUntilPosition)
{
    const auto client = std::make_shared<ClientFake>();
    DB::ReadSettings readSettings;
    readSettings.remote_fs_buffer_size = 2;
    auto subject = DB::ReadBufferFromS3(client, "test_bucket", "test_key", "test_version_id", DB::S3Settings::RequestSettings(), readSettings);

    auto session = std::make_shared<CountedSession>();
    const auto streamBuf = std::make_shared<StringHTTPBasicStreamBuf>("123456");
    client->setGetObjectSuccess(session, streamBuf.get());

    subject.setReadUntilPosition(4);
    readAndAssert(subject, "1234");

    session.reset();
    ASSERT_EQ(CountedSession::outstandingObjects(), 0);

    ASSERT_TRUE(subject.eof());
    ASSERT_FALSE(subject.nextImpl());
}

#endif
