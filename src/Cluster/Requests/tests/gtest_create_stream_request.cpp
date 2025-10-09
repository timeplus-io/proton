#include <Cluster/Requests/CreateStreamRequest.h>

#include <Core/UUID.h>
#include <IO/ReadBufferFromString.h>

#include <gtest/gtest.h>

TEST(RequestResponse, CreateStreamRequest)
{
    cluster::CreateStreamRequest request(
        "default",
        "test_stream",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "ATTACH STREAM test(i int);",
        "CREATE STREAM test(i int)",
        /*initiator_=*/1,
        /*timeout_ms_=*/1000,
        /*request_version_=*/1);

    auto & data = request.data();
    /// Init request to make it valid
    data.setFlushMessages(1000);
    data.setFlushInterval(5000);

    /// Serialize with header. We will need make sure the request header's
    /// opCode and version matches the Request' opCode and version.
    /// In production, on the wire, we always serialize with header
    /// Note, header version itself is not on the wire but calculate lively
    /// according to Request OpCode and request version
    cluster::RequestHeader request_header(request.opCode(), /*request_version=*/1, 101);
    auto serialized_with_header = request.serializeWithHeader(request_header);

    DB::ReadBufferFromString rb(serialized_with_header);

    /// First parse header
    auto request_header_des = cluster::RequestHeader::parse(rb);
    EXPECT_EQ(request_header_des.opCode(), request_header.opCode());
    EXPECT_EQ(request_header_des.requestVersion(), request_header.requestVersion());
    EXPECT_EQ(request_header_des.correlationID(), request_header.correlationID());

    /// Then parse request body
    auto request_des = cluster::Request::parse(request_header_des.requestOpCode(), request_header_des.requestVersion(), rb);
    ASSERT_EQ(request_des->opCode(), request.opCode());
    const auto & data_des = static_cast<cluster::CreateStreamRequest *>(request_des.get())->data();
    EXPECT_EQ(data_des.sql_ddl, data.sql_ddl);
    EXPECT_EQ(data_des.desc, data.desc);
    EXPECT_EQ(data_des.timeout_ms, data.timeout_ms);
}
