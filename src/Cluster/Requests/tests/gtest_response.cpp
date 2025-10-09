#include <Cluster/Requests/ErrorResponse.h>
#include <Cluster/Requests/ResponseHeader.h>

#include <gtest/gtest.h>

TEST(RequestResponse, ResponseSerializeInString)
{
    cluster::ErrorResponse response(3, "test error", /*data_version_=*/1);
    cluster::ResponseHeader header(response.opCode(), response.version(), /*correlation_id=*/11);
    auto data = response.serializeWithHeaderInString(header);

    cluster::ResponseHeader header_got;
    auto response_got = cluster::Response::parseWithHeader(data, header_got);

    EXPECT_EQ(header_got, header);
    EXPECT_EQ(response, *static_cast<cluster::ErrorResponse *>(response_got.get()));
}
