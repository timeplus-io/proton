#include <gtest/gtest.h>

#include <Interpreters/ClientInfo.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

TEST(ClientInfo, DefaultConstructedHasAddresses)
{
    ClientInfo client_info;

    ASSERT_TRUE(client_info.current_address);
    ASSERT_TRUE(client_info.initial_address);

    EXPECT_NO_THROW(client_info.current_address->host());
    EXPECT_NO_THROW(client_info.current_address->port());
    EXPECT_NO_THROW(client_info.initial_address->host());
    EXPECT_NO_THROW(client_info.initial_address->port());
}

TEST(ClientInfo, SerializationPreservesInitialAddress)
{
    ClientInfo client_info;
    client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
    client_info.initial_user = "test_user";
    client_info.initial_query_id = "test_query_id";
    *client_info.initial_address = Poco::Net::SocketAddress{"127.0.0.1", 9000};
    client_info.initial_query_start_time_microseconds = Decimal64{1234567};
    client_info.interface = ClientInfo::Interface::TCP;

    String serialized;
    WriteBufferFromString out(serialized);
    client_info.write(out, /*server_protocol_revision=*/0);
    out.finalize();

    ClientInfo roundtrip;
    ReadBufferFromString in(serialized);
    roundtrip.read(in, /*client_protocol_revision=*/0);

    ASSERT_TRUE(roundtrip.initial_address);
    EXPECT_EQ(roundtrip.initial_address->toString(), client_info.initial_address->toString());
    EXPECT_EQ(roundtrip.initial_user, client_info.initial_user);
    EXPECT_EQ(roundtrip.initial_query_id, client_info.initial_query_id);
}

