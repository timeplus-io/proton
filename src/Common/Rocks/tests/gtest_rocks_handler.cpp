#include <Common/Rocks/RocksHandler.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
/// Keys
std::string_view key_sv{"key_sv"};
std::string key_s("key_s");
int key_i{666};
double key_d{123.123};
struct KeyStruct
{
    int a;
    double b;
} key_struct{1, 2.0};

/// Values
std::string_view value_sv{"value_sv"};
std::string value_s("value_s");
int value_i{999};
double value_d{321.321};
struct ValueStruct
{
    int a;
    double b;
} value_struct{3, 4.0};
}

TEST(RocksHandler, Basic)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    auto rocks = Rocks::createOrLoadIfExists(options, "test_rocks");
    auto handler = rocks->getOrCreateHandler();
    ASSERT_TRUE(handler->getHandleID().empty());

    /// Put key value pairs
    handler->put(key_sv, value_struct);
    handler->put(key_s, value_sv);
    handler->put(key_i, value_s);
    handler->put(key_d, value_i);
    handler->put(key_struct, value_d);

    /// Get key value pairs
    ValueStruct value_struct_out;
    handler->get(key_sv, value_struct_out);
    ASSERT_EQ(value_struct_out.a, value_struct.a);
    ASSERT_EQ(value_struct_out.b, value_struct.b);

    std::string_view value_sv_out;
    handler->get(key_s, value_sv_out);
    ASSERT_EQ(value_sv_out, value_sv);

    std::string value_s_out;
    handler->get(key_i, value_s_out);
    ASSERT_EQ(value_s_out, value_s);

    int value_i_out;
    handler->get(key_d, value_i_out);
    ASSERT_EQ(value_i_out, value_i);

    double value_d_out;
    handler->get(key_struct, value_d_out);
    ASSERT_EQ(value_d_out, value_d);

    /// Remove key
    handler->remove(key_struct);
    ASSERT_FALSE(handler->tryGet(key_struct, value_d_out));

    /// Get non-existing key
    ASSERT_FALSE(handler->tryGet("none", value_d_out));

#ifndef DEBUG_OR_SANITIZER_BUILD
    /// Get value 'ValueStruct' into dismatched type 'double'
    ASSERT_THROW(handler->get(key_sv, value_d_out), DB::Exception);
#endif
}

TEST(RocksHandler, Complex)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    {
        auto rocks = Rocks::createOrLoadIfExists(options, "test_rocks2", /*cleanup=*/false);
        auto default_handler = rocks->getOrCreateHandler();
        ASSERT_EQ(default_handler->getHandleID(), "");
        default_handler->put(key_sv, value_struct);

        auto handler1 = rocks->getOrCreateHandler("handler1");
        ASSERT_EQ(handler1->getHandleID(), "handler1");
        handler1->put(key_s, value_sv);
        handler1->put(key_i, value_s);

        auto handler2 = rocks->getOrCreateHandler("handler2");
        ASSERT_EQ(handler2->getHandleID(), "handler2");
        handler2->put(key_d, value_i);
        handler2->put(key_struct, value_d);
    }

    /// Reopen the rocks
    auto rocks = Rocks::createOrLoadIfExists(
        options,
        "test_rocks2",
        /*cleanup=*/true,
        getLogger("RocksHandler"));

    {
        auto default_handler = rocks->getOrCreateHandler();
        ValueStruct value_struct_out;
        default_handler->get(key_sv, value_struct_out);
        ASSERT_EQ(value_struct_out.a, value_struct.a);
        ASSERT_EQ(value_struct_out.b, value_struct.b);

        /// Get non-existing key in default handler
        std::string_view value_sv_out;
        ASSERT_FALSE(default_handler->tryGet(key_s, value_sv_out));
    }

    {
        /// Case: Multiple same handler instances
        auto handler1_x = rocks->getOrCreateHandler("handler1");
        std::string_view value_sv_out;
        handler1_x->get(key_s, value_sv_out);
        ASSERT_EQ(value_sv_out, value_sv);

        std::string value_s_out;
        auto handler1_y = rocks->getOrCreateHandler("handler1");
        handler1_y->get(key_i, value_s_out);
        ASSERT_EQ(value_s_out, value_s);
    }

    {
        /// Case: Destroy handler and create a new one
        auto handler2_x = rocks->getOrCreateHandler("handler2");
        int value_i_out;
        handler2_x->get(key_d, value_i_out);
        ASSERT_EQ(value_i_out, value_i);
        handler2_x->destroy();

        auto handler2_y = rocks->getOrCreateHandler("handler2");
        ASSERT_FALSE(handler2_y->tryGet(key_d, value_i_out));

        handler2_y->put(key_d, value_i);
        handler2_y->get(key_d, value_i_out);
        ASSERT_EQ(value_i_out, value_i);
    }
}
