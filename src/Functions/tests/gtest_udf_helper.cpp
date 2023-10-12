#include <Functions/UserDefined/UDFHelper.h>

#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Util/JSONConfiguration.h>

#include <map>

using namespace DB;

TEST(createUserDefinedExecutableFunction, ValidJSFunc)
{
    String JS_FUNC1 = R"###(
{
    "function": {
        "type": "javascript",
        "name": "func1",
        "arguments": [
            {
                "name": "value",
                "type": "float32"
            }
        ],
        "return_type": "float32",
        "source": "{function add_five(value){for(let i=0;i<value.length;i++){value[i]=value[i]+5}return value}}"
    }
})###";

    auto context = getContext().context;
    Poco::JSON::Parser parser;

    auto json_func = parser.parse(JS_FUNC1).extract<Poco::JSON::Object::Ptr>();
    auto cfg = Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func));
    EXPECT_NE(cfg, nullptr);
}

TEST(createUserDefinedExecutableFunction, MissingType)
{
    String JS_FUNC1 = R"###(
{
    "function": {
        "name": "func1",
        "arguments": [
            {
                "name": "value",
                "type": "float32"
            }
        ],
        "return_type": "float32",
        "source": "{function add_five(value){for(let i=0;i<value.length;i++){value[i]=value[i]+5}return value}}"
    }
})###";

    auto context = getContext().context;
    Poco::JSON::Parser parser;

    auto json_func = parser.parse(JS_FUNC1).extract<Poco::JSON::Object::Ptr>();
    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);
}

TEST(createUserDefinedExecutableFunction, MissingReturnType)
{
    String JS_FUNC1 = R"###(
{
    "function": {
        "name": "func1",
        "type": "javascript",
        "arguments": [
            {
                "name": "value",
                "type": "float32"
            }
        ],
        "source": "{function add_five(value){for(let i=0;i<value.length;i++){value[i]=value[i]+5}return value}}"
    }
})###";

    auto context = getContext().context;
    Poco::JSON::Parser parser;

    auto json_func = parser.parse(JS_FUNC1).extract<Poco::JSON::Object::Ptr>();
    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);
}

TEST(createUserDefinedExecutableFunction, InvalidArguments)
{
    String FUNC_NO_ARGS = R"###(
{
    "function": {
        "type": "remote",
        "name": "func1",
        "return_type": "float32",
        "url": "https://test.on.aws/"
    }
})###";

    String FUNC_EMPTY_ARGS = R"###(
{
    "function": {
        "type": "remote",
        "name": "func1",
        "arguments": [],
        "return_type": "float32",
        "url": "https://test.on.aws/"
    }
})###";

    String FUNC_INVALID_ARG_TYPE = R"###(
{
    "function": {
        "type": "remote",
        "name": "func1",
        "arguments": [
            {
                "name": "value",
                "type": "double1"
            }
        ],
        "return_type": "float32",
        "url": "https://test.on.aws/"
    }
})###";
    auto context = getContext().context;
    Poco::JSON::Parser parser;

    auto json_func = parser.parse(FUNC_NO_ARGS).extract<Poco::JSON::Object::Ptr>();
    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);

    json_func = parser.parse(FUNC_EMPTY_ARGS).extract<Poco::JSON::Object::Ptr>();
    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);

    json_func = parser.parse(FUNC_INVALID_ARG_TYPE).extract<Poco::JSON::Object::Ptr>();
    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);
}

TEST(createUserDefinedExecutableFunction, MissingCommand)
{
    String JS_FUNC1 = R"###(
{
    "function": {
        "type": "executable",
        "name": "func1",
        "arguments": [
            {
                "name": "value",
                "type": "float32"
            }
        ],
        "return_type": "float32"
    }
})###";

    auto context = getContext().context;
    Poco::JSON::Parser parser;
    auto func = parser.parse(JS_FUNC1).extract<Poco::JSON::Object::Ptr>();

    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(func)), Exception);
}

TEST(createUserDefinedExecutableFunction, MissingURL)
{
    String REMOTE_NO_URL = R"###(
{
    "function": {
        "type": "remote",
        "name": "remote",
        "arguments": [
            {
                "name": "features",
                "type": "array(float32)"
            },
            {
                "name": "model",
                "type": "string"
            }
        ],
        "return_type": "float32",
        "auth_method": "auth_header",
        "auth_context": {
            "key_name": "auth",
            "key_value": "proton"
        }
    }
})###";

    String REMOTE_INVALID_URL = R"###(
{
    "function": {
        "type": "remote",
        "name": "remote",
        "arguments": [
            {
                "name": "features",
                "type": "array(float32)"
            },
            {
                "name": "model",
                "type": "string"
            }
        ],
        "return_type": "float32",
        "url": ":",
        "auth_method": "auth_header",
        "auth_context": {
            "key_name": "auth",
            "key_value": "proton"
        }
    }
})###";

    auto context = getContext().context;
    Poco::JSON::Parser parser;

    auto json_func = parser.parse(REMOTE_NO_URL).extract<Poco::JSON::Object::Ptr>();
    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);

    json_func = parser.parse(REMOTE_INVALID_URL).extract<Poco::JSON::Object::Ptr>();
    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);
}

TEST(createUserDefinedExecutableFunction, InvalidAuthMethod)
{
    String REMOTE_MISSING_KEY = R"###(
{
    "function": {
        "type": "remote",
        "name": "remote",
        "arguments": [
            {
                "name": "features",
                "type": "array(float32)"
            },
            {
                "name": "model",
                "type": "string"
            }
        ],
        "return_type": "float32",
        "url": "https://test.lambda-url.us-west-1.on.aws/",
        "auth_method": "auth_header",
        "auth_context": {
        }
    }
})###";

    String REMOTE_MISSING_AUTH_CONTEXT = R"###(
{
    "function": {
        "type": "remote",
        "name": "remote",
        "arguments": [
            {
                "name": "features",
                "type": "array(float32)"
            },
            {
                "name": "model",
                "type": "string"
            }
        ],
        "return_type": "float32",
        "url": "https://test.lambda-url.us-west-1.on.aws/",
        "auth_method": "auth_header"
    }
})###";

    auto context = getContext().context;
    Poco::JSON::Parser parser;

    auto json_func = parser.parse(REMOTE_MISSING_KEY).extract<Poco::JSON::Object::Ptr>();
    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);

    json_func = parser.parse(REMOTE_MISSING_AUTH_CONTEXT).extract<Poco::JSON::Object::Ptr>();
    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);
}

TEST(createUserDefinedExecutableFunction, MissingSource)
{
    String JS_FUNC1 = R"###(
{
    "function": {
        "type": "javascript",
        "name": "func1",
        "arguments": [
            {
                "name": "value",
                "type": "float32"
            }
        ],
        "return_type": "float32"
    }
})###";

    auto context = getContext().context;
    Poco::JSON::Parser parser;
    auto json_func = parser.parse(JS_FUNC1).extract<Poco::JSON::Object::Ptr>();

    ASSERT_THROW(Streaming::createUserDefinedExecutableFunction(context, "func1", Poco::Util::JSONConfiguration(json_func)), Exception);
}