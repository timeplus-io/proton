#include <AggregateFunctions/AggregateFunctionJavaScriptAdapter.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/BlockUtils.h>

#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/IExternalLoadable.h>
#include <Interpreters/UserDefinedFunctionConfiguration.h>

#include <gtest/gtest.h>
#include <Poco/JSON/Parser.h>

using namespace DB;

std::unique_ptr<v8::Platform> platform;

String ARGS_CEP1 = R"###([{ "name": "value","type": "int64"}])###";
String RETURN_CEP1 = "uint32";
String UDA_CEP1 = R"###(
{
    last_down_price: -1.0,
    down_duration: 0,
    result: [],

    add: function(prices) {
        var emit = false;
        for (let i = 0; i < prices.length; i++) {
            if (this.last_down_price < 0 || prices[i] <= this.last_down_price) {
                this.last_down_price = prices[i];
                this.down_duration = this.down_duration + 1;
            } else {
                this.result.push(this.down_duration);
                this.last_down_price = prices[i];
                this.down_duration = 1
                emit = true;
            }
        }
        return emit;
    },
    finalize: function () {
        var old_result = this.result;
        this.result = [];
        return old_result;
    },
    merge: function(state_str) {
    }
}
)###";

String ARGS_UDA1 = R"###([{ "name": "value","type": "int64"}])###";
String RETURN_UDA1 = "float32";
String UDA1 = R"###(
{
    max: -1.0,
    sec: -1.0,

    add: function (values) {
        for (let i = 0; i < values.length; i++) {
            if (values[i] > this.max) {
                this.sec = this.max;
                this.max = values[i];
            }

            if (values[i] < this.max && values[i] > this.sec)
                this.sec = values[i];
        }
    },
    finalize: function () {
        return [this.sec];
    },
    serialize: function () {
        let s = {
            'max': this.max,
            'sec': this.sec
        };

        return JSON.stringify(s);
    },
    deserialize: function (state_str) {
        let s = JSON.parse(state_str);
        this.max = s['max'];
        this.sec = s['sec'];
    },
    merge: function (state_str) {
        let s = JSON.parse(state_str);

        if (s['sec'] >= this.max) {
            this.max = s['max'];
            this.sec = s['sec'];
        } else if (s['max'] >= this.max) {
            this.sec = this.max;
            this.max = s['max'];
        } else if (s['max'] > this.sec) {
            this.sec = s['max']
        }
    }
}
)###";

void initV8()
{
    v8::V8::SetFlagsFromString("--single-threaded");
    platform = v8::platform::NewSingleThreadedDefaultPlatform();
    v8::V8::InitializePlatform(platform.get());
    v8::V8::Initialize();
}

void disposeV8()
{
    v8::V8::Dispose();
    v8::V8::ShutdownPlatform();
}

class UDATestCase : public ::testing::Test
{
public:
    static void SetUpTestSuite() { initV8(); }
    static void TearDownTestSuite() { disposeV8(); }
};

UserDefinedFunctionConfiguration
createUDFConfig(const String & name, const String & arg_str, const String & return_type, const String & source)
{
    DataTypePtr result_type = DataTypeFactory::instance().get(return_type);
    ExternalLoadableLifetime lifetime;

    std::vector<UserDefinedFunctionConfiguration::Argument> arguments;
    if (!arg_str.empty())
    {
        Poco::JSON::Parser parser;
        try
        {
            auto json_arguments = parser.parse(arg_str).extract<Poco::JSON::Array::Ptr>();
            for (unsigned int i = 0; i < json_arguments->size(); i++)
            {
                UserDefinedFunctionConfiguration::Argument argument;
                argument.name = json_arguments->getObject(i)->get("name").toString();
                argument.type = DataTypeFactory::instance().get(json_arguments->getObject(i)->get("type").toString());
                arguments.emplace_back(std::move(argument));
            }
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid UDF config");
        }
    }

    UserDefinedFunctionConfiguration function_configuration{
        .type = UserDefinedFunctionConfiguration::FuncType::JAVASCRIPT, //-V1030
        .arguments = std::move(arguments),
        .source = source,
        .is_aggregation = true,
        .has_user_defined_emit_strategy = true,
        .name = name, //-V1030
        .result_type = std::move(result_type), //-V1030
    };

    return function_configuration;
};

DataTypes getDataTypes(const String & arguments)
{
    Poco::JSON::Parser parser;
    auto json_arguments = parser.parse(arguments).extract<Poco::JSON::Array::Ptr>();

    DataTypes types;
    types.reserve(json_arguments->size());
    for (unsigned int i = 0; i < json_arguments->size(); i++)
        types.emplace_back(DataTypeFactory::instance().get(json_arguments->getObject(i)->get("type").toString()));

    return types;
}

TEST_F(UDATestCase, add)
{
    auto config = createUDFConfig("down", ARGS_CEP1, RETURN_CEP1, UDA_CEP1);
    DataTypes types = getDataTypes(ARGS_CEP1);
    Array params;
    size_t max_heap_size = 100 * 1024 * 1024;
    auto aggr_function = AggregateFunctionJavaScriptAdapter(config, types, params, max_heap_size);

    std::unique_ptr<AggregateFunctionJavaScriptAdapter::Data[], AggregateFunctionJavaScriptAdapter::DataDeleter> places{
        static_cast<AggregateFunctionJavaScriptAdapter::Data *>(malloc(aggr_function.sizeOfData())),
        AggregateFunctionJavaScriptAdapter::DataDeleter{}};

    auto * data_ptr = reinterpret_cast<char *>(&places[0]);

    aggr_function.create(data_ptr);

    /// prepare input data
    std::vector<std::pair<String, std::vector<String>>> string_cols;
    std::vector<std::pair<String, std::vector<Int64>>> int64_cols = {{"int64", {5, 4, 6}}};
    Block block = Streaming::buildBlock(string_cols, int64_cols);
    ColumnRawPtrs columns;
    for (auto & col : block.getColumns())
        columns.emplace_back(col.get());

    aggr_function.add(data_ptr, columns.data(), 0, nullptr);

    ASSERT_EQ(aggr_function.flush(data_ptr), false);

    aggr_function.add(data_ptr, columns.data(), 1, nullptr);
    aggr_function.add(data_ptr, columns.data(), 2, nullptr);
    ASSERT_EQ(aggr_function.flush(data_ptr), true);

    auto result_col = aggr_function.getReturnType()->createColumn();
    result_col->reserve(1);
    aggr_function.insertResultInto(data_ptr, *result_col, nullptr);
    ASSERT_EQ(result_col->getUInt(0), 2);
}

TEST_F(UDATestCase, CheckPoint)
{
    auto config = createUDFConfig("sec_large", ARGS_UDA1, RETURN_UDA1, UDA1);
    DataTypes types = getDataTypes(ARGS_UDA1);
    Array params;
    size_t max_heap_size = 100 * 1024 * 1024;
    auto aggr_function = AggregateFunctionJavaScriptAdapter(config, types, params, max_heap_size);

    std::unique_ptr<AggregateFunctionJavaScriptAdapter::Data[], AggregateFunctionJavaScriptAdapter::DataDeleter> places{
        static_cast<AggregateFunctionJavaScriptAdapter::Data *>(malloc(aggr_function.sizeOfData())),
        AggregateFunctionJavaScriptAdapter::DataDeleter{}};

    auto * data_ptr = reinterpret_cast<char *>(&places[0]);
    aggr_function.create(data_ptr);

    /// prepare input data
    std::vector<std::pair<String, std::vector<String>>> string_cols;
    std::vector<std::pair<String, std::vector<Int64>>> int64_cols = {{"int64", {5, 4}}};
    Block block = Streaming::buildBlock(string_cols, int64_cols);
    ColumnRawPtrs columns;
    for (auto & col : block.getColumns())
        columns.emplace_back(col.get());

    aggr_function.add(data_ptr, columns.data(), 0, nullptr);
    aggr_function.flush(data_ptr);

    WriteBufferFromOwnString out;
    aggr_function.serialize(data_ptr, out, 0);
    ASSERT_EQ(out.str(), "\x12{\"max\":5,\"sec\":-1}");

    String in{out.str()};
    ReadBufferFromString rb(in);
    aggr_function.deserialize(data_ptr, rb, 0, nullptr);

    aggr_function.add(data_ptr, columns.data(), 1, nullptr);
    aggr_function.flush(data_ptr);

    WriteBufferFromOwnString out1;
    aggr_function.serialize(data_ptr, out1, 0);
    ASSERT_EQ(out1.str(), "\x11{\"max\":5,\"sec\":4}");
}

TEST_F(UDATestCase, Merge)
{
    auto config = createUDFConfig("sec_large", ARGS_UDA1, RETURN_UDA1, UDA1);
    DataTypes types = getDataTypes(ARGS_UDA1);
    Array params;
    size_t max_heap_size = 100 * 1024 * 1024;
    auto aggr_function = AggregateFunctionJavaScriptAdapter(config, types, params, max_heap_size);

    std::unique_ptr<AggregateFunctionJavaScriptAdapter::Data[], AggregateFunctionJavaScriptAdapter::DataDeleter> places{
        static_cast<AggregateFunctionJavaScriptAdapter::Data *>(malloc(2 * aggr_function.sizeOfData())),
        AggregateFunctionJavaScriptAdapter::DataDeleter{}};

    auto * data_ptr = reinterpret_cast<char *>(&places[0]);
    auto * data1_ptr = reinterpret_cast<char *>(&places[1]);
    aggr_function.create(data_ptr);
    aggr_function.create(data1_ptr);

    /// prepare input data
    std::vector<std::pair<String, std::vector<String>>> string_cols;
    std::vector<std::pair<String, std::vector<Int64>>> int64_cols = {{"int64", {5, 4, 8, 3}}};
    Block block = Streaming::buildBlock(string_cols, int64_cols);
    ColumnRawPtrs columns;
    for (auto & col : block.getColumns())
        columns.emplace_back(col.get());

    aggr_function.add(data_ptr, columns.data(), 0, nullptr);
    aggr_function.add(data_ptr, columns.data(), 1, nullptr);
    aggr_function.flush(data_ptr);

    aggr_function.add(data1_ptr, columns.data(), 2, nullptr);
    aggr_function.add(data1_ptr, columns.data(), 3, nullptr);
    aggr_function.flush(data1_ptr);

    aggr_function.merge(data_ptr, data1_ptr, nullptr);

    WriteBufferFromOwnString out;
    aggr_function.serialize(data_ptr, out, 0);
    ASSERT_EQ(out.str(), "\x11{\"max\":8,\"sec\":5}");
}