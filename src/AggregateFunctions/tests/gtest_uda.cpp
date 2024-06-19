#include <AggregateFunctions/AggregateFunctionJavaScriptAdapter.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/BlockUtils.h>
#include <V8/ConvertDataTypes.h>
#include <V8/Utils.h>

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/UserDefined/UserDefinedFunctionConfiguration.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/IExternalLoadable.h>

#include <gtest/gtest.h>
#include <Poco/JSON/Parser.h>

using namespace DB;

std::unique_ptr<v8::Platform> platform;

String ARGS_CEP1 = R"###([{ "name": "value","type": "int64"}, {"name": "_tp_delta", "type": "int8"}])###";
String RETURN_CEP1 = "uint32";
String UDA_CEP1 = R"###(
{
    initialize : function() {
        this.last_down_price = -1.0;
        this.down_duration = 0;
        this.result = [];
    },
    process: function(prices) {
        for (let i = 0; i < prices.length; i++) {
            if (this.last_down_price < 0 || prices[i] <= this.last_down_price) {
                this.last_down_price = prices[i];
                this.down_duration = this.down_duration + 1;
            } else {
                this.result.push(this.down_duration);
                this.last_down_price = prices[i];
                this.down_duration = 1;
            }
        }
        return this.result.length;
    },
    finalize: function () {
        var old_result = this.result;
        this.result = [];
        return old_result;
    },
    has_customized_emit : true
})###";

String ARGS_UDA1 = R"###([{ "name": "value","type": "int64"}, {"name": "_tp_delta", "type": "int8"}])###";
String RETURN_UDA1 = "float32";
String UDA1 = R"###(
{
    initialize: function() {
        this.max = -1.0;
        this.sec = -1.0;
    },
    process: function (values) {
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
        return this.sec;
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
})###";

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
    v8::V8::DisposePlatform();
}

class UDATestCase : public ::testing::Test
{
public:
    static void SetUpTestSuite() { initV8(); }
    static void TearDownTestSuite() { disposeV8(); }
};

v8::Local<v8::Value> createV8Array(v8::Isolate * isolate, bool is_empty_array)
{
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    v8::Local<v8::Value> result = v8::Array::New(isolate, 1);
    v8::Local<v8::Array> elem1;
    if (is_empty_array)
    {
        elem1 = v8::Array::New(isolate, 0);
    }
    else
    {
        elem1 = v8::Array::New(isolate, 2);
        elem1->Set(context, 0, V8::to_v8(isolate, 1)).FromJust();
        elem1->Set(context, 1, V8::to_v8(isolate, 2)).FromJust();
    }
    v8::Local<v8::Array> elem2 = v8::Array::New(isolate, 3);
    elem2->Set(context, 0, V8::to_v8(isolate, 3)).FromJust();
    elem2->Set(context, 1, V8::to_v8(isolate, 4)).FromJust();
    elem2->Set(context, 2, V8::to_v8(isolate, 5)).FromJust();
    result.As<v8::Array>()->Set(context, 0, elem1).FromJust();
    result.As<v8::Array>()->Set(context, 1, elem2).FromJust();
    return scope.Escape(result);
}

JavaScriptUserDefinedFunctionConfigurationPtr
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

    auto function_configuration = std::make_shared<JavaScriptUserDefinedFunctionConfiguration>();
    function_configuration->source = source;
    function_configuration->is_aggregation = true;
    function_configuration->name = name;
    function_configuration->result_type = std::move(result_type);
    function_configuration->type = UserDefinedFunctionConfiguration::FuncType::JAVASCRIPT;
    function_configuration->arguments = std::move(arguments);

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
    auto aggr_function = AggregateFunctionJavaScriptAdapter(config, types, params, false, max_heap_size);

    ASSERT_TRUE(aggr_function.hasUserDefinedEmit());

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
    auto aggr_function = AggregateFunctionJavaScriptAdapter(config, types, params, false, max_heap_size);

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
    auto aggr_function = AggregateFunctionJavaScriptAdapter(config, types, params, false, max_heap_size);

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

using CREATE_V8_DATA_FUNC = std::function<v8::Local<v8::Value>(v8::Isolate *)>;
using CHECK_COL_FUNC = std::function<void(MutableColumnPtr &)>;

void checkV8ReturnResult(String type, bool is_result_array, CREATE_V8_DATA_FUNC create_fn, CHECK_COL_FUNC check_fn)
{
    v8::Isolate::CreateParams isolate_params;
    isolate_params.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    v8::Isolate * isolate = v8::Isolate::New(isolate_params);
    SCOPE_EXIT({ isolate->Dispose(); });
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);
    v8::TryCatch try_catch(isolate);
    /// try_catch.SetVerbose(true);
    try_catch.SetCaptureMessage(true);

    v8::Local<v8::Context> local_ctx = v8::Context::New(isolate);
    v8::Context::Scope context_scope(local_ctx);

    auto array_type_ptr = DataTypeFactory::instance().get(type);
    auto col_ptr = array_type_ptr->createColumn();

    auto result = create_fn(isolate);
    V8::insertResult(isolate, *col_ptr, array_type_ptr, result, is_result_array);

    check_fn(col_ptr);
}

TEST_F(UDATestCase, IntArray)
{
    /// prepare v8 data
    auto create_fn =[](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, 3)).FromJust();
        result.As<v8::Array>()->Set(context, 1, V8::to_v8(isolate, 4)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, 5)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr)
    {
        ASSERT_EQ(col_ptr->size(), 1);
        Field result_elem1;
        col_ptr->get(0, result_elem1);
        ASSERT_EQ(result_elem1.get<Array &>().size(), 3);
    };

    checkV8ReturnResult("array(int64)", false, create_fn, check_fn);
}

TEST_F(UDATestCase, ArrayInArray)
{
    /// prepare v8 data
    auto create_fn =[](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        return createV8Array(isolate, false);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr)
    {
        ASSERT_EQ(col_ptr->size(), 2);
        Field result_elem1;
        Field result_elem2;
        col_ptr->get(0, result_elem1);
        col_ptr->get(1, result_elem2);
        ASSERT_EQ(result_elem1.get<Array &>().size(), 2);
        ASSERT_EQ(result_elem2.get<Array &>().size(), 3);
    };

    checkV8ReturnResult("array(int64)", true, create_fn, check_fn);
}

TEST_F(UDATestCase, EmptyArray)
{
    /// prepare v8 data
    auto create_fn =[](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        return createV8Array(isolate, true);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr)
    {
        ASSERT_EQ(col_ptr->size(), 2);
        Field result_elem1;
        Field result_elem2;
        col_ptr->get(0, result_elem1);
        col_ptr->get(1, result_elem2);
        ASSERT_EQ(result_elem1.get<Array &>().size(), 0);
        ASSERT_EQ(result_elem2.get<Array &>().size(), 3);
    };

    checkV8ReturnResult("array(int64)", true, create_fn, check_fn);
}

TEST_F(UDATestCase, NestedArray)
{
    /// prepare v8 data
    auto create_fn =[](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        return createV8Array(isolate, true);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr)
    {
        ASSERT_EQ(col_ptr->size(), 1);
        Field result_elem1;
        col_ptr->get(0, result_elem1);
        ASSERT_EQ(result_elem1.get<Array &>().size(), 2);
    };

    checkV8ReturnResult("array(array(int64))", false, create_fn, check_fn);
}

TEST_F(UDATestCase, Bool)
{
    /// prepare v8 data
    auto create_fn =[](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        return V8::to_v8(isolate, true);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr)
    {
        ASSERT_EQ(col_ptr->size(), 1);
        ASSERT_EQ(col_ptr->getBool(0), true);
    };

    checkV8ReturnResult("bool", false, create_fn, check_fn);
}

TEST_F(UDATestCase, BoolArray)
{
    /// prepare v8 data
    auto create_fn =[](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, true)).FromJust();
        result.As<v8::Array>()->Set(context, 1, V8::to_v8(isolate, false)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, true)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr)
    {
        ASSERT_EQ(col_ptr->size(), 3);
        ASSERT_EQ(col_ptr->getBool(0), true);
        ASSERT_EQ(col_ptr->getBool(1), false);
        ASSERT_EQ(col_ptr->getBool(2), true);
    };

    checkV8ReturnResult("bool", true, create_fn, check_fn);
}

TEST_F(UDATestCase, returnNumberForBool)
{
    /// prepare v8 data
    auto create_fn =[](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        return V8::to_v8(isolate, 0);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr)
    {
        ASSERT_EQ(col_ptr->size(), 1);
        ASSERT_EQ(col_ptr->getBool(0), false);
    };

    checkV8ReturnResult("bool", false, create_fn, check_fn);
}

TEST_F(UDATestCase, returnNumberForBoolArray)
{
    /// prepare v8 data
    auto create_fn =[](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, 2)).FromJust();
        result.As<v8::Array>()->Set(context, 1, V8::to_v8(isolate, 0)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, 1)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr)
    {
        ASSERT_EQ(col_ptr->size(), 3);
        ASSERT_EQ(col_ptr->getBool(0), true);
        ASSERT_EQ(col_ptr->getBool(1), false);
        ASSERT_EQ(col_ptr->getBool(2), true);
    };

    checkV8ReturnResult("bool", true, create_fn, check_fn);
}

using CREATE_DATA_FUNC = std::function<void(MutableColumnPtr &)>;
using CHECK_V8_DATA_FUNC = std::function<void(v8::Isolate *, v8::Local<v8::Context> &, v8::Local<v8::Array> &)>;

void checkPrepareArguments(String type, CREATE_DATA_FUNC create_fn, CHECK_V8_DATA_FUNC check_fn)
{
    v8::Isolate::CreateParams isolate_params;
    isolate_params.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    v8::Isolate * isolate = v8::Isolate::New(isolate_params);
    SCOPE_EXIT({ isolate->Dispose(); });
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);
    v8::TryCatch try_catch(isolate);
    /// try_catch.SetVerbose(true);
    try_catch.SetCaptureMessage(true);

    String arg_str = R"([{ "name": "value","type": ")" + type + "\"}]";

    auto config = createUDFConfig(type, arg_str, RETURN_UDA1, UDA1);

    v8::Local<v8::Context> local_ctx = v8::Context::New(isolate);
    v8::Context::Scope context_scope(local_ctx);

    auto array_type_ptr = DataTypeFactory::instance().get(type);
    auto col_ptr = array_type_ptr->createColumn();

    create_fn(col_ptr);

    MutableColumns columns;
    columns.emplace_back(std::move(col_ptr));
    auto argv = V8::prepareArguments(isolate, config->arguments, columns);

    ASSERT_EQ(argv.size(), 1);
    v8::Local<v8::Array> v8_arr = argv[0].As<v8::Array>();

    check_fn(isolate, local_ctx, v8_arr);
}

TEST_F(UDATestCase, prepareArgumentsIntArray)
{
    /// prepare input column
    auto create_int_arr = [](MutableColumnPtr & col_ptr) {
        auto & column_array = assert_cast<ColumnArray &>(*col_ptr);
        column_array.insert(Array{1, 2});
        column_array.insert(Array{3, 4, 5});
    };

    auto check_int_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr)
    {
        ASSERT_EQ(v8_arr->Length(), 2);
        v8::Local<v8::Value> elem1 = v8_arr->Get(local_ctx, 0).ToLocalChecked();
        v8::Local<v8::Value> elem2 = v8_arr->Get(local_ctx, 1).ToLocalChecked();
        ASSERT_EQ(elem1.As<v8::Array>()->Length(), 2);
        ASSERT_EQ(elem2.As<v8::Array>()->Length(), 3);
    };

    checkPrepareArguments("array(int64)", create_int_arr, check_int_arr);
}

TEST_F(UDATestCase, prepareArgumentsBoolArray)
{
    /// prepare input column
    auto create_bool_arr = [](MutableColumnPtr & col_ptr) {
        auto & column_array = assert_cast<ColumnArray &>(*col_ptr);
        column_array.insert(Array{true, false});
        column_array.insert(Array{true, true, true});
    };

    auto check_bool_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr)
    {
        ASSERT_EQ(v8_arr->Length(), 2);
        v8::Local<v8::Value> elem1 = v8_arr->Get(local_ctx, 0).ToLocalChecked();
        v8::Local<v8::Value> elem2 = v8_arr->Get(local_ctx, 1).ToLocalChecked();
        ASSERT_EQ(elem1.As<v8::Array>()->Length(), 2);
        ASSERT_EQ(elem2.As<v8::Array>()->Length(), 3);
    };

    checkPrepareArguments("array(bool)", create_bool_arr, check_bool_arr);
}

TEST_F(UDATestCase, prepareArgumentsStrArray)
{
    /// prepare input column
    auto create_str_arr = [](MutableColumnPtr & col_ptr) {
        auto & column_array = assert_cast<ColumnArray &>(*col_ptr);
        column_array.insert(Array{"aaa", "bbb"});
        column_array.insert(Array{"ccc", "ddd", "eee"});
    };

    auto check_str_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr)
    {
        ASSERT_EQ(v8_arr->Length(), 2);
        v8::Local<v8::Value> elem1 = v8_arr->Get(local_ctx, 0).ToLocalChecked();
        v8::Local<v8::Value> elem2 = v8_arr->Get(local_ctx, 1).ToLocalChecked();
        ASSERT_EQ(elem1.As<v8::Array>()->Length(), 2);
        ASSERT_EQ(elem2.As<v8::Array>()->Length(), 3);
    };

    checkPrepareArguments("array(string)", create_str_arr, check_str_arr);
}

TEST_F(UDATestCase, prepareArgumentsEmptyArray)
{
    /// prepare input column
    auto create_int_arr = [](MutableColumnPtr & col_ptr) {
        auto & column_array = assert_cast<ColumnArray &>(*col_ptr);
        column_array.insert(Array{});
        column_array.insert(Array{3, 4, 5});
    };

    auto check_int_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr)
    {
        ASSERT_EQ(v8_arr->Length(), 2);
        v8::Local<v8::Value> elem1 = v8_arr->Get(local_ctx, 0).ToLocalChecked();
        v8::Local<v8::Value> elem2 = v8_arr->Get(local_ctx, 1).ToLocalChecked();
        ASSERT_EQ(elem1.As<v8::Array>()->Length(), 0);
        ASSERT_EQ(elem2.As<v8::Array>()->Length(), 3);
    };

    checkPrepareArguments("array(int64)", create_int_arr, check_int_arr);
}

TEST_F(UDATestCase, prepareArgumentsNestedArray)
{
    /// prepare input column
    auto create_nested_arr = [](MutableColumnPtr & col_ptr) {
        auto & column_array = assert_cast<ColumnArray &>(*col_ptr);
        Array nested1 = Array{Array{1, 2}, Array{3, 4, 5}};
        column_array.insert(nested1);
        Array nested2 = Array{Array{6}, Array{7}};
        column_array.insert(nested2);
    };

    auto check_nested_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr)
    {
        ASSERT_EQ(v8_arr->Length(), 2);
        v8::Local<v8::Array> v8_nested1 = v8_arr->Get(local_ctx, 0).ToLocalChecked().As<v8::Array>();
        v8::Local<v8::Array> v8_nested2 = v8_arr->Get(local_ctx, 1).ToLocalChecked().As<v8::Array>();
        ASSERT_EQ(v8_nested1->Length(), 2);
        v8::Local<v8::Value> elem1 = v8_nested1->Get(local_ctx, 0).ToLocalChecked();
        v8::Local<v8::Value> elem2 = v8_nested1->Get(local_ctx, 1).ToLocalChecked();
        ASSERT_EQ(elem1.As<v8::Array>()->Length(), 2);
        ASSERT_EQ(elem2.As<v8::Array>()->Length(), 3);
        elem1 = v8_nested2->Get(local_ctx, 0).ToLocalChecked();
        elem2 = v8_nested2->Get(local_ctx, 1).ToLocalChecked();
        ASSERT_EQ(elem1.As<v8::Array>()->Length(), 1);
        ASSERT_EQ(elem2.As<v8::Array>()->Length(), 1);
    };

    checkPrepareArguments("array(array(int64))", create_nested_arr, check_nested_arr);
}

TEST_F(UDATestCase, prepareArgumentsBool)
{
    /// prepare input column
    auto create_bool_arr = [](MutableColumnPtr & col_ptr) {
        col_ptr->insert(true);
        col_ptr->insert(false);
        col_ptr->insert(1);
        col_ptr->insert(3);
        col_ptr->insert(0);
    };

    auto check_bool = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr)
    {
        ASSERT_EQ(v8_arr->Length(), 5);
        std::vector<bool> expect = {true, false, true, true, false};
        for (int i = 0; i < v8_arr->Length(); i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(elem->IsBoolean(), true);
            ASSERT_EQ(V8::from_v8<bool>(isolate, elem), expect[i]);
        }
    };

    checkPrepareArguments("bool", create_bool_arr, check_bool);
}

TEST_F(UDATestCase, prepareArgumentsUInt8)
{
    /// prepare input column
    auto create_uint8_arr = [](MutableColumnPtr & col_ptr) {
        col_ptr->insert(0);
        col_ptr->insert(1);
        col_ptr->insert(2);
        col_ptr->insert(3);
        col_ptr->insert(4);
    };

    auto check_uint8 = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr)
    {
        ASSERT_EQ(v8_arr->Length(), 5);
        for (uint i = 0; i < v8_arr->Length(); i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(V8::from_v8<uint8_t>(isolate, elem), i);
        }
    };

    checkPrepareArguments("uint8", create_uint8_arr, check_uint8);
}
