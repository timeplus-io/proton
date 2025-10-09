#include <AggregateFunctions/AggregateFunctionJavaScriptAdapter.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/BlockUtils.h>
#include <V8/ConvertDataTypes.h>
#include <V8/Utils.h>

#include <Cluster/Common/Nulls.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionsConversion.h>
#include <IO/ReadBufferFromString.h>

#include <gtest/gtest.h>
#include <Poco/JSON/Parser.h>

#include <chrono>

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

cluster::protocol::UserDefinedFunctionDescriptorPtr
createUDFDescription(const String & name, const String & arg_str, const String & return_type, const String & source)
{
    std::vector<cluster::protocol::UserDefinedFunctionDescriptor::Argument> arguments;
    if (!arg_str.empty())
    {
        Poco::JSON::Parser parser;
        try
        {
            auto json_arguments = parser.parse(arg_str).extract<Poco::JSON::Array::Ptr>();
            for (unsigned int i = 0; i < json_arguments->size(); i++)
            {
                cluster::protocol::UserDefinedFunctionDescriptor::Argument argument;
                argument.name = json_arguments->getObject(i)->get("name").toString();
                argument.type = json_arguments->getObject(i)->get("type").toString();
                arguments.emplace_back(std::move(argument));
            }
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid UDF Desciption");
        }
    }

    auto javascript_udf_payload = std::make_shared<cluster::protocol::JavaScriptUserDefinedFunctionPayload>();
    javascript_udf_payload->source = source;
    javascript_udf_payload->is_aggregation = true;

    return std::make_shared<cluster::protocol::UserDefinedFunctionDescriptor>(
        std::move(arguments),
        name,
        return_type,
        std::move(javascript_udf_payload),
        0,
        0,
        "created_by",
        "last_modified_by",
        cluster::Nulls::NullVersion);
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
    auto udf_desc = createUDFDescription("down", ARGS_CEP1, RETURN_CEP1, UDA_CEP1);
    DataTypes types = getDataTypes(ARGS_CEP1);
    Array params;
    size_t max_heap_size = 100 * 1024 * 1024;
    auto aggr_function = AggregateFunctionJavaScriptAdapter(std::move(udf_desc), types, params, false, max_heap_size, 1000);

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

    auto result_col = aggr_function.getResultType()->createColumn();
    result_col->reserve(1);
    aggr_function.insertResultInto(data_ptr, *result_col, nullptr);
    ASSERT_EQ(result_col->getUInt(0), 2);
}

TEST_F(UDATestCase, CheckPoint)
{
    auto udf_desc = createUDFDescription("sec_large", ARGS_UDA1, RETURN_UDA1, UDA1);
    DataTypes types = getDataTypes(ARGS_UDA1);
    Array params;
    size_t max_heap_size = 100 * 1024 * 1024;
    auto aggr_function = AggregateFunctionJavaScriptAdapter(std::move(udf_desc), types, params, false, max_heap_size, 1000);

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
    auto udf_desc = createUDFDescription("sec_large", ARGS_UDA1, RETURN_UDA1, UDA1);
    DataTypes types = getDataTypes(ARGS_UDA1);
    Array params;
    size_t max_heap_size = 100 * 1024 * 1024;
    auto aggr_function = AggregateFunctionJavaScriptAdapter(std::move(udf_desc), types, params, false, max_heap_size, 1000);

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
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, 3)).FromJust();
        result.As<v8::Array>()->Set(context, 1, V8::to_v8(isolate, 4)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, 5)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
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
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> { return createV8Array(isolate, false); };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
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
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> { return createV8Array(isolate, true); };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
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
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> { return createV8Array(isolate, true); };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
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
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> { return V8::to_v8(isolate, true); };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
        ASSERT_EQ(col_ptr->size(), 1);
        ASSERT_EQ(col_ptr->getBool(0), true);
    };

    checkV8ReturnResult("bool", false, create_fn, check_fn);
}

TEST_F(UDATestCase, BoolArray)
{
    /// prepare v8 data
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, true)).FromJust();
        result.As<v8::Array>()->Set(context, 1, V8::to_v8(isolate, false)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, true)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
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
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> { return V8::to_v8(isolate, 0); };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
        ASSERT_EQ(col_ptr->size(), 1);
        ASSERT_EQ(col_ptr->getBool(0), false);
    };

    checkV8ReturnResult("bool", false, create_fn, check_fn);
}

TEST_F(UDATestCase, returnNumberForBoolArray)
{
    /// prepare v8 data
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, 2)).FromJust();
        result.As<v8::Array>()->Set(context, 1, V8::to_v8(isolate, 0)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, 1)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
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

    auto udf_desc = createUDFDescription(type, arg_str, RETURN_UDA1, UDA1);

    v8::Local<v8::Context> local_ctx = v8::Context::New(isolate);
    v8::Context::Scope context_scope(local_ctx);

    auto array_type_ptr = DataTypeFactory::instance().get(type);
    auto col_ptr = array_type_ptr->createColumn();

    create_fn(col_ptr);

    MutableColumns columns;
    columns.emplace_back(std::move(col_ptr));
    auto argv = V8::prepareArguments(isolate, udf_desc->arguments, columns);

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

    auto check_int_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
        ASSERT_EQ(v8_arr->Length(), 2);
        v8::Local<v8::Value> elem1 = v8_arr->Get(local_ctx, 0).ToLocalChecked();
        v8::Local<v8::Value> elem2 = v8_arr->Get(local_ctx, 1).ToLocalChecked();
        ASSERT_EQ(elem1.As<v8::Array>()->Length(), 2);
        ASSERT_EQ(elem2.As<v8::Array>()->Length(), 3);
    };

    checkPrepareArguments("array(int64)", create_int_arr, check_int_arr);
}


TEST_F(UDATestCase, NullableInt)
{
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, 1)).FromJust();
        result.As<v8::Array>()->Set(context, 1, v8::Null(isolate)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, 3)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
        ASSERT_EQ(col_ptr->size(), 3);
        Field result_elem1;
        col_ptr->get(0, result_elem1);
        ASSERT_EQ(result_elem1.get<int>(), 1);
        ASSERT_EQ(col_ptr->isNullAt(1), true);
        col_ptr->get(2, result_elem1);
        ASSERT_EQ(result_elem1.get<int>(), 3);
        
    };

    checkV8ReturnResult("nullable(int)", false, create_fn, check_fn);
}


TEST_F(UDATestCase, NullableUInt8)
{
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, 1)).FromJust();
        result.As<v8::Array>()->Set(context, 1, v8::Null(isolate)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, 3)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
        ASSERT_EQ(col_ptr->size(), 3);
        Field result_elem1;
        col_ptr->get(0, result_elem1);
        ASSERT_EQ(result_elem1.get<unsigned char>(), 1);
        ASSERT_EQ(col_ptr->isNullAt(1), true);
        col_ptr->get(2, result_elem1);
        ASSERT_EQ(result_elem1.get<unsigned char>(), 3);
        
    };

    checkV8ReturnResult("nullable(uint8)", false, create_fn, check_fn);
}
inline int64_t getTimestampfromDate(UInt16 value)
{
    std::chrono::system_clock::time_point date_time = 
        std::chrono::system_clock::from_time_t(0) + std::chrono::hours(24 * value);
    int64_t timestamp = std::chrono::duration_cast<std::chrono::seconds>(date_time.time_since_epoch()).count();
    return timestamp;
}
TEST_F(UDATestCase, NullableDate)
{
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        // need pay attention to the timezone and make code irrelative to a special timezone
        const auto & datetime64_tmp = V8::toDateTime64(getTimestampfromDate(1), 0, 3).value;
        result.As<v8::Array>()->Set(context, 0, V8::toV8Date(isolate, datetime64_tmp)).FromJust();
        result.As<v8::Array>()->Set(context, 1, v8::Null(isolate)).FromJust();
        const auto & datetime64_tmp1 = V8::toDateTime64(getTimestampfromDate(3), 0, 3).value;
        result.As<v8::Array>()->Set(context, 2, V8::toV8Date(isolate, datetime64_tmp1)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
        ASSERT_EQ(col_ptr->size(), 3);
        Field result_elem1;
        col_ptr->get(0, result_elem1);
        ASSERT_EQ(result_elem1.get<UInt16>(), 1);
        ASSERT_EQ(col_ptr->isNullAt(1), true);
        col_ptr->get(2, result_elem1);
        ASSERT_EQ(result_elem1.get<UInt16>(), 3);
        
    };

    checkV8ReturnResult("nullable(date)", false, create_fn, check_fn);
}

TEST_F(UDATestCase, NullableDateTime)
{
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::toV8Date(isolate, V8::toDateTime64(57600, 0, 3).value)).FromJust();
        result.As<v8::Array>()->Set(context, 1, v8::Null(isolate)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::toV8Date(isolate, V8::toDateTime64(230400, 0, 3).value)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
        ASSERT_EQ(col_ptr->size(), 3);
        Field result_elem1;
        col_ptr->get(0, result_elem1);
        ASSERT_EQ(result_elem1.get<UInt64>(), 57600);
        ASSERT_EQ(col_ptr->isNullAt(1), true);
        col_ptr->get(2, result_elem1);
        ASSERT_EQ(result_elem1.get<UInt64>(), 230400);
        
    };

    checkV8ReturnResult("nullable(datetime)", false, create_fn, check_fn);
}

TEST_F(UDATestCase, NullableDateTime64)
{
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::toV8Date(isolate, V8::toDateTime64(57600, 0, 3).value)).FromJust();
        result.As<v8::Array>()->Set(context, 1, v8::Null(isolate)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::toV8Date(isolate, V8::toDateTime64(230400, 0, 3).value)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
        ASSERT_EQ(col_ptr->size(), 3);
        Field result_elem1;
        col_ptr->get(0, result_elem1);
        ASSERT_EQ(result_elem1.get<Decimal64>().getValue(), Decimal64(57600000));
        ASSERT_EQ(col_ptr->isNullAt(1), true);
        col_ptr->get(2, result_elem1);
        ASSERT_EQ(result_elem1.get<Decimal64>().getValue(), Decimal64(230400000));
        
    };

    checkV8ReturnResult("nullable(datetime64(3))", false, create_fn, check_fn);
}

TEST_F(UDATestCase, NullableBool)
{
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, true)).FromJust();
        result.As<v8::Array>()->Set(context, 1, v8::Null(isolate)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, true)).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
        ASSERT_EQ(col_ptr->size(), 3);
        ASSERT_EQ(col_ptr->getBool(0), true);
        ASSERT_EQ(col_ptr->isNullAt(1), true);
        ASSERT_EQ(col_ptr->getBool(2), true);
    };

    checkV8ReturnResult("nullable(bool)", false, create_fn, check_fn);
}

TEST_F(UDATestCase, NullableString)
{
    auto create_fn = [](v8::Isolate * isolate) -> v8::Local<v8::Value> {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Value> result = v8::Array::New(isolate, 3);
        result.As<v8::Array>()->Set(context, 0, V8::to_v8(isolate, "abc")).FromJust();
        result.As<v8::Array>()->Set(context, 1, v8::Null(isolate)).FromJust();
        result.As<v8::Array>()->Set(context, 2, V8::to_v8(isolate, "def")).FromJust();
        return scope.Escape(result);
    };

    auto check_fn = [](MutableColumnPtr & col_ptr) {
        Field res;
        col_ptr->get(0, res);
        ASSERT_EQ(res.get<String &>(), "abc");
        ASSERT_EQ(col_ptr->isNullAt(1), true);
        col_ptr->get(2, res);
        ASSERT_EQ(res.get<String &>(), "def");
    };

    checkV8ReturnResult("nullable(string)", false, create_fn, check_fn);
}


TEST_F(UDATestCase, prepareArgumentsNullableBool)
{
    /// prepare input column
    auto create_bool_arr = [](MutableColumnPtr & col_ptr) {
        col_ptr->insert(true);
        col_ptr->insert(false);
        col_ptr->insert(true);
        col_ptr->insert(Null());
    };

    auto check_bool = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
        ASSERT_EQ(v8_arr->Length(), 4);
        std::vector<bool> expect = {true, false, true};
        for (int i = 0; i < v8_arr->Length() - 1; i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(elem->IsBoolean(), true);
            ASSERT_EQ(V8::from_v8<bool>(isolate, elem), expect[i]);
        }
        v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, 3).ToLocalChecked();
        ASSERT_EQ(elem->IsNull(), true);
    };

    checkPrepareArguments("nullable(bool)", create_bool_arr, check_bool);
}

TEST_F(UDATestCase, prepareArgumentsNullableInt)
{
    /// prepare input column
    auto create_int_arr = [](MutableColumnPtr & col_ptr) {
        col_ptr->insert(1);
        col_ptr->insert(2);
        col_ptr->insert(3);
        col_ptr->insert(Null());
        col_ptr->insert(4);
    };

    auto check_bool = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
        const auto len = v8_arr->Length();
        ASSERT_EQ(len, 5);
        std::vector<std::optional<Int32>> expect = {1, 2, 3, std::nullopt, 4};
        for (int i = 0; i < len; i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(elem->IsNull(), !expect[i].has_value());
            if(expect[i].has_value())
            {
                ASSERT_EQ(elem->IsInt32(), true);
                ASSERT_EQ(V8::from_v8<Int32>(isolate, elem), expect[i].value());
            }
        }
    };

    checkPrepareArguments("nullable(int)", create_int_arr, check_bool);
}

TEST_F(UDATestCase, prepareArgumentsNullableUInt8)
{
    /// prepare input column
    auto create_int_arr = [](MutableColumnPtr & col_ptr) {
        col_ptr->insert(1);
        col_ptr->insert(2);
        col_ptr->insert(3);
        col_ptr->insert(Null());
        col_ptr->insert(256);
    };

    auto check_bool = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
        const auto len = v8_arr->Length();
        ASSERT_EQ(len, 5);
        std::vector<std::optional<UInt8>> expect = {1, 2, 3, std::nullopt, 0};
        for (int i = 0; i < len; i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(elem->IsNull(), !expect[i].has_value());
            if(expect[i].has_value())
            {
                ASSERT_EQ(elem->IsUint32(), true);
                ASSERT_EQ(V8::from_v8<Int32>(isolate, elem), expect[i].value());
            }
        }
    };

    checkPrepareArguments("nullable(uint8)", create_int_arr, check_bool);
}
TEST_F(UDATestCase, prepareArgumentsNullableDate)
{
    /// prepare input column
    auto create_date_arr = [](MutableColumnPtr & col_ptr) {
        col_ptr->insert(1);
        col_ptr->insert(2);
        col_ptr->insert(3);
        col_ptr->insert(Null());
        col_ptr->insert(112);
    };

    auto check_bool = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
        const auto len = v8_arr->Length();
        ASSERT_EQ(len, 5);
        std::vector<std::optional<int64_t>> expect_days = {1, 2, 3, std::nullopt, 112};
        std::vector<std::optional<int64_t>> expect;
        for (int i = 0; i < len; i++)
        {
            if(expect_days[i].has_value())
            {
                expect.emplace_back(V8::toDateTime64<UInt16>(expect_days[i].value()));
            }
            else
            {
                expect.emplace_back(std::nullopt);
            }
        }
        for (int i = 0; i < len; i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(elem->IsNull(), !expect[i].has_value());
            if(expect[i].has_value())
            {
                ASSERT_EQ(elem->IsDate(), true);
                ASSERT_EQ(static_cast<int64_t>(elem.As<v8::Date>()->NumberValue(local_ctx).ToChecked()), expect[i].value());
            }
        }
    };

    checkPrepareArguments("nullable(date)", create_date_arr, check_bool);
}

TEST_F(UDATestCase, prepareArgumentsNullableDateTime)
{
    /// prepare input column
    auto create_date_arr = [](MutableColumnPtr & col_ptr) {
        col_ptr->insert(1);
        col_ptr->insert(2);
        col_ptr->insert(3);
        col_ptr->insert(Null());
        col_ptr->insert(112);
    };

    auto check_bool = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
        const auto len = v8_arr->Length();
        ASSERT_EQ(len, 5);
        std::vector<std::optional<int64_t>> expect = {1, 2, 3, std::nullopt, 112};
        for (int i = 0; i < len; i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(elem->IsNull(), !expect[i].has_value());
            if(expect[i].has_value())
            {
                ASSERT_EQ(elem->IsDate(), true);
                
                ASSERT_EQ(V8::toDateTime64(static_cast<int64_t>(elem.As<v8::Date>()->NumberValue(local_ctx).ToChecked()), 3, 0), expect[i].value());
            }
        }
    };

    checkPrepareArguments("nullable(datetime)", create_date_arr, check_bool);
}

TEST_F(UDATestCase, prepareArgumentsNullableDateTime64)
{
    /// prepare input column
    auto create_date_arr = [](MutableColumnPtr & col_ptr) {
        Decimal64 v = 1u;
        col_ptr->insert(v);
        v = 2u;
        col_ptr->insert(v);
        v = 3u;
        col_ptr->insert(v);
        col_ptr->insert(Null());
        v = 112u;
        col_ptr->insert(v);
    };

    auto check_bool = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
        const auto len = v8_arr->Length();
        ASSERT_EQ(len, 5);
        std::vector<std::optional<int64_t>> expect = {1, 2, 3, std::nullopt, 112};
        for (int i = 0; i < len; i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(elem->IsNull(), !expect[i].has_value());
            if(expect[i].has_value())
            {
                ASSERT_EQ(elem->IsDate(), true);
                
                ASSERT_EQ(V8::toDateTime64(static_cast<int64_t>(elem.As<v8::Date>()->NumberValue(local_ctx).ToChecked()), 3, 3), expect[i].value());
            }
        }
    };

    checkPrepareArguments("nullable(datetime64(3))", create_date_arr, check_bool);
}

TEST_F(UDATestCase, prepareArgumentsNullableString)
{
    /// prepare input column
    auto create_string_arr = [](MutableColumnPtr & col_ptr) {
        col_ptr->insert("abc");
        col_ptr->insert("def");
        col_ptr->insert("ghl");
        col_ptr->insert(Null());
    };

    auto check_bool = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
        ASSERT_EQ(v8_arr->Length(), 4);
        std::vector<String> expect = {"abc", "def", "ghl"};
        for (int i = 0; i < v8_arr->Length() - 1; i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(elem->IsString(), true);
            ASSERT_EQ(V8::from_v8<String>(isolate, elem), expect[i]);
        }
        v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, 3).ToLocalChecked();
        ASSERT_EQ(elem->IsNull(), true);
    };

    checkPrepareArguments("nullable(string)", create_string_arr, check_bool);
}

TEST_F(UDATestCase, prepareArgumentsBoolArray)
{
    /// prepare input column
    auto create_bool_arr = [](MutableColumnPtr & col_ptr) {
        auto & column_array = assert_cast<ColumnArray &>(*col_ptr);
        column_array.insert(Array{true, false});
        column_array.insert(Array{true, true, true});
    };

    auto check_bool_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
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

    auto check_str_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
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

    auto check_int_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
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

    auto check_nested_arr = [](v8::Isolate *, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
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

    auto check_bool = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
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

    auto check_uint8 = [](v8::Isolate * isolate, v8::Local<v8::Context> & local_ctx, v8::Local<v8::Array> & v8_arr) {
        ASSERT_EQ(v8_arr->Length(), 5);
        for (uint i = 0; i < v8_arr->Length(); i++)
        {
            v8::Local<v8::Value> elem = v8_arr->Get(local_ctx, i).ToLocalChecked();
            ASSERT_EQ(V8::from_v8<uint8_t>(isolate, elem), i);
        }
    };

    checkPrepareArguments("uint8", create_uint8_arr, check_uint8);
}

TEST_F(UDATestCase, consoleModule)
{
    v8::Isolate::CreateParams isolate_params;
    isolate_params.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    v8::Isolate * isolate = v8::Isolate::New(isolate_params);
    SCOPE_EXIT({ isolate->Dispose(); });
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);

    auto check
        = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch & try_catch, v8::Local<v8::Value> & blueprint) {
              v8::Local<v8::Value> console_val;
              ASSERT_EQ(local_ctx->Global()->Get(local_ctx, V8::to_v8(isolate, "console")).ToLocal(&console_val), true);
              ASSERT_EQ(console_val->IsObject(), true);

              auto obj = console_val.As<v8::Object>();
              v8::Local<v8::Value> function_val;
              ASSERT_EQ(obj->Get(local_ctx, V8::to_v8(isolate_, "log")).ToLocal(&function_val), true);
              ASSERT_EQ(function_val->IsFunction(), true);
          };

    V8::compileSource(isolate, "empty_fun", "var a=1;", check);
}
