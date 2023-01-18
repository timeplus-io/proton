#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <V8/ConvertDataTypes.h>
#include <V8/Utils.h>
#include <base/ClockUtils.h>
#include <base/scope_guard.h>
#include <v8.h>
#include <Common/logger_useful.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>

#include <numeric>

Poco::Logger * v8_log = nullptr;
v8::Isolate * global_isolate = nullptr;

/// https://chromium.googlesource.com/v8/v8.git/+/4.5.56/test/cctest/test-api.cc

void setupLog()
{
    Poco::AutoPtr<OwnPatternFormatter> pf(new OwnPatternFormatter(true));
    Poco::AutoPtr<DB::OwnFormattingChannel> console_channel(new DB::OwnFormattingChannel(pf, new Poco::ConsoleChannel));
    Poco::Logger::root().setChannel(console_channel);
    Poco::Logger::root().setLevel("debug");

    v8_log = &Poco::Logger::get("v8_perf");
}

void messageHandler(v8::Local<v8::Message> message, v8::Local<v8::Value> data)
{
    auto msg =  message->Get();
    auto str_msg = DB::V8::from_v8<std::string>(global_isolate, msg);
    LOG_ERROR(v8_log, "message={} is_string={}, is_number={}", str_msg, data->IsString(), data->IsNumber());
}

std::string getSource()
{
    return R"###(
var uda = {
    initialize: function() {
        this.bottom_ts = new Date();
        this.last_down_price = -1.0;
        this.start_price = -1.0;
        this.down_duration = 1;
        this.result = [];
    },
    process: function(rowtimes, prices) {
        let emit = false;

        for (let i = 0; i < rowtimes.length; i++) {
            if (this.start_price < 0 || (this.last_down_price < 0 && prices[i] >= this.start_price)) {
                this.start_price = prices[i];
                this.bottom_ts = rowtimes[i];
                this.down_duration = 1;
            } else if ((this.last_down_price < 0 && prices[i] < this.start_price) || (prices[i] < this.last_down_price)) {
                this.last_down_price = prices[i];
                this.bottom_ts = rowtimes[i];
                this.down_duration = this.down_duration + 1;
            } else if (prices[i] > this.last_down_price) {
                this.result.push({
                    'start_price': this.start_price,
                    'bottom_ts': this.bottom_ts,
                    'end_price': prices[i],
                    'down_duration': this.down_duration
                });

                emit = true;
                this.bottom_ts = rowtimes[i];
                this.start_price = prices[i];
                this.last_down_price = -1.0;
                this.down_duration = 1;
            } else {
                this.down_duration = this.down_duration + 1;
            }
        }
        return emit;
    },
    finalize: function() {
        var old_result = this.result;
        this.result = [];
        return old_result;
    },
    has_customized_emit: true
})###";
}

std::string getSourceBlueprint()
{
    return R"###(
var uda = {
    initialize: function() {
        this.bottom_ts = new Date();
        this.last_down_price = -1.0;
        this.start_price = -1.0;
        this.down_duration = 1;
        this.result = [];
    },
    process: function(rowtimes, prices) {
        let emit = false;

        for (let i = 0; i < rowtimes.length; i++) {
            if (this.start_price < 0 || (this.last_down_price < 0 && prices[i] >= this.start_price)) {
                this.start_price = prices[i];
                this.bottom_ts = rowtimes[i];
                this.down_duration = 1;
            } else if ((this.last_down_price < 0 && prices[i] < this.start_price) || (prices[i] < this.last_down_price)) {
                this.last_down_price = prices[i];
                this.bottom_ts = rowtimes[i];
                this.down_duration = this.down_duration + 1;
            } else if (prices[i] > this.last_down_price) {
                this.result.push({
                    'start_price': this.start_price,
                    'bottom_ts': this.bottom_ts,
                    'end_price': prices[i],
                    'down_duration': this.down_duration
                });

                emit = true;
                this.bottom_ts = rowtimes[i];
                this.start_price = prices[i];
                this.last_down_price = -1.0;
                this.down_duration = 1;
            } else {
                this.down_duration = this.down_duration + 1;
            }
        }
        return emit;
    },
    finalize: function() {
        var old_result = this.result;
        this.result = [];
        return old_result;
    },
    has_customized_emit: true
};

function _proton_create_uda()
{
    return Object.create(uda);
}
)###";
}

std::string getSourceBlueprintSum()
{
    return R"###(
    var uda = {
        initialize: function() { this.sum = 0; },
        process: function(values) {
            for (let i = 0; i < values.length; i++)
                this.sum += values[i];
        },
        finalize: function() {
            return this.sum;
        },

    };

    function _proton_create_sum()
    {
        return Object.create(uda);
    }
)###";
}

std::string getSourceBlueprintSumMutable()
{
    return R"###(
    var uda = {
        initialize: function() { this.sum = [0]; },
        process: function(values) {
            for (let i = 0; i < values.length; i++)
                this.sum[0] += values[i];
        },
        finalize: function() {
            return this.sum[0];
        },

    };

    function _proton_create_sum()
    {
        return Object.create(uda);
    }
)###";
}



std::string getSourceClass()
{
    return R"###(
var uda = class {
    initialize() {
        this.bottom_ts = new Date();
        this.last_down_price = -1.0;
        this.start_price = -1.0;
        this.down_duration = 1;
        this.result = [];
    }

    process(rowtimes, prices) {
        let emit = false;

        for (let i = 0; i < rowtimes.length; i++) {
            if (this.start_price < 0 || (this.last_down_price < 0 && prices[i] >= this.start_price)) {
                this.start_price = prices[i];
                this.bottom_ts = rowtimes[i];
                this.down_duration = 1;
            } else if ((this.last_down_price < 0 && prices[i] < this.start_price) || (prices[i] < this.last_down_price)) {
                this.last_down_price = prices[i];
                this.bottom_ts = rowtimes[i];
                this.down_duration = this.down_duration + 1;
            } else if (prices[i] > this.last_down_price) {
                this.result.push({
                    'start_price': this.start_price,
                    'bottom_ts': this.bottom_ts,
                    'end_price': prices[i],
                    'down_duration': this.down_duration
                });

                emit = true;
                this.bottom_ts = rowtimes[i];
                this.start_price = prices[i];
                this.last_down_price = -1.0;
                this.down_duration = 1;
            } else {
                this.down_duration = this.down_duration + 1;
            }
        }
        return emit;
    }

    finalize() {
        var old_result = this.result;
        this.result = [];
        return old_result;
    }

    has_customized_emit() {
    }
})###";
}

struct JavaScriptObjectState
{
    v8::Persistent<v8::Object> object;
    v8::Persistent<v8::Context> context;
    v8::Persistent<v8::Function> initialize_func;
    v8::Persistent<v8::Function> process_func;
    v8::Persistent<v8::Function> finalize_func;
    v8::Persistent<v8::Function> merge_func;
    v8::Persistent<v8::Function> serialize_func;
    v8::Persistent<v8::Function> deserialize_func;
};

using JavaScriptObjectStatePtr = std::unique_ptr<JavaScriptObjectState>;

void run(v8::Isolate * isolate, const v8::Persistent<v8::Context> & obj_blue_print_ctx, std::vector<JavaScriptObjectStatePtr> & all_states)
{
    /// Call sum for each object
    std::vector<size_t> values(100);
    std::iota(values.begin(), values.end(), 1);
    std::vector<size_t> results;
    results.reserve(100);

    auto start = DB::MonotonicMicroseconds::now();
    for (auto & state : all_states)
    {
        const auto & ctx = state->context.IsEmpty() ? obj_blue_print_ctx : state->context;
        {
            DB::V8::run(isolate, ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch & try_catch) {
                v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate_, state->object);
                v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, state->process_func);

                std::vector<v8::Local<v8::Value>> argv(1, DB::V8::to_v8(isolate_, values.begin(), values.end()));


                v8::Local<v8::Value> res;
                if (!local_func->Call(local_ctx, local_obj, 1, argv.data()).ToLocal(&res))
                    LOG_FATAL(v8_log, "Failed to call process function");
            });
        }
    }


    for (auto & state : all_states)
    {
        const auto & ctx = state->context.IsEmpty() ? obj_blue_print_ctx : state->context;
        DB::V8::run(isolate, ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch & try_catch) {
            v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate_, state->object);
            v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, state->finalize_func);

            v8::Local<v8::Value> res;
            if (!local_func->Call(local_ctx, local_obj, 0, nullptr).ToLocal(&res) || res.IsEmpty())
                LOG_FATAL(v8_log, "Failed to call finalize function");

            results.push_back(DB::V8::from_v8<size_t>(isolate, res));
        });
    }

    auto duration = DB::MonotonicMicroseconds::now() - start;

    LOG_INFO(v8_log, "took {}us to run {} uda script, us_per_iteration={}", duration, all_states.size(), duration / all_states.size());

    /// validate the results
    auto sum = std::accumulate(values.begin(), values.end(), 0);
    for (auto s : results)
    {
        if (s != sum)
            LOG_ERROR(v8_log, "expect {}, got {}", sum, s);
    }
}

void perf_recompile(v8::Isolate * isolate)
{
    auto source = getSourceBlueprintSum();
    int64_t iterations = 2;

    std::vector<JavaScriptObjectStatePtr> all_states;
    all_states.reserve(iterations);

    auto start = DB::MonotonicMicroseconds::now();

    for (int64_t i = 0; i < iterations; ++i)
    {
        DB::V8::compileSource(isolate, "uda", source, [&] (v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch & try_catch, v8::Local<v8::Value> & obj) {
            all_states.push_back(std::make_unique<JavaScriptObjectState>());
            auto & state = *all_states.back();

            state.object.Reset(isolate, obj.As<v8::Object>());

            /// get functions defined in JavaScript UDA code
            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "initialize")).ToLocal(&function_val) && function_val->IsFunction())
                    state.initialize_func.Reset(isolate_, function_val.As<v8::Function>());
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "process")).ToLocal(&function_val))
                {
                    assert(function_val->IsFunction());
                    state.process_func.Reset(isolate_, function_val.As<v8::Function>());
                }
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "finalize")).ToLocal(&function_val))
                {
                    assert(function_val->IsFunction());
                    state.finalize_func.Reset(isolate_, function_val.As<v8::Function>());
                }
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "merge")).ToLocal(&function_val) && function_val->IsFunction())
                    state.merge_func.Reset(isolate_, function_val.As<v8::Function>());
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "serialize")).ToLocal(&function_val) && function_val->IsFunction())
                    state.serialize_func.Reset(isolate_, function_val.As<v8::Function>());
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "deserialize")).ToLocal(&function_val) && function_val->IsFunction())
                    state.deserialize_func.Reset(isolate_, function_val.As<v8::Function>());
            }

            state.context.Reset(isolate_, local_ctx);

            if (!state.initialize_func.IsEmpty())
            {
                v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, state.initialize_func);

                v8::Local<v8::Value> res;
                if (!local_func->Call(local_ctx, obj, 0, nullptr).ToLocal(&res))
                    LOG_ERROR(v8_log, "failed to initialize");
            }
        });

    }
    auto duration = DB::MonotonicMicroseconds::now() - start;

    LOG_INFO(v8_log, "took {}us to compile and run {} uda script, us_per_iteration={}", duration, iterations, duration / iterations);

    v8::Persistent<v8::Context> obj_blue_print_ctx;
    run(isolate, obj_blue_print_ctx, all_states);
}

void perf_compile_once(v8::Isolate * isolate)
{
    auto source = getSourceBlueprintSum();
    int64_t iterations = 1000;

    std::vector<JavaScriptObjectStatePtr> all_states;
    all_states.reserve(iterations);

    v8::Persistent<v8::Function> obj_blueprint;
    v8::Persistent<v8::Context> obj_blue_print_ctx;

    DB::V8::compileSource(isolate, "_proton_create_sum", source, [&] (v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch, v8::Local<v8::Value> & obj_func) {
        obj_blueprint.Reset(isolate_, obj_func.As<v8::Function>());
        obj_blue_print_ctx.Reset(isolate_, ctx);
    });

    auto start = DB::MonotonicMicroseconds::now();

    for (int64_t i = 0; i < iterations; ++i)
    {
        DB::V8::run(
            isolate, obj_blue_print_ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &) {
            v8::Local<v8::Function> local_create_func = v8::Local<v8::Function>::New(isolate_, obj_blueprint);

            v8::Local<v8::Value> obj;
            if (!local_create_func->Call(ctx, ctx->Global(), 0, nullptr).ToLocal(&obj) || !obj->IsObject())
            {
                LOG_ERROR(v8_log, "failed to create create uda");
                return;
            }

            auto & state = *all_states.emplace_back(std::make_unique<JavaScriptObjectState>());

            /// Cache the result as state object
            state.object.Reset(isolate_, obj.As<v8::Object>());

            /// get functions defined in JavaScript UDA code
            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "initialize")).ToLocal(&function_val) && function_val->IsFunction())
                    state.initialize_func.Reset(isolate_, function_val.As<v8::Function>());
//                else
//                    LOG_INFO(v8_log, "initialize function is not defined");
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "process")).ToLocal(&function_val) && function_val->IsFunction())
                {
                    assert(function_val->IsFunction());
                    state.process_func.Reset(isolate_, function_val.As<v8::Function>());
                }
                else
                    LOG_FATAL(v8_log, "process function is not defined");
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "finalize")).ToLocal(&function_val) && function_val->IsFunction())
                {
                    assert(function_val->IsFunction());
                    state.finalize_func.Reset(isolate_, function_val.As<v8::Function>());
                }
                else
                    LOG_FATAL(v8_log, "finalize function is not defined");
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "merge")).ToLocal(&function_val) && function_val->IsFunction())
                    state.merge_func.Reset(isolate_, function_val.As<v8::Function>());
//                else
//                    LOG_INFO(v8_log, "merge function is not defined");
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "serialize")).ToLocal(&function_val) && function_val->IsFunction())
                    state.serialize_func.Reset(isolate_, function_val.As<v8::Function>());
//                else
//                    LOG_INFO(v8_log, "serialize function is not defined");
            }

            {
                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "deserialize")).ToLocal(&function_val) && function_val->IsFunction())
                    state.deserialize_func.Reset(isolate_, function_val.As<v8::Function>());
//                else
//                    LOG_INFO(v8_log, "deserialize function is not defined");
            }

            /// Create persistent context
            /// state.context.Reset(isolate_, v8::Context::New(isolate_));

            if (!state.initialize_func.IsEmpty())
            {
                v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, state.initialize_func);

                v8::Local<v8::Value> res;
                if (!local_func->Call(ctx, obj, 0, nullptr).ToLocal(&res))
                    LOG_ERROR(v8_log, "failed to initialize");
            }
        });
    }

    auto duration = DB::MonotonicMicroseconds::now() - start;

    LOG_INFO(v8_log, "took {}us to compile {} uda script, us_per_iteration={}", duration, iterations, duration / iterations);

    run(isolate, obj_blue_print_ctx, all_states);
}

void perf_compile_once_and_native_clone(v8::Isolate * isolate)
{
    auto source = getSourceBlueprintSum();
    int64_t iterations = 1000;

    std::vector<JavaScriptObjectStatePtr> all_states;
    all_states.reserve(iterations);

    v8::Persistent<v8::Object> obj_blueprint;
    v8::Persistent<v8::Context> obj_blue_print_ctx;

    DB::V8::compileSource(isolate, "uda", source, [&] (v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch, v8::Local<v8::Value> & obj) {
        obj_blueprint.Reset(isolate_, obj.As<v8::Object>());
        obj_blue_print_ctx.Reset(isolate_, ctx);
    });

    auto start = DB::MonotonicMicroseconds::now();

    for (int64_t i = 0; i < iterations; ++i)
    {
        DB::V8::run(
            isolate, obj_blue_print_ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &) {
                auto local_obj = v8::Local<v8::Object>::New(isolate_, obj_blueprint);
                auto obj = local_obj->Clone();

                auto & state = *all_states.emplace_back(std::make_unique<JavaScriptObjectState>());

                /// Cache the result as state object
                state.object.Reset(isolate_, obj);

                /// get functions defined in JavaScript UDA code
                {
                    v8::Local<v8::Value> function_val;
                    if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "initialize")).ToLocal(&function_val) && function_val->IsFunction())
                        state.initialize_func.Reset(isolate_, function_val.As<v8::Function>());
                    //                else
                    //                    LOG_INFO(v8_log, "initialize function is not defined");
                }

                {
                    v8::Local<v8::Value> function_val;
                    if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "process")).ToLocal(&function_val) && function_val->IsFunction())
                    {
                        assert(function_val->IsFunction());
                        state.process_func.Reset(isolate_, function_val.As<v8::Function>());
                    }
                    else
                        LOG_FATAL(v8_log, "process function is not defined");
                }

                {
                    v8::Local<v8::Value> function_val;
                    if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "finalize")).ToLocal(&function_val) && function_val->IsFunction())
                    {
                        assert(function_val->IsFunction());
                        state.finalize_func.Reset(isolate_, function_val.As<v8::Function>());
                    }
                    else
                        LOG_FATAL(v8_log, "finalize function is not defined");
                }

                {
                    v8::Local<v8::Value> function_val;
                    if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "merge")).ToLocal(&function_val) && function_val->IsFunction())
                        state.merge_func.Reset(isolate_, function_val.As<v8::Function>());
                    //                else
                    //                    LOG_INFO(v8_log, "merge function is not defined");
                }

                {
                    v8::Local<v8::Value> function_val;
                    if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "serialize")).ToLocal(&function_val) && function_val->IsFunction())
                        state.serialize_func.Reset(isolate_, function_val.As<v8::Function>());
                    //                else
                    //                    LOG_INFO(v8_log, "serialize function is not defined");
                }

                {
                    v8::Local<v8::Value> function_val;
                    if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "deserialize")).ToLocal(&function_val) && function_val->IsFunction())
                        state.deserialize_func.Reset(isolate_, function_val.As<v8::Function>());
                    //                else
                    //                    LOG_INFO(v8_log, "deserialize function is not defined");
                }

                if (!state.initialize_func.IsEmpty())
                {
                    v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, state.initialize_func);

                    v8::Local<v8::Value> res;
                    if (!local_func->Call(ctx, obj, 0, nullptr).ToLocal(&res))
                        LOG_ERROR(v8_log, "failed to initialize");
                }
            });
    }

    auto duration = DB::MonotonicMicroseconds::now() - start;

    LOG_INFO(v8_log, "took {}us to compile {} uda script, us_per_iteration={}", duration, iterations, duration / iterations);

    run(isolate, obj_blue_print_ctx, all_states);
}

int main(int argc, char ** argv)
{
    setupLog();

    /// Collect argv
    std::string v8_options;
    v8_options.reserve(1024);

    for (int i = 1; i < argc; ++i)
    {
        v8_options += " ";
        v8_options += argv[i];
    }

    LOG_INFO(v8_log, "start options: {}", v8_options);

    v8::V8::InitializeICU();
    v8::V8::SetFlagsFromString(v8_options.c_str(), static_cast<int>(v8_options.size()));

    std::unique_ptr<v8::Platform> platform = v8::platform::NewDefaultPlatform();
    v8::V8::InitializePlatform(platform.get());
    v8::V8::Initialize();

    v8::Isolate::CreateParams isolateparams;
    isolateparams.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    auto * isolate = v8::Isolate::New(isolateparams);
    isolate->AddMessageListenerWithErrorLevel(messageHandler, v8::Isolate::kMessageAll);
    global_isolate = isolate;

    SCOPE_EXIT({ isolate->Dispose(); });

    /// perf_recompile(isolate);
    perf_compile_once(isolate);
    /// perf_compile_once_and_native_clone(isolate);

    return 0;
}
