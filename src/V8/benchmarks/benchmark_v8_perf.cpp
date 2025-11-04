#include <V8/ConvertDataTypes.h>
#include <V8/Utils.h>
#include <V8/V8Includes.h>

#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <base/ClockUtils.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>

#include <benchmark/benchmark.h>

#include <cstdlib>
#include <cstring>
#include <numeric>

bool LOG_PANIC_ABORT = true;
LoggerPtr v8_log = nullptr;
v8::Isolate * global_isolate = nullptr;
static std::once_flag g_v8_once_flag;
static std::unique_ptr<v8::Platform> g_v8_platform;

void setupLog()
{
    Poco::AutoPtr<OwnPatternFormatter> pf(new OwnPatternFormatter(true));
    Poco::AutoPtr<DB::OwnFormattingChannel> console_channel(new DB::OwnFormattingChannel(pf, new Poco::ConsoleChannel));
    Poco::Logger::root().setChannel(console_channel);
    Poco::Logger::root().setLevel("debug");

    v8_log = getLogger("v8_bench");
}

void messageHandler(v8::Local<v8::Message> message, v8::Local<v8::Value> data)
{
    auto msg = message->Get();
    auto str_msg = DB::V8::from_v8<std::string>(global_isolate, msg);
    LOG_ERROR(v8_log, "message={} is_string={}, is_number={}", str_msg, data->IsString(), data->IsNumber());
}

static inline std::string getSourceBlueprintSum()
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

static inline std::string getSourceBlueprintSumMutable()
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

static inline void release_states(v8::Isolate * isolate, std::vector<JavaScriptObjectStatePtr> & all_states)
{
    for (auto & s : all_states)
    {
        s->object.Reset();
        s->context.Reset();
        s->initialize_func.Reset();
        s->process_func.Reset();
        s->finalize_func.Reset();
        s->merge_func.Reset();
        s->serialize_func.Reset();
        s->deserialize_func.Reset();
    }
    all_states.clear();
    all_states.shrink_to_fit();
    if (isolate)
        isolate->LowMemoryNotification();
}

static void
run(v8::Isolate * isolate, const v8::Persistent<v8::Context> & obj_blue_print_ctx, std::vector<JavaScriptObjectStatePtr> & all_states, size_t values_size, int process_repeats)
{
    std::vector<size_t> values(values_size);
    std::iota(values.begin(), values.end(), 1);
    std::vector<size_t> results;
    results.reserve(all_states.size());

    for (auto & state : all_states)
    {
        const auto & ctx = state->context.IsEmpty() ? obj_blue_print_ctx : state->context;
        DB::V8::run(isolate, ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch &) {
            v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate_, state->object);
            v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, state->process_func);

            for (int r = 0; r < process_repeats; ++r)
            {
                std::vector<v8::Local<v8::Value>> argv(1, DB::V8::to_v8(isolate_, values.begin(), values.end()));
                v8::Local<v8::Value> res;
                if (!local_func->Call(local_ctx, local_obj, 1, argv.data()).ToLocal(&res))
                    LOG_FATAL(v8_log, "Failed to call process function");
            }
        });
    }

    for (auto & state : all_states)
    {
        const auto & ctx = state->context.IsEmpty() ? obj_blue_print_ctx : state->context;
        DB::V8::run(isolate, ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch &) {
            v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate_, state->object);
            v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, state->finalize_func);

            v8::Local<v8::Value> res;
            if (!local_func->Call(local_ctx, local_obj, 0, nullptr).ToLocal(&res) || res.IsEmpty())
                LOG_FATAL(v8_log, "Failed to call finalize function");

            results.push_back(DB::V8::from_v8<size_t>(isolate, res));
        });
    }

    size_t sum = std::accumulate(values.begin(), values.end(), 0);
    size_t expected = sum * static_cast<size_t>(process_repeats);
    for (auto s : results)
        if (s != expected)
            LOG_ERROR(v8_log, "expect {}, got {}", expected, s);
}

class V8PerfFixture : public benchmark::Fixture
{
public:
    void SetUp(const benchmark::State &) override
    {
        setupLog();

        std::call_once(g_v8_once_flag, [] {
            v8::V8::InitializeICU();
            const char * flags = std::getenv("V8_FLAGS");
            if (flags && *flags)
                v8::V8::SetFlagsFromString(flags, static_cast<int>(strlen(flags)));

            g_v8_platform = v8::platform::NewDefaultPlatform();
            v8::V8::InitializePlatform(g_v8_platform.get());
            v8::V8::Initialize();
        });

        v8::Isolate::CreateParams isolateparams;
        isolateparams.array_buffer_allocator_shared
            = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
        isolate = v8::Isolate::New(isolateparams);
        {
            v8::Locker locker(isolate);
            v8::Isolate::Scope isolate_scope(isolate);
            v8::HandleScope handle_scope(isolate);
            isolate->AddMessageListenerWithErrorLevel(messageHandler, v8::Isolate::kMessageAll);
        }
        global_isolate = isolate;
    }

    void TearDown(const benchmark::State &) override
    {
        if (isolate)
        {
            isolate->Dispose();
            isolate = nullptr;
            global_isolate = nullptr;
        }
    }

    v8::Isolate * isolate{nullptr};
};

BENCHMARK_DEFINE_F(V8PerfFixture, BM_V8_Recompile)(benchmark::State & state)
{
    const int64_t iterations = state.range(0);
    const size_t values_size = static_cast<size_t>(state.range(1));
    constexpr int kProcessRepeats = 3;
    auto source = getSourceBlueprintSum();

    for (auto _ : state)
    {
        state.PauseTiming();
        std::vector<JavaScriptObjectStatePtr> all_states;
        all_states.reserve(iterations);
        state.ResumeTiming();

        for (int64_t i = 0; i < iterations; ++i)
        {
            DB::V8::compileSource(
                isolate,
                "uda",
                source,
                [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch &, v8::Local<v8::Value> & obj) {
                    all_states.push_back(std::make_unique<JavaScriptObjectState>());
                    auto & st = *all_states.back();

                    st.object.Reset(isolate_, obj.As<v8::Object>());

                    v8::Local<v8::Value> function_val;
                    if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "initialize")).ToLocal(&function_val)
                        && function_val->IsFunction())
                        st.initialize_func.Reset(isolate_, function_val.As<v8::Function>());

                    if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "process")).ToLocal(&function_val)
                        && function_val->IsFunction())
                        st.process_func.Reset(isolate_, function_val.As<v8::Function>());

                    if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "finalize")).ToLocal(&function_val)
                        && function_val->IsFunction())
                        st.finalize_func.Reset(isolate_, function_val.As<v8::Function>());

                    if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "merge")).ToLocal(&function_val)
                        && function_val->IsFunction())
                        st.merge_func.Reset(isolate_, function_val.As<v8::Function>());

                    if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "serialize")).ToLocal(&function_val)
                        && function_val->IsFunction())
                        st.serialize_func.Reset(isolate_, function_val.As<v8::Function>());

                    if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "deserialize")).ToLocal(&function_val)
                        && function_val->IsFunction())
                        st.deserialize_func.Reset(isolate_, function_val.As<v8::Function>());

                    st.context.Reset(isolate_, local_ctx);

                    if (!st.initialize_func.IsEmpty())
                    {
                        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, st.initialize_func);
                        v8::Local<v8::Value> res;
                        if (!local_func->Call(local_ctx, obj, 0, nullptr).ToLocal(&res))
                            LOG_ERROR(v8_log, "failed to initialize");
                    }
                });
        }

        run(isolate, v8::Persistent<v8::Context>{}, all_states, values_size, kProcessRepeats);
        state.counters["UDAObjects"] = static_cast<double>(iterations);
        state.counters["ValuesSize"] = static_cast<double>(values_size);
        state.counters["ProcessRepeats"] = static_cast<double>(kProcessRepeats);

        release_states(isolate, all_states);
    }
}

BENCHMARK_DEFINE_F(V8PerfFixture, BM_V8_CompileOnce_Create)(benchmark::State & state)
{
    const int64_t iterations = state.range(0);
    const size_t values_size = static_cast<size_t>(state.range(1));
    constexpr int kProcessRepeats = 3;
    auto source = getSourceBlueprintSum();

    for (auto _ : state)
    {
        state.PauseTiming();
        v8::Persistent<v8::Function> obj_blueprint;
        v8::Persistent<v8::Context> obj_blue_print_ctx;
        DB::V8::compileSource(
            isolate,
            "_proton_create_sum",
            source,
            [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &, v8::Local<v8::Value> & obj_func) {
                obj_blueprint.Reset(isolate_, obj_func.As<v8::Function>());
                obj_blue_print_ctx.Reset(isolate_, ctx);
            });

        std::vector<JavaScriptObjectStatePtr> all_states;
        all_states.reserve(iterations);
        state.ResumeTiming();

        for (int64_t i = 0; i < iterations; ++i)
        {
            DB::V8::run(isolate, obj_blue_print_ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &) {
                v8::Local<v8::Function> local_create_func = v8::Local<v8::Function>::New(isolate_, obj_blueprint);

                v8::Local<v8::Value> obj;
                if (!local_create_func->Call(ctx, ctx->Global(), 0, nullptr).ToLocal(&obj) || !obj->IsObject())
                {
                    LOG_ERROR(v8_log, "failed to create uda");
                    return;
                }

                auto & st = *all_states.emplace_back(std::make_unique<JavaScriptObjectState>());
                st.object.Reset(isolate_, obj.As<v8::Object>());

                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "initialize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.initialize_func.Reset(isolate_, function_val.As<v8::Function>());

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "process")).ToLocal(&function_val) && function_val->IsFunction())
                    st.process_func.Reset(isolate_, function_val.As<v8::Function>());
                else
                    LOG_FATAL(v8_log, "process function is not defined");

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "finalize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.finalize_func.Reset(isolate_, function_val.As<v8::Function>());
                else
                    LOG_FATAL(v8_log, "finalize function is not defined");

                if (!st.initialize_func.IsEmpty())
                {
                    v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, st.initialize_func);
                    v8::Local<v8::Value> res;
                    if (!local_func->Call(ctx, obj, 0, nullptr).ToLocal(&res))
                        LOG_ERROR(v8_log, "failed to initialize");
                }
            });
        }

        run(isolate, obj_blue_print_ctx, all_states, values_size, kProcessRepeats);
        state.counters["UDAObjects"] = static_cast<double>(iterations);
        state.counters["ValuesSize"] = static_cast<double>(values_size);
        state.counters["ProcessRepeats"] = static_cast<double>(kProcessRepeats);

        release_states(isolate, all_states);
        obj_blueprint.Reset();
        obj_blue_print_ctx.Reset();
        isolate->LowMemoryNotification();
    }
}

BENCHMARK_DEFINE_F(V8PerfFixture, BM_V8_CompileOnce_Clone)(benchmark::State & state)
{
    const int64_t iterations = state.range(0);
    const size_t values_size = static_cast<size_t>(state.range(1));
    constexpr int kProcessRepeats = 3;
    auto source = getSourceBlueprintSum();

    for (auto _ : state)
    {
        state.PauseTiming();
        std::vector<JavaScriptObjectStatePtr> all_states;
        all_states.reserve(iterations);

        v8::Persistent<v8::Object> obj_blueprint;
        v8::Persistent<v8::Context> obj_blue_print_ctx;
        DB::V8::compileSource(
            isolate, "uda", source, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &, v8::Local<v8::Value> & obj) {
                obj_blueprint.Reset(isolate_, obj.As<v8::Object>());
                obj_blue_print_ctx.Reset(isolate_, ctx);
            });
        state.ResumeTiming();

        for (int64_t i = 0; i < iterations; ++i)
        {
            DB::V8::run(isolate, obj_blue_print_ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &) {
                auto local_obj = v8::Local<v8::Object>::New(isolate_, obj_blueprint);
                auto obj = local_obj->Clone();

                auto & st = *all_states.emplace_back(std::make_unique<JavaScriptObjectState>());
                st.object.Reset(isolate_, obj);

                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "initialize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.initialize_func.Reset(isolate_, function_val.As<v8::Function>());

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "process")).ToLocal(&function_val) && function_val->IsFunction())
                    st.process_func.Reset(isolate_, function_val.As<v8::Function>());
                else
                    LOG_FATAL(v8_log, "process function is not defined");

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "finalize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.finalize_func.Reset(isolate_, function_val.As<v8::Function>());
                else
                    LOG_FATAL(v8_log, "finalize function is not defined");

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "merge")).ToLocal(&function_val) && function_val->IsFunction())
                    st.merge_func.Reset(isolate_, function_val.As<v8::Function>());

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "serialize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.serialize_func.Reset(isolate_, function_val.As<v8::Function>());

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "deserialize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.deserialize_func.Reset(isolate_, function_val.As<v8::Function>());

                if (!st.initialize_func.IsEmpty())
                {
                    v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, st.initialize_func);
                    v8::Local<v8::Value> res;
                    if (!local_func->Call(ctx, obj, 0, nullptr).ToLocal(&res))
                        LOG_ERROR(v8_log, "failed to initialize");
                }
            });
        }

        run(isolate, obj_blue_print_ctx, all_states, values_size, kProcessRepeats);
        state.counters["UDAObjects"] = static_cast<double>(iterations);
        state.counters["ValuesSize"] = static_cast<double>(values_size);
        state.counters["ProcessRepeats"] = static_cast<double>(kProcessRepeats);

        release_states(isolate, all_states);
        obj_blueprint.Reset();
        obj_blue_print_ctx.Reset();
        isolate->LowMemoryNotification();
    }
}

/// Scalar-sum variant
BENCHMARK_REGISTER_F(V8PerfFixture, BM_V8_Recompile)
    ->RangeMultiplier(4)
    ->Ranges({{8, 2048}, {64, 2048}})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(V8PerfFixture, BM_V8_CompileOnce_Create)
    ->RangeMultiplier(4)
    ->Ranges({{8, 2048}, {64, 2048}})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(V8PerfFixture, BM_V8_CompileOnce_Clone)
    ->RangeMultiplier(4)
    ->Ranges({{8, 2048}, {64, 2048}})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

/// Mutable-array-sum variant
BENCHMARK_DEFINE_F(V8PerfFixture, BM_V8_Recompile_Mutable)(benchmark::State & state)
{
    const int64_t iterations = state.range(0);
    const size_t values_size = static_cast<size_t>(state.range(1));
    constexpr int kProcessRepeats = 3;
    auto source = getSourceBlueprintSumMutable();

    for (auto _ : state)
    {
        state.PauseTiming();
        std::vector<JavaScriptObjectStatePtr> all_states;
        all_states.reserve(iterations);
        state.ResumeTiming();

        for (int64_t i = 0; i < iterations; ++i)
        {
            DB::V8::compileSource(
                isolate,
                "uda",
                source,
                [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch &, v8::Local<v8::Value> & obj) {
                    all_states.push_back(std::make_unique<JavaScriptObjectState>());
                    auto & st = *all_states.back();

                    st.object.Reset(isolate_, obj.As<v8::Object>());

                    v8::Local<v8::Value> function_val;
                    if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "initialize")).ToLocal(&function_val)
                        && function_val->IsFunction())
                        st.initialize_func.Reset(isolate_, function_val.As<v8::Function>());

                    if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "process")).ToLocal(&function_val)
                        && function_val->IsFunction())
                        st.process_func.Reset(isolate_, function_val.As<v8::Function>());

                    if (obj.As<v8::Object>()->Get(local_ctx, DB::V8::to_v8(isolate_, "finalize")).ToLocal(&function_val)
                        && function_val->IsFunction())
                        st.finalize_func.Reset(isolate_, function_val.As<v8::Function>());

                    st.context.Reset(isolate_, local_ctx);

                    if (!st.initialize_func.IsEmpty())
                    {
                        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, st.initialize_func);
                        v8::Local<v8::Value> res;
                        if (!local_func->Call(local_ctx, obj, 0, nullptr).ToLocal(&res))
                            LOG_ERROR(v8_log, "failed to initialize");
                    }
                });
        }

        run(isolate, v8::Persistent<v8::Context>{}, all_states, values_size, kProcessRepeats);
        state.counters["UDAObjects"] = static_cast<double>(iterations);
        state.counters["ValuesSize"] = static_cast<double>(values_size);
        state.counters["ProcessRepeats"] = static_cast<double>(kProcessRepeats);

        release_states(isolate, all_states);
    }
}

BENCHMARK_DEFINE_F(V8PerfFixture, BM_V8_CompileOnce_Create_Mutable)(benchmark::State & state)
{
    const int64_t iterations = state.range(0);
    const size_t values_size = static_cast<size_t>(state.range(1));
    constexpr int kProcessRepeats = 3;
    auto source = getSourceBlueprintSumMutable();

    for (auto _ : state)
    {
        state.PauseTiming();
        v8::Persistent<v8::Function> obj_blueprint;
        v8::Persistent<v8::Context> obj_blue_print_ctx;
        DB::V8::compileSource(
            isolate,
            "_proton_create_sum",
            source,
            [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &, v8::Local<v8::Value> & obj_func) {
                obj_blueprint.Reset(isolate_, obj_func.As<v8::Function>());
                obj_blue_print_ctx.Reset(isolate_, ctx);
            });

        std::vector<JavaScriptObjectStatePtr> all_states;
        all_states.reserve(iterations);
        state.ResumeTiming();

        for (int64_t i = 0; i < iterations; ++i)
        {
            DB::V8::run(isolate, obj_blue_print_ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &) {
                v8::Local<v8::Function> local_create_func = v8::Local<v8::Function>::New(isolate_, obj_blueprint);

                v8::Local<v8::Value> obj;
                if (!local_create_func->Call(ctx, ctx->Global(), 0, nullptr).ToLocal(&obj) || !obj->IsObject())
                {
                    LOG_ERROR(v8_log, "failed to create uda");
                    return;
                }

                auto & st = *all_states.emplace_back(std::make_unique<JavaScriptObjectState>());
                st.object.Reset(isolate_, obj.As<v8::Object>());

                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "initialize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.initialize_func.Reset(isolate_, function_val.As<v8::Function>());

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "process")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.process_func.Reset(isolate_, function_val.As<v8::Function>());
                else
                    LOG_FATAL(v8_log, "process function is not defined");

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "finalize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.finalize_func.Reset(isolate_, function_val.As<v8::Function>());
                else
                    LOG_FATAL(v8_log, "finalize function is not defined");

                if (!st.initialize_func.IsEmpty())
                {
                    v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, st.initialize_func);
                    v8::Local<v8::Value> res;
                    if (!local_func->Call(ctx, obj, 0, nullptr).ToLocal(&res))
                        LOG_ERROR(v8_log, "failed to initialize");
                }
            });
        }

        run(isolate, obj_blue_print_ctx, all_states, values_size, kProcessRepeats);
        state.counters["UDAObjects"] = static_cast<double>(iterations);
        state.counters["ValuesSize"] = static_cast<double>(values_size);
        state.counters["ProcessRepeats"] = static_cast<double>(kProcessRepeats);

        release_states(isolate, all_states);
        obj_blueprint.Reset();
        obj_blue_print_ctx.Reset();
        isolate->LowMemoryNotification();
    }
}

BENCHMARK_DEFINE_F(V8PerfFixture, BM_V8_CompileOnce_Clone_Mutable)(benchmark::State & state)
{
    const int64_t iterations = state.range(0);
    const size_t values_size = static_cast<size_t>(state.range(1));
    constexpr int kProcessRepeats = 3;
    auto source = getSourceBlueprintSumMutable();

    for (auto _ : state)
    {
        state.PauseTiming();
        std::vector<JavaScriptObjectStatePtr> all_states;
        all_states.reserve(iterations);

        v8::Persistent<v8::Object> obj_blueprint;
        v8::Persistent<v8::Context> obj_blue_print_ctx;
        DB::V8::compileSource(
            isolate,
            "uda",
            source,
            [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &, v8::Local<v8::Value> & obj) {
                obj_blueprint.Reset(isolate_, obj.As<v8::Object>());
                obj_blue_print_ctx.Reset(isolate_, ctx);
            });
        state.ResumeTiming();

        for (int64_t i = 0; i < iterations; ++i)
        {
            DB::V8::run(isolate, obj_blue_print_ctx, [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch &) {
                auto local_obj = v8::Local<v8::Object>::New(isolate_, obj_blueprint);
                auto obj = local_obj->Clone();

                auto & st = *all_states.emplace_back(std::make_unique<JavaScriptObjectState>());
                st.object.Reset(isolate_, obj);

                v8::Local<v8::Value> function_val;
                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "initialize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.initialize_func.Reset(isolate_, function_val.As<v8::Function>());

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "process")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.process_func.Reset(isolate_, function_val.As<v8::Function>());
                else
                    LOG_FATAL(v8_log, "process function is not defined");

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "finalize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.finalize_func.Reset(isolate_, function_val.As<v8::Function>());
                else
                    LOG_FATAL(v8_log, "finalize function is not defined");

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "merge")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.merge_func.Reset(isolate_, function_val.As<v8::Function>());

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "serialize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.serialize_func.Reset(isolate_, function_val.As<v8::Function>());

                if (obj.As<v8::Object>()->Get(ctx, DB::V8::to_v8(isolate_, "deserialize")).ToLocal(&function_val)
                    && function_val->IsFunction())
                    st.deserialize_func.Reset(isolate_, function_val.As<v8::Function>());

                if (!st.initialize_func.IsEmpty())
                {
                    v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, st.initialize_func);
                    v8::Local<v8::Value> res;
                    if (!local_func->Call(ctx, obj, 0, nullptr).ToLocal(&res))
                        LOG_ERROR(v8_log, "failed to initialize");
                }
            });
        }

        run(isolate, obj_blue_print_ctx, all_states, values_size, kProcessRepeats);
        state.counters["UDAObjects"] = static_cast<double>(iterations);
        state.counters["ValuesSize"] = static_cast<double>(values_size);
        state.counters["ProcessRepeats"] = static_cast<double>(kProcessRepeats);

        release_states(isolate, all_states);
        obj_blueprint.Reset();
        obj_blue_print_ctx.Reset();
        isolate->LowMemoryNotification();
    }
}

BENCHMARK_REGISTER_F(V8PerfFixture, BM_V8_Recompile_Mutable)
    ->RangeMultiplier(4)
    ->Ranges({{8, 2048}, {64, 2048}})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(V8PerfFixture, BM_V8_CompileOnce_Create_Mutable)
    ->RangeMultiplier(4)
    ->Ranges({{8, 2048}, {64, 2048}})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(V8PerfFixture, BM_V8_CompileOnce_Clone_Mutable)
    ->RangeMultiplier(4)
    ->Ranges({{8, 2048}, {64, 2048}})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_MAIN();
