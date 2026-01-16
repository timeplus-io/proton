#include "config.h"

#if USE_V8 && defined(OS_LINUX) && (defined(__x86_64__) || defined(__i386__))

#include <V8/PkuSupport.h>
#include <V8/V8Includes.h>

#include <libplatform/libplatform.h>

#include <sys/mman.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace
{
uint32_t readPkru()
{
    uint32_t eax = 0;
    uint32_t edx = 0;
    asm volatile("rdpkru" : "=a"(eax), "=d"(edx) : "c"(0)); // NOLINT
    return eax;
}

void disableAccessForPkey(int key)
{
    if (pkey_set(key, PKEY_DISABLE_ACCESS) != 0)
        std::cerr << "pkey_set(key=" << key << ", DISABLE_ACCESS) failed: " << std::strerror(errno) << "\n";
}

void initializeV8(std::unique_ptr<v8::Platform> & platform)
{
    v8::V8::InitializeICU();
    platform = v8::platform::NewDefaultPlatform();
    v8::V8::InitializePlatform(platform.get());
    v8::V8::Initialize();
}

void disposeV8(std::unique_ptr<v8::Platform> & platform)
{
    v8::V8::Dispose();
    v8::V8::DisposePlatform();
    platform.reset();
}

v8::Global<v8::Function> compileFunction(v8::Isolate * isolate, const v8::Local<v8::Context> & ctx)
{
    v8::Context::Scope context_scope(ctx);
    v8::TryCatch try_catch(isolate);

    constexpr const char * source_code = R"(
function hot(x) {
  return x + 1;
}
hot
)";

    v8::Local<v8::String> source = v8::String::NewFromUtf8(isolate, source_code).ToLocalChecked();
    v8::Local<v8::Script> script;
    if (!v8::Script::Compile(ctx, source).ToLocal(&script))
        throw std::runtime_error("failed to compile script");

    v8::Local<v8::Value> result;
    if (!script->Run(ctx).ToLocal(&result) || !result->IsFunction())
        throw std::runtime_error("script did not return a function");

    v8::Global<v8::Function> func;
    func.Reset(isolate, result.As<v8::Function>());
    return func;
}

void callHot(v8::Isolate * isolate, const v8::Local<v8::Context> & ctx, const v8::Local<v8::Function> & func, int iterations)
{
    v8::Context::Scope context_scope(ctx);
    v8::TryCatch try_catch(isolate);

    v8::Local<v8::Value> argv[] = {v8::Integer::New(isolate, 41)};
    for (int i = 0; i < iterations; ++i)
    {
        v8::Local<v8::Value> result;
        if (!func->Call(ctx, ctx->Global(), 1, argv).ToLocal(&result))
            throw std::runtime_error("call failed");

        if (i == 0 && !result->IsInt32())
            throw std::runtime_error("unexpected return type");
    }
}

struct Options
{
    int pkey = 1;
    int iterations = 1000;
    bool disable_access = false;
    bool use_pkru_guard = false;
};

bool tryParseIntArg(const std::string & arg, const std::string & prefix, int & out)
{
    if (!arg.starts_with(prefix))
        return false;

    const std::string tail = arg.substr(prefix.size());
    try
    {
        out = std::stoi(tail);
        return true;
    }
    catch (const std::exception &)
    {
        std::cerr << "Invalid value for " << prefix << ": " << tail << "\n";
        std::exit(2);
    }
}

Options parseArgs(int argc, char ** argv)
{
    Options opts;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg(argv[i]);
        if (tryParseIntArg(arg, "--pkey=", opts.pkey))
            continue;
        if (tryParseIntArg(arg, "--iterations=", opts.iterations))
            continue;

        if (arg == "--disable-access")
            opts.disable_access = true;
        else if (arg == "--disable-pkeys")
            opts.disable_access = true;
        else if (arg == "--guard")
            opts.use_pkru_guard = true;
        else if (arg == "--crash")
        {
            opts.disable_access = true;
            opts.use_pkru_guard = false;
        }
        else if (arg == "--help")
        {
            std::cout << "Usage:\n"
                      << "  v8_pkru_repro [--disable-access] [--guard]\n"
                      << "  v8_pkru_repro --crash\n\n"
                      << "Options:\n"
                      << "  --pkey=N         Target protection key number (default: 1)\n"
                      << "  --iterations=N   JS call iterations on the worker thread (default: 1000)\n"
                      << "  --disable-access Call pkey_set(pkey, DISABLE_ACCESS) on the worker thread\n"
                      << "  --guard          Use DB::V8::PkuSupport::ThreadPkruGuard on the worker thread before entering V8\n"
                      << "  --crash          Convenience alias for: --disable-access (and without --guard)\n";
            std::exit(0);
        }
        else
        {
            std::cerr << "Unknown arg: " << arg << " (use --help)\n";
            std::exit(2);
        }
    }

    if (opts.pkey < 1 || opts.pkey >= 16)
    {
        std::cerr << "Invalid --pkey value: " << opts.pkey << " (expected 1..15)\n";
        std::exit(2);
    }

    if (opts.iterations < 1)
    {
        std::cerr << "Invalid --iterations value: " << opts.iterations << " (expected >= 1)\n";
        std::exit(2);
    }

    return opts;
}
}

int main(int argc, char ** argv)
{
    const Options opts = parseArgs(argc, argv);

    if (!DB::V8::PkuSupport::detail::cpuSupportsPku())
    {
        std::cout << "PKU is not supported by CPU/OS; nothing to reproduce.\n";
        return 0;
    }

    std::unique_ptr<v8::Platform> platform;
    initializeV8(platform);

    v8::Isolate::CreateParams params;
    params.array_buffer_allocator = v8::ArrayBuffer::Allocator::NewDefaultAllocator();
    v8::Isolate * isolate = v8::Isolate::New(params);

    v8::Global<v8::Context> global_ctx;
    v8::Global<v8::Function> hot_function;

    {
        v8::Locker locker(isolate);
        v8::Isolate::Scope isolate_scope(isolate);
        v8::HandleScope handle_scope(isolate);

        v8::Local<v8::Context> ctx = v8::Context::New(isolate);
        global_ctx.Reset(isolate, ctx);
        hot_function = compileFunction(isolate, ctx);
        callHot(isolate, ctx, hot_function.Get(isolate), /*iterations=*/1);

        DB::V8::PkuSupport::captureBaselinePkruForV8();
    }

    std::cout << "main pkru=" << std::hex << readPkru() << std::dec << "\n";
    std::cout << "target pkey=" << opts.pkey << " iterations=" << opts.iterations << "\n";

    std::atomic<bool> worker_finished = false;
    std::thread worker([&] {
        if (opts.disable_access)
            disableAccessForPkey(opts.pkey);

        std::cout << "worker pkru(before v8)=" << std::hex << readPkru() << std::dec << "\n";

        auto run_in_v8 = [&] {
            v8::Locker locker(isolate);
            v8::Isolate::Scope isolate_scope(isolate);
            v8::HandleScope handle_scope(isolate);
            v8::Local<v8::Context> ctx = global_ctx.Get(isolate);
            v8::Local<v8::Function> func = hot_function.Get(isolate);
            callHot(isolate, ctx, func, opts.iterations);
        };

        if (opts.use_pkru_guard)
        {
            [[maybe_unused]] DB::V8::PkuSupport::ThreadPkruGuard guard;
            std::cout << "worker pkru(with guard)=" << std::hex << readPkru() << std::dec << "\n";
            run_in_v8();
        }
        else
        {
            run_in_v8();
        }

        worker_finished.store(true, std::memory_order_release);
    });

    worker.join();

    if (worker_finished.load(std::memory_order_acquire))
        std::cout << "OK\n";

    hot_function.Reset();
    global_ctx.Reset();

    isolate->Dispose();
    delete params.array_buffer_allocator;

    disposeV8(platform);
    return 0;
}

#else

int main()
{
    std::cerr << "v8_pkru_repro requires USE_V8 on Linux x86.\n";
    return 0;
}

#endif
