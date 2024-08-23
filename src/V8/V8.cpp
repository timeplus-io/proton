#include <Interpreters/Context.h>
#include <V8/V8.h>
#include <base/getMemoryAmount.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace V8
{

namespace
{
/// FIXME, use our own allocator
class ArrayBufferAllocator : public v8::ArrayBuffer::Allocator
{
public:
    void * Allocate(size_t length) override
    {
        void * data = AllocateUninitialized(length);
        return data == nullptr ? data : memset(data, 0, length);
    }

    void * AllocateUninitialized(size_t length) override { return malloc(length); }

    void Free(void * data, size_t) override { free(data); }
};

void gcPrologueCallback(v8::Isolate * isolate, v8::GCType type, v8::GCCallbackFlags flags)
{
    (void)isolate;
    (void)type;
    (void)flags;
}

void gcEpilogueCallback(v8::Isolate * isolate, v8::GCType type, v8::GCCallbackFlags flags)
{
    (void)isolate;
    (void)type;
    (void)flags;
}

void OOMCallback(const char* location, const v8::OOMDetails& oom_details) {
    (void)location;
    (void)oom_details;
}

void fatalErrorHandler(const char * location, const char * message)
{
    (void)location;
    (void)message;
}
}

V8::V8(const ContextPtr & global_context) : log(&Poco::Logger::get("V8"))
{
}

void V8::startup()
{
    if (started.test_and_set())
        return;

    v8::V8::InitializeICU();
    v8::V8::SetFlagsFromString(v8_options.c_str(), static_cast<int>(v8_options.size()));

    platform = v8::platform::NewDefaultPlatform();
    v8::V8::InitializePlatform(platform.get());
    v8::V8::Initialize();

    allocator.reset(new ArrayBufferAllocator);
}

void V8::shutdown()
{
    if (stopped.test_and_set())
        return;

    v8::V8::Dispose();
    v8::V8::DisposePlatform();

    platform.reset();
    allocator.reset();
}

v8::Isolate * V8::createIsolate()
{
    v8::Isolate::CreateParams isolate_params;
    isolate_params.array_buffer_allocator = allocator.get();

    v8_max_heap_bytes = static_cast<size_t>(getMemoryAmountOrZero() * 0.6);
    isolate_params.constraints.ConfigureDefaultsFromHeapSize(0, v8_max_heap_bytes);
    isolate_params.constraints.set_max_old_generation_size_in_bytes(v8_max_heap_bytes);

    auto * isolate = v8::Isolate::New(isolate_params);
    assert(isolate);
    isolate->SetOOMErrorHandler(OOMCallback);
    isolate->SetFatalErrorHandler(fatalErrorHandler);
    isolate->AddGCPrologueCallback(gcPrologueCallback);
    isolate->AddGCEpilogueCallback(gcEpilogueCallback);

    auto data = std::make_unique<IsolateData>();
    isolate->SetData(V8_INFO, data.get());

    {
        std::scoped_lock lock(mutex);
        /// FIXME, limit the number of concurrent isolates
        try
        {
            isolate_data_map.try_emplace(isolate, std::move(data));
        }
        catch (...)
        {
            isolate->SetData(V8_INFO, nullptr);
            isolate->Dispose();
            throw;
        }
    }

    return isolate;
}

void V8::disposeIsolate(v8::Isolate * isolate)
{
    {
        std::scoped_lock lock(mutex);
        isolate_data_map.erase(isolate);
    }

    isolate->Dispose();
}
}
}
