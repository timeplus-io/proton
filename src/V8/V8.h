#pragma once

#include <v8.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
namespace V8
{
class V8 final
{
public:
    explicit V8(const ContextPtr & global_context);
    ~V8();

    void startup();
    void shutdown();

    v8::Isolate * createIsolate();
    void disposeIsolate(v8::Isolate * isolate);

    struct IsolateData
    {
        bool out_of_memory = false;
        size_t heap_size_at_start = 0;
    };

private:
    static constexpr uint32_t V8_INFO = 0;
    static constexpr uint32_t V8_DATA_SLOT = 1;

private:
    std::atomic_flag stopped;
    std::atomic_flag started;

    std::string v8_options;
    uint64_t v8_max_heap_bytes = 0;

    std::unique_ptr<v8::Platform> platform;
    std::unique_ptr<v8::ArrayBuffer::Allocator> allocator;

    std::mutex mutex;
    std::unordered_map<v8::Isolate * , std::unique_ptr<IsolateData>> isolate_data_map;

    Poco::Logger * log;
};

}
}