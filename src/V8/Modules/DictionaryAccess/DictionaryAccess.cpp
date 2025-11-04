#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>
#include <V8/Modules/DictionaryAccess/DictionaryAccess.h>

#include <Common/Stopwatch.h>

#include <regex>

namespace DB::V8
{

DictionaryAccess & DictionaryAccess::instance()
{
    static DictionaryAccess inst;
    return inst;
}

void DictionaryAccess::initialize(ContextMutablePtr context)
{
    global_context = context;
    CacheDictionaryBridge::instance().initialize(context);
}

void DictionaryAccess::installJS(v8::Isolate * isolate, v8::Local<v8::Context> & context)
{
    context->Global()
        ->Set(
            context,
            v8::String::NewFromUtf8(isolate, "getValue").ToLocalChecked(),
            v8::FunctionTemplate::New(isolate, getValue)->GetFunction(context).ToLocalChecked())
        .Check();

    context->Global()
        ->Set(
            context,
            v8::String::NewFromUtf8(isolate, "setValue").ToLocalChecked(),
            v8::FunctionTemplate::New(isolate, setValue)->GetFunction(context).ToLocalChecked())
        .Check();

    context->Global()
        ->Set(
            context,
            v8::String::NewFromUtf8(isolate, "getCardinality").ToLocalChecked(),
            v8::FunctionTemplate::New(isolate, getCardinality)->GetFunction(context).ToLocalChecked())
        .Check();

    context->Global()
        ->Set(
            context,
            v8::String::NewFromUtf8(isolate, "batchGetValues").ToLocalChecked(),
            v8::FunctionTemplate::New(isolate, batchGetValues)->GetFunction(context).ToLocalChecked())
        .Check();

    context->Global()
        ->Set(
            context,
            v8::String::NewFromUtf8(isolate, "batchSetValues").ToLocalChecked(),
            v8::FunctionTemplate::New(isolate, batchSetValues)->GetFunction(context).ToLocalChecked())
        .Check();
}

void DictionaryAccess::log(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::Isolate * isolate = args.GetIsolate();
    if (args.Length() < 1)
        return;

    try
    {
        std::string message;
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        for (int i = 0; i < args.Length(); i++)
        {
            if (i > 0)
                message += " ";
            if (args[i]->IsObject())
            {
                v8::Local<v8::String> json = v8::JSON::Stringify(context, args[i].As<v8::Object>()).ToLocalChecked();
                v8::String::Utf8Value str(isolate, json);
                message += *str;
            }
            else
            {
                v8::String::Utf8Value str(isolate, args[i]);
                message += *str;
            }
        }

        auto logger = getLogger("DictionaryAccess");
        LOG_TRACE(logger, "JavaScript: {}", message);
    }
    catch (...)
    {
        std::cerr << "Error in log function" << std::endl;
    }
}

void DictionaryAccess::getCardinality(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    CacheDictionaryBridge::getCardinality(args);
}

void DictionaryAccess::getValue(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    CacheDictionaryBridge::getValue(args);
}

void DictionaryAccess::setValue(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    CacheDictionaryBridge::setValue(args);
}

void DictionaryAccess::batchGetValues(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    CacheDictionaryBridge::batchGetValues(args);
}

void DictionaryAccess::batchSetValues(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    CacheDictionaryBridge::batchSetValues(args);
}

void DictionaryAccess::runBenchmark(size_t operation_count)
{
    auto logger = getLogger("DictionaryAccess");
    LOG_INFO(logger, "Starting dictionary benchmark with {} operations", operation_count);

    Stopwatch total_timer;

    try
    {
        v8::V8::InitializeICU();
        std::unique_ptr<v8::Platform> platform = v8::platform::NewDefaultPlatform();
        v8::V8::InitializePlatform(platform.get());
        v8::V8::Initialize();

        v8::Isolate::CreateParams create_params;
        create_params.array_buffer_allocator = v8::ArrayBuffer::Allocator::NewDefaultAllocator();
        v8::Isolate * isolate = v8::Isolate::New(create_params);

        {
            v8::Isolate::Scope isolate_scope(isolate);
            v8::HandleScope handle_scope(isolate);
            v8::Local<v8::Context> context = v8::Context::New(isolate);
            v8::Context::Scope context_scope(context);

            installJS(isolate, context);

            // Benchmark script that tests both simple and complex key dictionaries
            const char * benchmark_code = R"(
            // Dictionary names to test - one simple key and one complex key
            const dictionaries = ["simple_key_dict", "complex_key_dict"];
            
            // Run benchmark for each dictionary
            function runBenchmark(dict_name, num_operations) {
                console.log(`Running benchmark for ${dict_name} with ${num_operations} operations`);
                
                // Track timings
                const timings = {
                    setValue: 0,
                    getValue: 0,
                    batchSet: 0,
                    batchGet: 0
                };
                
                const isSimpleKey = dict_name.includes("simple");
                
                // Single operations
                for (let i = 0; i < num_operations; i++) {
                    // Create key based on dictionary type
                    const key = isSimpleKey 
                        ? i + 1000  // Simple numeric key
                        : { key1: `key_${i}`, key2: `val_${i}` };  // Complex key
                    
                    // Create complete value object with all required fields
                    const value = isSimpleKey
                        ? {
                            id: key,  // Include key field in the value for simple key dict
                            value1: `value_${i}`,
                            value2: i,
                            active: i % 2 === 0
                          }
                        : {
                            key1: key.key1,  // Include key fields in the value for complex key dict
                            key2: key.key2,
                            value1: `value_${i}`,
                            value2: i,
                            active: i % 2 === 0
                          };
                    
                    // Test setValue
                    const setValue_start = Date.now();
                    setValue(dict_name, key, value);
                    timings.setValue += Date.now() - setValue_start;
                    
                    // Test getValue
                    const getValue_start = Date.now();
                    const retrieved = getValue(dict_name, key);
                    timings.getValue += Date.now() - getValue_start;
                    
                    if (i % 100 === 0) {
                        console.log(`Completed ${i} single operations`);
                    }
                }
                
                // Batch operations (10 batches of batch_size)
                const batch_size = Math.min(100, Math.floor(num_operations / 10));
                for (let b = 0; b < 10; b++) {
                    const keys = [];
                    const values = [];
                    
                    // Create batch data
                    for (let i = 0; i < batch_size; i++) {
                        const index = b * batch_size + i;
                        
                        // Create key based on dictionary type
                        const key = isSimpleKey 
                            ? index + 5000  // Simple numeric key for batch ops
                            : { key1: `batch_${index}`, key2: `val_${index}` };  // Complex key
                        
                        keys.push(key);
                        
                        // Create value based on dictionary type
                        const value = isSimpleKey
                            ? {
                                id: key,  // Include key field in the value for simple key dict
                                value1: `batch_value_${index}`,
                                value2: index,
                                active: index % 2 === 0
                              }
                            : {
                                key1: key.key1,  // Include key fields in the value for complex key dict
                                key2: key.key2,
                                value1: `batch_value_${index}`,
                                value2: index,
                                active: index % 2 === 0
                              };
                              
                        values.push(value);
                    }
                    
                    // Test batchSetValues
                    const batchSet_start = Date.now();
                    batchSetValues(dict_name, keys, values);
                    timings.batchSet += Date.now() - batchSet_start;
                    
                    // Test batchGetValues
                    const batchGet_start = Date.now();
                    const batchResults = batchGetValues(dict_name, keys);
                    timings.batchGet += Date.now() - batchGet_start;
                    
                    console.log(`Completed batch ${b+1}/10`);
                }
                
                // Calculate operations per second
                const single_ops = num_operations;
                const batch_ops = 10 * batch_size;
                const total_ops = single_ops * 2 + batch_ops * 2;
                
                const setValue_ops_sec = (single_ops / (timings.setValue / 1000)).toFixed(2);
                const getValue_ops_sec = (single_ops / (timings.getValue / 1000)).toFixed(2);
                const batchSet_ops_sec = (batch_ops / (timings.batchSet / 1000)).toFixed(2);
                const batchGet_ops_sec = (batch_ops / (timings.batchGet / 1000)).toFixed(2);
                
                // Print results
                console.log(`\nResults for ${dict_name}:`);
                console.log(`setValue: ${timings.setValue}ms (${setValue_ops_sec} ops/sec)`);
                console.log(`getValue: ${timings.getValue}ms (${getValue_ops_sec} ops/sec)`);
                console.log(`batchSetValues: ${timings.batchSet}ms (${batchSet_ops_sec} ops/sec)`);
                console.log(`batchGetValues: ${timings.batchGet}ms (${batchGet_ops_sec} ops/sec)`);
                
                return {
                    dictionary: dict_name,
                    single_ops,
                    batch_ops,
                    total_ops,
                    timings,
                    ops_per_sec: {
                        setValue: parseFloat(setValue_ops_sec),
                        getValue: parseFloat(getValue_ops_sec),
                        batchSet: parseFloat(batchSet_ops_sec),
                        batchGet: parseFloat(batchGet_ops_sec)
                    }
                };
            }
            
            // Run benchmarks for all dictionaries
            function runAllBenchmarks(num_operations) {
                const start_time = Date.now();
                const results = [];
                
                for (const dict_name of dictionaries) {
                    // Check if dictionary exists
                    try {
                        const size = getCardinality(dict_name);
                        console.log(`Dictionary ${dict_name} has ${size} entries before benchmark`);
                        results.push(runBenchmark(dict_name, num_operations));
                    } catch (e) {
                        console.log(`Skipping ${dict_name}: ${e.message}`);
                    }
                }
                
                const total_time = Date.now() - start_time;
                console.log(`\nTotal benchmark time: ${(total_time / 1000).toFixed(2)} seconds`);
                
                return {
                    total_time,
                    results
                };
            }
            
            // Run with specified number of operations
            runAllBenchmarks(OPERATION_COUNT);
            )";

            // Replace OPERATION_COUNT with the actual count
            std::string script_with_count = std::string(benchmark_code);
            script_with_count = std::regex_replace(script_with_count, std::regex("OPERATION_COUNT"), std::to_string(operation_count));

            v8::Local<v8::String> source
                = v8::String::NewFromUtf8(isolate, script_with_count.c_str(), v8::NewStringType::kNormal).ToLocalChecked();
            v8::Local<v8::Script> script = v8::Script::Compile(context, source).ToLocalChecked();
            script->Run(context).ToLocalChecked();
        }

        isolate->Dispose();
        v8::V8::Dispose();
        v8::V8::DisposePlatform();
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Error during benchmark: {}", e.displayText());
    }
    catch (...)
    {
        LOG_ERROR(logger, "Unknown error during benchmark");
    }

    LOG_INFO(logger, "Dictionary benchmark completed in {} ms", total_timer.elapsedMilliseconds());
}

}
