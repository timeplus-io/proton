#include <V8/ConvertDataTypes.h>
#include <V8/Modules/Console.h>

#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace V8
{

void log(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::Isolate * isolate = args.GetIsolate();
    assert(args.Length() >= 0);
    std::vector<String> result(args.Length());
    try
    {
        auto context = isolate->GetCurrentContext();
        for (int i = 0; i < args.Length(); i++)
        {
            if (args[i]->IsString() || args[i]->IsNumber())
            {
                result[i] = from_v8<String>(isolate, args[i]);
            }
            else if (args[i]->IsObject())
            {
                /// trans Object to String
                bool is_key_value = false;
                auto obj = args[i].As<v8::Object>();
                /// trans Map,Set to String
                auto array = obj->PreviewEntries(&is_key_value);
                if (!array.IsEmpty())
                    result[i] = from_v8<String>(isolate, array.ToLocalChecked()->ToString(context).ToLocalChecked());
                else if (obj->IsRegExp())
                    result[i] = from_v8<String>(isolate, obj->ToString(context).ToLocalChecked());
                else
                    result[i] = from_v8<String>(isolate, v8::JSON::Stringify(context, obj).ToLocalChecked());
            }
            else
            {
                /// default trans to String
                result[i] = from_v8<String>(isolate, args[i]->ToString(context).ToLocalChecked());
            }
        }
        LOG_INFO(&Poco::Logger::get(from_v8<String>(isolate, args.Data())), "{}", fmt::join(result, " "));
    }
    catch (DB::Exception & e)
    {
        LOG_ERROR(&Poco::Logger::get(from_v8<String>(isolate, args.Data())), "Hit an udf/uda error : {}", e.what());
    }
    catch(...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

v8::Local<v8::Object> WrapObject(v8::Isolate * isolate, const std::string & func_name)
{
    v8::EscapableHandleScope handle_scope(isolate);

    v8::Local<v8::ObjectTemplate> module = v8::ObjectTemplate::New(isolate);

    module->SetInternalFieldCount(1);

    /// add 'log' function
    v8::Local<v8::Value> logger_name = to_v8(isolate, fmt::format("{}({})", DB::ProtonConsts::PROTON_JAVASCRIPT_UDF_LOGGER_PREFIX, func_name));
    module->Set(isolate, "log", v8::FunctionTemplate::New(isolate, log, logger_name));

    /// create instance
    v8::Local<v8::Object> result = module->NewInstance(isolate->GetCurrentContext()).ToLocalChecked();

    return handle_scope.Escape(result);
}

void installConsole(v8::Isolate * isolate, v8::Local<v8::Context> & ctx, const std::string & func_name)
{
    v8::Local<v8::Object> console = WrapObject(isolate, func_name);
    ctx->Global()->Set(ctx, to_v8(isolate, "console"), console).Check();
}

}
}
