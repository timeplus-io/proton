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
    LOG_DEBUG(&Poco::Logger::get(V8::from_v8<String>(isolate, args.Data())), "{}", V8::from_v8<String>(isolate, args[0]));
}

v8::Local<v8::Object> WrapObject(v8::Isolate * isolate, const std::string & func_name)
{
    v8::EscapableHandleScope handle_scope(isolate);

    v8::Local<v8::ObjectTemplate> module = v8::ObjectTemplate::New(isolate);

    module->SetInternalFieldCount(1);

    /// add 'log' function
    v8::Local<v8::Value> logger_name = to_v8(isolate, fmt::format("{}({})", DB::ProtonConsts::PROTON_FUNC_LOGGER_PREFIX, func_name));
    module->Set(isolate, "log", v8::FunctionTemplate::New(isolate, log, logger_name));

    /// create instance
    v8::Local<v8::Object> result = module->NewInstance(isolate->GetCurrentContext()).ToLocalChecked();

    return handle_scope.Escape(result);
}

void installConsole(v8::Isolate * isolate, v8::Local<v8::Context> & ctx, const std::string func_name)
{
    v8::Local<v8::Object> console = WrapObject(isolate, func_name);
    ctx->Global()->Set(ctx, to_v8(isolate, "console"), console).Check();
}

}
}
