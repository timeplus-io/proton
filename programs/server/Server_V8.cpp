#include "Server.h"

#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>

#include <v8.h>

namespace DB
{

/// Init v8 engine for the whole proton process
void Server::initV8()
{
    if (v8_initialized)
        return;

    /// Init default platform which enable a work thread pool and the default pool size is: the number of CPU processors -1
    platform = v8::platform::NewDefaultPlatform();
    v8::V8::InitializePlatform(platform.get());
    v8::V8::Initialize();

    DB::V8::CacheDictionaryBridge::instance().initialize(global_context);
    v8_initialized = true;
}

void Server::disposeV8()
{
    if (!v8_initialized)
        return;

    v8::V8::Dispose();
    v8::V8::DisposePlatform();
    v8_initialized = false;
}

}
