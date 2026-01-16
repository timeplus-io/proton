#include "Server.h"
#include "config.h"

#if USE_V8
#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>
#include <V8/PkuSupport.h>
#include <V8/V8Includes.h>
#endif

namespace DB
{

/// Init v8 engine for the whole proton process
void Server::initV8()
{
#if USE_V8
    if (v8_initialized)
        return;

    /// Init ICU and default platform (work thread pool default to CPU count - 1)
    v8::V8::InitializeICU();
    /// Init default platform which enable a work thread pool and the default pool size is: the number of CPU processors -1
    platform = v8::platform::NewDefaultPlatform();
    v8::V8::InitializePlatform(platform.get());
    v8::V8::Initialize();

    V8::PkuSupport::captureBaselinePkruForV8();

    DB::V8::CacheDictionaryBridge::instance().initialize(global_context);
    v8_initialized = true;
#endif
}

void Server::disposeV8()
{
#if USE_V8
    if (!v8_initialized)
        return;

    v8::V8::Dispose();
    v8::V8::DisposePlatform();
    v8_initialized = false;
#endif
}

void Server::maybeDisposeV8()
{
#if USE_V8
    if (v8_initialized)
        disposeV8();
#endif
}
}
