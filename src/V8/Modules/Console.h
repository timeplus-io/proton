#pragma once

#include <v8.h>

#include <string>

namespace DB
{

namespace V8
{
/// Install the module console to the specified v8 context,
/// \param func_name the function name of Javascript UDF or UDA, which is used to get the corresponding function logger
void installConsole(v8::Isolate * isolate, v8::Local<v8::Context> & ctx, const std::string & func_name);

}
}
