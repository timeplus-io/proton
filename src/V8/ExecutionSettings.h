#pragma once

#include <V8/V8Includes.h>

namespace DB::V8
{

struct ExecutionSettings
{
    bool dictionary_getvalue_null_on_missing_key = false;
};

namespace detail
{
/// Slots 0 and 1 are reserved by V8::V8-created isolates. JavaScript UDF
/// isolates are created directly, but keep this slot distinct to avoid
/// collisions if isolate creation is unified later.
inline constexpr uint32_t EXECUTION_SETTINGS_SLOT = 2;
}

inline void setExecutionSettings(v8::Isolate * isolate, const ExecutionSettings * settings)
{
    isolate->SetData(detail::EXECUTION_SETTINGS_SLOT, const_cast<ExecutionSettings *>(settings));
}

inline const ExecutionSettings * getExecutionSettings(v8::Isolate * isolate)
{
    return static_cast<const ExecutionSettings *>(isolate->GetData(detail::EXECUTION_SETTINGS_SLOT));
}

inline bool dictionaryGetValueNullOnMissingKey(v8::Isolate * isolate)
{
    const auto * settings = getExecutionSettings(isolate);
    return settings && settings->dictionary_getvalue_null_on_missing_key;
}

}
