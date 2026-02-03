#pragma once

/// Suppress warnings originating from V8 headers only.
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wundef"
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wundefined-reinterpret-cast"
#pragma clang diagnostic ignored "-Wnon-virtual-dtor"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#endif

#include <v8.h>
#include <libplatform/libplatform.h>

#if defined(__clang__)
#pragma clang diagnostic pop
#endif
