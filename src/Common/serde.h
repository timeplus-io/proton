#pragma once

#include <Common/VersionRevision.h>

namespace DB
{
/// macro tag to indicate the data members or struct or class will
/// be serialized / deserialized via network or file system IO.
/// Hence, data structure versioning / backward / forward compatibility
/// are concerns
#define SERDE
#define NO_SERDE
}