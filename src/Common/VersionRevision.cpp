#include <Common/VersionRevision.h>
#include <Common/config_version.h>

namespace ProtonRevision
{
    unsigned getVersionRevision() { return VERSION_REVISION; }
    unsigned getVersionInteger() { return VERSION_INTEGER; }
}
