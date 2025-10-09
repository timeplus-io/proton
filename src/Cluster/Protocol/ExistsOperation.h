#pragma once

namespace cluster::protocol
{
/// Options for available operations when creating a resource and that resource already exists.
enum ExistsOperation : uint8_t
{
    Ignore = 1, /// do nothing
    Replace = 2, /// replace with the new data
    Throw = 3, /// throw an exception
};
}
