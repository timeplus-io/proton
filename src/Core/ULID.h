#pragma once

#include <Core/UUID.h>
#include <IO/WriteHelpers.h>

#if USE_ULID

#include <ulid.h>

namespace DB
{

struct ULIDHelpers
{
    ULIDHelpers() { ulid_generator_init(&generator, 0); }

    std::string generate()
    {
        std::string id;
        id.resize(26);

        ulid_generate(&generator, id.data());

        return id;
    }

private:
    ulid_generator generator;
};

}

#else

namespace DB
{

struct ULIDHelpers
{
    ULIDHelpers() { }

    std::string generate() { return DB::toString(UUIDHelpers::generateV4()); }
};

}

#endif
