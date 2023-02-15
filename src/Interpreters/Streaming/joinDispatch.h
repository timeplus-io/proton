#pragma once

#include "HashJoin.h"
#include "joinKind.h"

#include <base/constexpr_helpers.h>

#include <array>

/** Used in implementation of Join to process different data structures.
  */

namespace DB
{
namespace Streaming
{
template <Kind kind, Strictness>
struct MapGetter;

template <>
struct MapGetter<Kind::Left, Strictness::Any>
{
    using Map = HashJoin::MapsOne;
    static constexpr bool flagged = false;
};

template <>
struct MapGetter<Kind::Inner, Strictness::Any>
{
    using Map = HashJoin::MapsOne;
    static constexpr bool flagged = true;
};

template <>
struct MapGetter<Kind::Right, Strictness::Any>
{
    using Map = HashJoin::MapsAll;
    static constexpr bool flagged = true;
};

template <>
struct MapGetter<Kind::Left, Strictness::All>
{
    using Map = HashJoin::MapsAll;
    static constexpr bool flagged = false;
};
template <>
struct MapGetter<Kind::Inner, Strictness::All>
{
    using Map = HashJoin::MapsAll;
    static constexpr bool flagged = false;
};

template <>
struct MapGetter<Kind::Right, Strictness::All>
{
    using Map = HashJoin::MapsAll;
    static constexpr bool flagged = true;
};

template <Kind kind>
struct MapGetter<kind, Strictness::Asof>
{
    using Map = HashJoin::MapsAsof;
    static constexpr bool flagged = false;
};

template <Kind kind>
struct MapGetter<kind, Strictness::RangeAsof>
{
    using Map = HashJoin::MapsRangeAsof;
    static constexpr bool flagged = false;
};

template <Kind kind>
struct MapGetter<kind, Strictness::Range>
{
    using Map = HashJoin::MapsRangeAsof;
    static constexpr bool flagged = false;
};

static constexpr std::array<Strictness, 5> STRICTNESSES
    = {Strictness::All, Strictness::Range, Strictness::RangeAsof, Strictness::Asof, Strictness::Any};

static constexpr std::array<Kind, 3> KINDS = {Kind::Left, Kind::Inner, Kind::Right};

/// Init specified join map
inline bool joinDispatchInit(Kind kind, Strictness strictness, HashJoin::MapsVariant & maps)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij) {
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            maps = typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map();
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapsVariant, typename Func>
inline bool joinDispatch(Kind kind, Strictness strictness, MapsVariant & maps, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij) {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            func(
                std::integral_constant<Kind, KINDS[i]>(),
                std::integral_constant<Strictness, STRICTNESSES[j]>(),
                std::get<typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps));
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapsVariant, typename Func>
inline bool joinDispatch(Kind kind, Strictness strictness, std::vector<const MapsVariant *> & mapsv, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij) {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            using MapType = typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map;
            std::vector<const MapType *> v;
            for (const auto & el : mapsv)
                v.push_back(&std::get<MapType>(*el));

            func(
                std::integral_constant<Kind, KINDS[i]>(), std::integral_constant<Strictness, STRICTNESSES[j]>(), v
                /*std::get<typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps)*/);
            return true;
        }
        return false;
    });
}

/// proton : starts. Need mutate
/// Call function on specified join map
template <typename MapsVariant, typename Func>
inline bool joinDispatch(Kind kind, Strictness strictness, std::vector<MapsVariant *> & mapsv, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij) {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            using MapType = typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map;
            std::vector<MapType *> v;
            for (auto & el : mapsv)
                v.push_back(&std::get<MapType>(*el));

            func(
                std::integral_constant<Kind, KINDS[i]>(), std::integral_constant<Strictness, STRICTNESSES[j]>(), v
                /*std::get<typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps)*/);
            return true;
        }
        return false;
    });
}
/// proton: end
}
}
