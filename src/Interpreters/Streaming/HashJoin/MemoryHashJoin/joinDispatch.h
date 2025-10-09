#pragma once

#include <Interpreters/Streaming/HashJoin/joinKind.h>

#include <base/constexpr_helpers.h>

#include <array>

/// Used in implementation of Join to process different data structures.
namespace DB::Streaming
{
template <Kind kind, Strictness>
struct MapGetter;

template <>
struct MapGetter<Kind::Left, Strictness::Multiple>
{
    using Map = MemoryHashJoin::MapsMultiple;
    static constexpr bool flagged = false;
};

template <>
struct MapGetter<Kind::Inner, Strictness::Multiple>
{
    using Map = MemoryHashJoin::MapsMultiple;
    static constexpr bool flagged = true;
};

template <>
struct MapGetter<Kind::Right, Strictness::Multiple>
{
    using Map = MemoryHashJoin::MapsMultiple;
    static constexpr bool flagged = true;
};

template <>
struct MapGetter<Kind::Full, Strictness::Multiple>
{
    using Map = MemoryHashJoin::MapsMultiple;
    static constexpr bool flagged = true;
};

template <>
struct MapGetter<Kind::Left, Strictness::Latest>
{
    using Map = MemoryHashJoin::MapsOne;
    static constexpr bool flagged = false;
};

template <>
struct MapGetter<Kind::Inner, Strictness::Latest>
{
    using Map = MemoryHashJoin::MapsOne;
    static constexpr bool flagged = true;
};

template <>
struct MapGetter<Kind::Right, Strictness::Latest>
{
    using Map = MemoryHashJoin::MapsAll;
    static constexpr bool flagged = true;
};

template <>
struct MapGetter<Kind::Full, Strictness::Latest>
{
    using Map = MemoryHashJoin::MapsOne;
    static constexpr bool flagged = true;
};

template <>
struct MapGetter<Kind::Left, Strictness::All>
{
    using Map = MemoryHashJoin::MapsAll;
    static constexpr bool flagged = false;
};
template <>
struct MapGetter<Kind::Inner, Strictness::All>
{
    using Map = MemoryHashJoin::MapsAll;
    static constexpr bool flagged = false;
};

template <>
struct MapGetter<Kind::Right, Strictness::All>
{
    using Map = MemoryHashJoin::MapsAll;
    static constexpr bool flagged = true;
};

template <>
struct MapGetter<Kind::Full, Strictness::All>
{
    using Map = MemoryHashJoin::MapsAll;
    static constexpr bool flagged = true;
};

template <Kind kind>
struct MapGetter<kind, Strictness::Asof>
{
    using Map = MemoryHashJoin::MapsAsof;
    static constexpr bool flagged = false;
};

template <Kind kind>
struct MapGetter<kind, Strictness::Range>
{
    using Map = MemoryHashJoin::MapsRangeAsof;
    static constexpr bool flagged = false;
};

static constexpr std::array<Strictness, 5> STRICTNESSES
    = {Strictness::All, Strictness::Range, Strictness::Asof, Strictness::Latest, Strictness::Multiple};

static constexpr std::array<Kind, 4> KINDS = {Kind::Left, Kind::Inner, Kind::Right, Kind::Full};

/// Init specified join map
inline bool joinDispatchInit(Kind kind, Strictness strictness, MemoryHashJoin::MapsVariant & maps)
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
inline bool joinDispatch(Kind kind, Strictness strictness, MapsVariant & map, Func && func)
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
                std::get<typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map>(map));
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapsVariant, typename Func>
inline bool joinDispatch(Kind kind, Strictness strictness, std::vector<const MapsVariant *> & mapv, Func && func)
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
            for (const auto & el : mapv)
                v.push_back(&std::get<MapType>(*el));

            func(
                std::integral_constant<Kind, KINDS[i]>(), std::integral_constant<Strictness, STRICTNESSES[j]>(), v
                /*std::get<typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps)*/);
            return true;
        }
        return false;
    });
}

/// Call function on specified join maps
template <typename MapsVariant, typename Func>
inline bool joinDispatch(Kind kind, Strictness strictness, std::vector<std::vector<const MapsVariant *>> & mapvv, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij) {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            using MapType = typename MapGetter<KINDS[i], STRICTNESSES[j]>::Map;
            std::vector<std::vector<const MapType *>> vv;
            vv.reserve(mapvv.size());
            for (const auto & el : mapvv)
            {
                auto & v = vv.emplace_back();
                v.reserve(el.size());
                for (const auto & el2 : el)
                    v.push_back(&std::get<MapType>(*el2));
            }

            func(
                std::integral_constant<Kind, KINDS[i]>(), std::integral_constant<Strictness, STRICTNESSES[j]>(), vv
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
