#pragma once

#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HybridHashJoinMapsVariants.h>
#include <Interpreters/Streaming/HashJoin/joinKind.h>

#include <base/constexpr_helpers.h>

#include <array>

/// Used in implementation of Join to process different data structures.
namespace DB::Streaming
{
template <Kind, Strictness>
struct HybridMapGetter;

template <>
struct HybridMapGetter<Kind::Left, Strictness::Multiple>
{
    using Map = HybridMapsMultiple;
    static constexpr bool flagged = false;
};

template <>
struct HybridMapGetter<Kind::Inner, Strictness::Multiple>
{
    using Map = HybridMapsMultiple;
    static constexpr bool flagged = true;
};

template <>
struct HybridMapGetter<Kind::Right, Strictness::Multiple>
{
    using Map = HybridMapsMultiple;
    static constexpr bool flagged = true;
};

template <>
struct HybridMapGetter<Kind::Full, Strictness::Multiple>
{
    using Map = HybridMapsMultiple;
    static constexpr bool flagged = true;
};

template <>
struct HybridMapGetter<Kind::Left, Strictness::Latest>
{
    using Map = HybridMapsOne;
    static constexpr bool flagged = false;
};

template <>
struct HybridMapGetter<Kind::Inner, Strictness::Latest>
{
    using Map = HybridMapsOne;
    static constexpr bool flagged = true;
};

template <>
struct HybridMapGetter<Kind::Right, Strictness::Latest>
{
    using Map = HybridMapsAll;
    static constexpr bool flagged = true;
};

template <>
struct HybridMapGetter<Kind::Full, Strictness::Latest>
{
    using Map = HybridMapsOne;
    static constexpr bool flagged = true;
};

template <>
struct HybridMapGetter<Kind::Left, Strictness::All>
{
    using Map = HybridMapsAll;
    static constexpr bool flagged = false;
};
template <>
struct HybridMapGetter<Kind::Inner, Strictness::All>
{
    using Map = HybridMapsAll;
    static constexpr bool flagged = false;
};

template <>
struct HybridMapGetter<Kind::Right, Strictness::All>
{
    using Map = HybridMapsAll;
    static constexpr bool flagged = true;
};

template <>
struct HybridMapGetter<Kind::Full, Strictness::All>
{
    using Map = HybridMapsAll;
    static constexpr bool flagged = true;
};

template <Kind kind>
struct HybridMapGetter<kind, Strictness::Asof>
{
    using Map = HybridMapsAsof;
    static constexpr bool flagged = false;
};

template <Kind kind>
struct HybridMapGetter<kind, Strictness::Range>
{
    using Map = HybridMapsRangeAsof;
    static constexpr bool flagged = false;
};

static constexpr std::array<Strictness, 5> STRICTNESSES
    = {Strictness::All, Strictness::Range, Strictness::Asof, Strictness::Latest, Strictness::Multiple};

static constexpr std::array<Kind, 4> KINDS = {Kind::Left, Kind::Inner, Kind::Right, Kind::Full};

/// Init specified join map
template <typename MapInit>
inline bool hybridJoinDispatchInit(Kind kind, Strictness strictness, HybridMapsVariant & map_variant, MapInit map_init)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij) {
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            map_variant = typename HybridMapGetter<KINDS[i], STRICTNESSES[j]>::Map();
            map_init(std::get<typename HybridMapGetter<KINDS[i], STRICTNESSES[j]>::Map>(map_variant));
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapVariants, typename Func>
inline bool hybridJoinDispatch(Kind kind, Strictness strictness, MapVariants & map_variants, Func && func)
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
                std::get<typename HybridMapGetter<KINDS[i], STRICTNESSES[j]>::Map>(map_variants));
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapVariants, typename Func>
inline bool hybridJoinDispatch(Kind kind, Strictness strictness, std::vector<const MapVariants *> & mapv, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij) {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            using MapType = typename HybridMapGetter<KINDS[i], STRICTNESSES[j]>::Map;
            std::vector<const MapType *> v;
            for (const auto * el : mapv)
                v.push_back(&std::get<MapType>(*el));

            func(
                std::integral_constant<Kind, KINDS[i]>(), std::integral_constant<Strictness, STRICTNESSES[j]>(), v
                /*std::get<typename HybridMapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps)*/);
            return true;
        }
        return false;
    });
}

/// Call function on specified join map
template <typename MapVariants, typename Func>
inline bool hybridJoinDispatch(Kind kind, Strictness strictness, std::vector<std::vector<const MapVariants *>> & mapvv, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij) {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            using MapType = typename HybridMapGetter<KINDS[i], STRICTNESSES[j]>::Map;
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
                /*std::get<typename HybridMapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps)*/);
            return true;
        }
        return false;
    });
}

/// proton : starts. Need mutate
/// Call function on specified join map
template <typename MapVariants, typename Func>
inline bool hybridJoinDispatch(Kind kind, Strictness strictness, std::vector<MapVariants *> & mapsv, Func && func)
{
    return static_for<0, KINDS.size() * STRICTNESSES.size()>([&](auto ij) {
        // NOTE: Avoid using nested static loop as GCC and CLANG have bugs in different ways
        // See https://stackoverflow.com/questions/44386415/gcc-and-clang-disagree-about-c17-constexpr-lambda-captures
        constexpr auto i = ij / STRICTNESSES.size();
        constexpr auto j = ij % STRICTNESSES.size();
        if (kind == KINDS[i] && strictness == STRICTNESSES[j])
        {
            using MapType = typename HybridMapGetter<KINDS[i], STRICTNESSES[j]>::Map;
            std::vector<MapType *> v;
            for (auto * el : mapsv)
                v.push_back(&std::get<MapType>(*el));

            func(
                std::integral_constant<Kind, KINDS[i]>(), std::integral_constant<Strictness, STRICTNESSES[j]>(), v
                /*std::get<typename HybridMapGetter<KINDS[i], STRICTNESSES[j]>::Map>(maps)*/);
            return true;
        }
        return false;
    });
}
/// proton: end

}
