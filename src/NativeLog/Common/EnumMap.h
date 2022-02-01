#pragma once

#include <array>
#include <cassert>
#include <functional>
#include <vector>

namespace nlog
{
template <typename Enum, typename Val, Enum InvalidEnum = Enum::INVALID, int32_t Size = static_cast<int32_t>(Enum::MAX)>
class EnumMap
{
public:
    EnumMap() : map_() { map_.fill(invalidValue()); }

    auto begin() const { return map_.cbegin(); }

    auto end() const { return map_.cend(); }

    auto size() const { return map_.size(); }

    const Val & operator[](int32_t n) const
    {
        if (n >= 0 && n < Size)
            return map_[n];
        else
            return invalidValue();
    }

    const Val & operator[](Enum n) const { return (*this)[static_cast<int32_t>(n)]; }

    template <typename T>
    Enum reverseLookup(const T & search_val) const
    {
        return reverseLookup<T>(search_val, [](const T & lhs, const T & rhs) { return lhs == rhs; });
    }

    template <typename T>
    Enum reverseLookup(const T & search_val, std::function<bool(const T &, const Val &)> equal) const
    {
        if (cmp(search_val, invalidValue()))
            return InvalidEnum;

        int32_t idx = 0;

        for (auto & val : map_)
        {
            if (equal(search_val, val))
            {
                return static_cast<Enum>(idx);
            }
            ++idx;
        }

        return invalidEnum();
    }

    std::vector<Enum> allValidKeys() const
    {
        std::vector<Enum> vec;

        for (int32_t i = 0; i < Size; ++i)
            if (map_[i] != invalidValue())
                vec.push_back(static_cast<Enum>(i));

        return vec;
    }

    template <typename ValT>
    void set(Enum n, ValT && val)
    {
        assert(static_cast<int32_t>(n) < Size);
        map_[static_cast<int32_t>(n)] = std::forward<ValT>(val);
    }

    static constexpr Enum invalidEnum() { return InvalidEnum; }

    /// Must be specialized
    static const Val & invalidValue();

private:
    void setValues();

    std::array<Val, Size> map_;
};
}
