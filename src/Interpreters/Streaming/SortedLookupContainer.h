#pragma once

#include <deque>

namespace DB::Streaming
{
/// Sorted container for asof join
/// 1) Fast lookup
/// 2) Fast push back
/// 3) Fast pop front
template <typename RowRefDataBlock, typename TEntry>
class SortedLookupContainer
{
public:
    using Base = std::deque<TEntry>;

    void insert(TEntry entry, bool ascending, size_t max_size)
    {
        /// Happy and perf path
        if (ascending)
        {
            if (array.empty() || less(array.back(), entry)) /// Very likely
            {
                array.push_back(std::move(entry));
                eraseIfExceedingMaxSize(max_size, ascending);
                return;
            }
        }
        else
        {
            if (array.empty() || less(array.front(), entry)) /// Very likely
            {
                array.push_front(std::move(entry));
                eraseIfExceedingMaxSize(max_size, ascending);
                return;
            }
        }

        /// Slow path
        auto it = std::lower_bound(array.begin(), array.end(), entry, (ascending ? less : greater));
        array.insert(it, std::move(entry));
        eraseIfExceedingMaxSize(max_size, ascending);
    }

    const RowRefDataBlock * upperBound(const TEntry & k, bool ascending)
    {
        auto it = std::upper_bound(array.cbegin(), array.cend(), k, (ascending ? less : greater));
        if (it != array.cend())
            return &(it->row_ref);
        return nullptr;
    }

    const RowRefDataBlock * lowerBound(const TEntry & k, bool ascending)
    {
        auto it = std::lower_bound(array.cbegin(), array.cend(), k, (ascending ? less : greater));
        if (it != array.cend())
            return &(it->row_ref);
        return nullptr;
    }

    using iterator = typename Base::iterator;
    using const_iterator = typename Base::const_iterator;

    iterator begin() { return array.begin(); }
    iterator end() { return array.end(); }

    size_t size() const { return array.size(); }
    void resize(size_t s) { array.resize(s); }

    const_iterator begin() const { return array.begin(); }
    const_iterator end() const { return array.end(); }

private:
    inline void eraseIfExceedingMaxSize(size_t max_size, bool ascending)
    {
        if (array.size() > max_size)
        {
            if (ascending)
                array.pop_front();
            else
                array.pop_back();
        }
    }

private:
    Base array;

    static bool less(const TEntry & a, const TEntry & b) { return a.asof_value < b.asof_value; }

    static bool greater(const TEntry & a, const TEntry & b) { return a.asof_value > b.asof_value; }
};

}
