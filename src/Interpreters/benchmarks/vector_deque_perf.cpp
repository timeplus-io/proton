#include <deque>
#include <list>
#include <vector>

#include <benchmark/benchmark.h>

struct RowRefWithRefCount
{
    std::list<int32_t> * blocks;
    std::list<int32_t>::iterator iter;
    uint32_t row_num = 0;
};

struct Entry
{
    explicit Entry(uint64_t value_) : value(value_) { }

    uint64_t value;
    RowRefWithRefCount row_ref;
};

bool lessEntry(const Entry & lhs, const Entry & rhs)
{
    return lhs.value < rhs.value;
}

bool greaterEntry(const Entry & lhs, const Entry & rhs)
{
    return lhs.value > rhs.value;
}

template <typename... Args>
void vectorPerfVanilla(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto max_size = std::get<0>(args_tuple);

    std::vector<Entry> sorted_row_refs;
    for (uint64_t i = 0; auto _ : state)
    {
        sorted_row_refs.emplace_back(i++);
        if (sorted_row_refs.size() > max_size)
            sorted_row_refs.erase(sorted_row_refs.begin());
        benchmark::ClobberMemory();
    }
}

template <typename... Args>
void dequePerfVanilla(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto max_size = std::get<0>(args_tuple);

    std::deque<Entry> sorted_row_refs;
    for (uint64_t i = 0; auto _ : state)
    {
        sorted_row_refs.emplace_back(i++);
        if (sorted_row_refs.size() > max_size)
            sorted_row_refs.pop_front();
        benchmark::ClobberMemory();
    }
}

template <typename... Args>
void vectorPerfSortInsertDelete(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto max_size = std::get<0>(args_tuple);
    auto ascending = std::get<1>(args_tuple);

    std::vector<Entry> sorted_row_refs;
    for (uint64_t i = 0; auto _ : state)
    {
        Entry entry(i++);
        auto it = std::lower_bound(sorted_row_refs.begin(), sorted_row_refs.end(), entry, ascending ? lessEntry : greaterEntry);
        sorted_row_refs.insert(it, entry);


        if (sorted_row_refs.size() > max_size)
        {
            if (ascending)
                sorted_row_refs.erase(sorted_row_refs.begin(), sorted_row_refs.begin() + sorted_row_refs.size() - max_size);
            else
                sorted_row_refs.erase(sorted_row_refs.end() - (sorted_row_refs.size() - max_size), sorted_row_refs.end());
        }

        benchmark::ClobberMemory();
    }
}

template <typename... Args>
void dequePerfSortInsertDelete(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto max_size = std::get<0>(args_tuple);
    auto ascending = std::get<1>(args_tuple);

    std::deque<Entry> sorted_row_refs;
    for (uint64_t i = 0; auto _ : state)
    {
        Entry entry(i++);
        auto it = std::lower_bound(sorted_row_refs.begin(), sorted_row_refs.end(), entry, ascending ? lessEntry : greaterEntry);
        sorted_row_refs.insert(it, entry);

        if (sorted_row_refs.size() > max_size)
        {
            if (ascending)
                sorted_row_refs.erase(sorted_row_refs.begin(), sorted_row_refs.begin() + sorted_row_refs.size() - max_size);
            else
                sorted_row_refs.erase(sorted_row_refs.end() - (sorted_row_refs.size() - max_size), sorted_row_refs.end());
        }

        benchmark::ClobberMemory();
    }
}

template <typename... Args>
void vectorPerfSortInsertDeleteOpt(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto max_size = std::get<0>(args_tuple);
    auto ascending = std::get<1>(args_tuple);

    std::vector<Entry> sorted_row_refs;
    for (uint64_t i = 0; auto _ : state)
    {
        Entry entry(i++);

        if (ascending)
        {
            if (sorted_row_refs.empty() || lessEntry(sorted_row_refs.back(), entry)) /// likely
            {
                sorted_row_refs.emplace_back(std::move(entry));
                if (sorted_row_refs.size() > max_size)
                {
                    sorted_row_refs.erase(sorted_row_refs.begin());
                    /// sorted_row_refs.shrink_to_fit();
                }

                benchmark::ClobberMemory();
                continue;
            }
        }
        else
        {
            if (sorted_row_refs.empty() || lessEntry(sorted_row_refs.front(), entry)) /// likely
            {
                sorted_row_refs.insert(sorted_row_refs.begin(), std::move(entry));
                if (sorted_row_refs.size() > max_size)
                {
                    sorted_row_refs.pop_back();
                    /// sorted_row_refs.shrink_to_fit();
                }

                benchmark::ClobberMemory();
                continue;
            }
        }

        auto it = std::lower_bound(sorted_row_refs.begin(), sorted_row_refs.end(), entry, ascending ? lessEntry : greaterEntry);
        sorted_row_refs.insert(it, std::move(entry));

        if (sorted_row_refs.size() > max_size)
        {
            if (ascending)
                sorted_row_refs.erase(sorted_row_refs.begin());
            else
                sorted_row_refs.pop_back();

            /// sorted_row_refs.shrink_to_fit();
        }

        benchmark::ClobberMemory();
    }
}

template <typename... Args>
void dequePerfSortInsertDeleteOpt(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto max_size = std::get<0>(args_tuple);
    auto ascending = std::get<1>(args_tuple);

    std::deque<Entry> sorted_row_refs;
    for (uint64_t i = 0; auto _ : state)
    {
        Entry entry(i++);
        if (ascending)
        {
            if (sorted_row_refs.empty() || lessEntry(sorted_row_refs.back(), entry)) /// likely
            {
                sorted_row_refs.push_back(std::move(entry));
                if (sorted_row_refs.size() > max_size)
                    sorted_row_refs.pop_front();

                continue;
            }
        }
        else
        {
            if (sorted_row_refs.empty() || lessEntry(sorted_row_refs.front(), entry)) /// likely
            {
                sorted_row_refs.push_front(std::move(entry));
                if (sorted_row_refs.size() > max_size)
                    sorted_row_refs.pop_back();

                continue;
            }
        }

        auto it = std::lower_bound(sorted_row_refs.begin(), sorted_row_refs.end(), entry, ascending ? lessEntry : greaterEntry);
        sorted_row_refs.insert(it, entry);

        if (sorted_row_refs.size() > max_size)
        {
            if (ascending)
                sorted_row_refs.pop_front();
            else
                sorted_row_refs.pop_back();
        }

        benchmark::ClobberMemory();
    }
}

template <typename... Args>
void vectorLookup(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto max_size = std::get<0>(args_tuple);
    auto ascending = std::get<1>(args_tuple);

    std::vector<Entry> sorted_row_refs;
    sorted_row_refs.reserve(max_size);

    if (ascending)
    {
        for (size_t i = 0; i < max_size; ++i)
            sorted_row_refs.emplace_back(i);
    }
    else
    {
        for (size_t i = max_size; i > 0; --i)
            sorted_row_refs.emplace_back(i - 1);
    }

    for (size_t i = 0; auto _ : state)
        benchmark::DoNotOptimize(std::lower_bound(
            sorted_row_refs.begin(), sorted_row_refs.end(), Entry(i % (max_size << 1)), (ascending ? lessEntry : greaterEntry)));
}

template <typename... Args>
void dequeLookup(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto max_size = std::get<0>(args_tuple);
    auto ascending = std::get<1>(args_tuple);

    std::deque<Entry> sorted_row_refs;

    if (ascending)
    {
        for (size_t i = 0; i < max_size; ++i)
            sorted_row_refs.emplace_back(i);
    }
    else
    {
        for (size_t i = max_size; i > 0; --i)
            sorted_row_refs.emplace_back(i - 1);
    }

    for (size_t i = 0; auto _ : state)
        benchmark::DoNotOptimize(std::lower_bound(
            sorted_row_refs.begin(), sorted_row_refs.end(), Entry(i % (max_size << 1)), (ascending ? lessEntry : greaterEntry)));
}

BENCHMARK_CAPTURE(vectorPerfVanilla, vectorPerfVanilla10, /*keep_versions=*/10ull);
BENCHMARK_CAPTURE(vectorPerfVanilla, vectorPerfVanilla100, /*keep_versions=*/100ull);
BENCHMARK_CAPTURE(vectorPerfVanilla, vectorPerfVanilla1000, /*keep_versions=*/1000ull);

BENCHMARK_CAPTURE(dequePerfVanilla, dequePerfVanilla10, /*keep_versions=*/10ull);
BENCHMARK_CAPTURE(dequePerfVanilla, dequePerfVanilla100, /*keep_versions=*/100ull);
BENCHMARK_CAPTURE(dequePerfVanilla, dequePerfVanilla1000, /*keep_versions=*/1000ull);

BENCHMARK_CAPTURE(vectorPerfSortInsertDelete, vectorPerfSortInsertDelete10asc, /*keep_versions=*/10ull, /*ascending=*/true);
BENCHMARK_CAPTURE(vectorPerfSortInsertDelete, vectorPerfSortInsertDelete100asc, /*keep_versions=*/100ull, /*ascending=*/true);
BENCHMARK_CAPTURE(vectorPerfSortInsertDelete, vectorPerfSortInsertDelete1000desc, /*keep_versions=*/1000ull, /*ascending=*/false);

BENCHMARK_CAPTURE(vectorPerfSortInsertDelete, vectorPerfSortInsertDelete10desc, /*keep_versions=*/10ull, /*ascending=*/false);
BENCHMARK_CAPTURE(vectorPerfSortInsertDelete, vectorPerfSortInsertDelete100desc, /*keep_versions=*/100ull, /*ascending=*/false);
BENCHMARK_CAPTURE(vectorPerfSortInsertDelete, vectorPerfSortInsertDelete1000desc, /*keep_versions=*/1000ull, /*ascending=*/false);

BENCHMARK_CAPTURE(vectorPerfSortInsertDeleteOpt, vectorPerfSortInsertDeleteOpt10asc, /*keep_versions=*/10ull, /*ascending=*/true);
BENCHMARK_CAPTURE(vectorPerfSortInsertDeleteOpt, vectorPerfSortInsertDeleteOpt100asc, /*keep_versions=*/100ull, /*ascending=*/true);
BENCHMARK_CAPTURE(vectorPerfSortInsertDeleteOpt, vectorPerfSortInsertDeleteOpt1000asc, /*keep_versions=*/1000ull, /*ascending=*/true);

BENCHMARK_CAPTURE(vectorPerfSortInsertDeleteOpt, vectorPerfSortInsertDeleteOpt10desc, /*keep_versions=*/10ull, /*ascending=*/false);
BENCHMARK_CAPTURE(vectorPerfSortInsertDeleteOpt, vectorPerfSortInsertDeleteOpt100desc, /*keep_versions=*/100ull, /*ascending=*/false);
BENCHMARK_CAPTURE(vectorPerfSortInsertDeleteOpt, vectorPerfSortInsertDeleteOpt1000desc, /*keep_versions=*/1000ull, /*ascending=*/false);

BENCHMARK_CAPTURE(dequePerfSortInsertDelete, dequePerfSortInsertDelete10asc, /*keep_versions=*/10ull, /*ascending=*/true);
BENCHMARK_CAPTURE(dequePerfSortInsertDelete, dequePerfSortInsertDelete100asc, /*keep_versions=*/100ull, /*ascending=*/true);
BENCHMARK_CAPTURE(dequePerfSortInsertDelete, dequePerfSortInsertDelete1000asc, /*keep_versions=*/1000ull, /*ascending=*/true);

BENCHMARK_CAPTURE(dequePerfSortInsertDelete, dequePerfSortInsertDelete10desc, /*keep_versions=*/10ull, /*ascending=*/false);
BENCHMARK_CAPTURE(dequePerfSortInsertDelete, dequePerfSortInsertDelete100desc, /*keep_versions=*/100ull, /*ascending=*/false);
BENCHMARK_CAPTURE(dequePerfSortInsertDelete, dequePerfSortInsertDelete1000desc, /*keep_versions=*/1000ull, /*ascending=*/false);

BENCHMARK_CAPTURE(dequePerfSortInsertDeleteOpt, dequePerfSortInsertDeleteOpt10asc, /*keep_versions=*/10ull, /*ascending=*/true);
BENCHMARK_CAPTURE(dequePerfSortInsertDeleteOpt, dequePerfSortInsertDeleteOpt100asc, /*keep_versions=*/100ull, /*ascending=*/true);
BENCHMARK_CAPTURE(dequePerfSortInsertDeleteOpt, dequePerfSortInsertDeleteOpt1000asc, /*keep_versions=*/1000ull, /*ascending=*/true);

BENCHMARK_CAPTURE(dequePerfSortInsertDeleteOpt, dequePerfSortInsertDeleteOpt10desc, /*keep_versions=*/10ull, /*ascending=*/false);
BENCHMARK_CAPTURE(dequePerfSortInsertDeleteOpt, dequePerfSortInsertDeleteOpt100desc, /*keep_versions=*/100ull, /*ascending=*/false);
BENCHMARK_CAPTURE(dequePerfSortInsertDeleteOpt, dequePerfSortInsertDeleteOpt1000desc, /*keep_versions=*/1000ull, /*ascending=*/false);

BENCHMARK_CAPTURE(vectorLookup, vectorLookup10desc, /*keep_versions=*/10ull, /*ascending=*/true);
BENCHMARK_CAPTURE(vectorLookup, vectorLookup100desc, /*keep_versions=*/100ull, /*ascending=*/true);
BENCHMARK_CAPTURE(vectorLookup, vectorLookup1000desc, /*keep_versions=*/1000ull, /*ascending=*/true);
BENCHMARK_CAPTURE(vectorLookup, vectorLookup10000desc, /*keep_versions=*/10000ull, /*ascending=*/true);

BENCHMARK_CAPTURE(dequeLookup, dequeLookup10desc, /*keep_versions=*/10ull, /*ascending=*/false);
BENCHMARK_CAPTURE(dequeLookup, dequeLookup100desc, /*keep_versions=*/100ull, /*ascending=*/false);
BENCHMARK_CAPTURE(dequeLookup, dequeLookup10000desc, /*keep_versions=*/10000ull, /*ascending=*/false);

BENCHMARK_MAIN();
