#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/HybridHashTable/HybridHashTable.h>

#include <benchmark/benchmark.h>
#include <fmt/format.h>

namespace
{
class HybridTableFixture : public benchmark::Fixture
{
public:
    void SetUp(benchmark::State & state) override
    {
        size_t case_id = state.range(0);
        uint64_t max_hot_keys_count = state.range(1);
        bool clean_up_on_disk_data = state.range(2);

        DB::HybridHashTableConfig config;
        config.spill_dir_path = fmt::format("{}_{}", db_path, case_id);
        config.max_hot_key_count = max_hot_keys_count;
        config.cleanup_on_disk_data = clean_up_on_disk_data;
        config.value_object_size = sizeof(std::string);
        config.align_value_object_size = alignof(std::string);
        config.value_constructor = [](void * data) { new (data) std::string(); };
        config.value_destructor = [](void * data) {
            using string = std::string;
            reinterpret_cast<std::string *>(data)->~string();
        };
        config.value_serializer = [](const void * data, DB::WriteBuffer & wb) {
            const auto * s = reinterpret_cast<const std::string *>(data);
            DB::writeStringBinary(*s, wb);
            return DB::ErrorCodes::OK;
        };
        config.value_deserializer = [](void * data, DB::ReadBuffer & rb) {
            auto * s = reinterpret_cast<std::string *>(data);
            DB::readStringBinary(*s, rb);
            return DB::ErrorCodes::OK;
        };

        auto key_serializer = [](const std::string & k, DB::WriteBuffer & wb) {
            DB::writeStringBinary(k, wb);
            return DB::ErrorCodes::OK;
        };
        auto key_deserializer = [](std::string & k, DB::ReadBuffer & rb) {
            DB::readStringBinary(k, rb);
            return DB::ErrorCodes::OK;
        };

        hybrid_table = std::make_unique<DB::HybridHashTable<std::string>>(
            std::move(config), std::move(key_serializer), std::move(key_deserializer), getLogger("benchmark"));
    }

    void TearDown(benchmark::State & /*state*/) override { hybrid_table.reset(); }

protected:
    const std::string db_path = "./hybrid_table";
    std::unique_ptr<DB::HybridHashTable<std::string>> hybrid_table;
};

BENCHMARK_DEFINE_F(HybridTableFixture, Upsert)(benchmark::State & state)
{
    bool new_key = state.range(3);

    std::vector<char> data(256, 'v');
    std::string_view value{data.data(), data.size()};

    size_t inserted = 0;
    size_t i = 0;
    for (auto _ : state)
    {
        auto key = fmt::format("key+++++++++++++++++_{}", ++i);
        DB::HybridEmplaceResult result
            = new_key ? hybrid_table->emplaceNewKey(key) : hybrid_table->emplaceKey(key, /*disable_spill=*/false);
        if (result.isInserted())
        {
            auto * s = reinterpret_cast<std::string *>(result.find_result.getMutableMapped());
            *s = value;
        }
        else if (result.hasError())
        {
            std::cerr << result.errorString() << "\n";
        }

        inserted += result.inserted;
    }

    std::cout << fmt::format("Finished inserting i={} keys\n", i);

    if (inserted != i)
        std::cerr << fmt::format("Look like some inserts failed. inserted = {}, i={}\n", inserted, i);
}

BENCHMARK_DEFINE_F(HybridTableFixture, UpsertBatch)(benchmark::State & state)
{
    bool new_key = state.range(3);
    size_t batch_size = state.range(4);

    std::vector<char> data(256, 'v');
    std::string_view value{data.data(), data.size()};

    size_t inserted = 0;
    size_t i = 0;

    std::vector<std::string> keys;

    auto batch_insert = [&]() {
        DB::HybridEmplaceResults results
            = new_key ? hybrid_table->emplaceNewKeys(keys.begin(), keys.end(), /*disable_spill=*/false) : hybrid_table->emplaceKeys(keys);
        if (!results.hasError())
        {
            for (auto & result : results.results)
            {
                inserted += result.inserted;
                auto * s = reinterpret_cast<std::string *>(result.find_result.getMutableMapped());
                *s = value;
            }
        }
        else
        {
            std::cerr << results.errorString() << "\n";
        }

        keys.clear();
    };

    for (auto _ : state)
    {
        keys.emplace_back(fmt::format("key+++++++++++++++++_{}", ++i));
        if (keys.size() >= batch_size)
            batch_insert();
    }

    if (!keys.empty())
        batch_insert();

    std::cout << fmt::format("Finished inserting i={} keys\n", i);

    if (inserted != i)
        std::cerr << fmt::format("Look like some inserts failed. inserted = {}, i={}\n", inserted, i);
}

BENCHMARK_DEFINE_F(HybridTableFixture, ForKeyValueSequential)(benchmark::State & state)
{
    size_t found = 0;
    size_t i = 0;
    for (auto _ : state)
    {
        auto result = hybrid_table->findKey(fmt::format("key+++++++++++++++++_{}", ++i), /*disable_spill=*/false);
        found += result.isFound();
    }

    std::cout << fmt::format("Sequence found {} keys, iteration={}\n", found, i);
}

BENCHMARK_DEFINE_F(HybridTableFixture, ForKeyValueRandom)(benchmark::State & state)
{
    size_t key_count = state.range(3);

    std::random_device rd; // a seed source for the random number engine
    std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<size_t> distrib(1, key_count);

    size_t found = 0;
    size_t i = 0;
    for (auto _ : state)
    {
        auto result = hybrid_table->findKey(fmt::format("key+++++++++++++++++_{}", distrib(gen)), /*disable_spill=*/false);
        found += result.isFound();
        ++i;
    }

    std::cout << fmt::format("Random found {} keys, iteration={}\n", found, i);
}

BENCHMARK_DEFINE_F(HybridTableFixture, ForEachKeyValue)(benchmark::State & state)
{
    size_t found = 0;
    for (auto _ : state)
        hybrid_table->forEachKeyValue([&](const std::string & /*key*/, auto /*value*/) { ++found; });

    std::cout << fmt::format("For each key found {} keys\n", found);
}

BENCHMARK_DEFINE_F(HybridTableFixture, RemoveKeyRandom)(benchmark::State & state)
{
    size_t key_count = state.range(3);

    std::random_device rd; // a seed source for the random number engine
    std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<size_t> distrib(1, key_count);

    size_t failed = 0;
    size_t i = 0;
    for (auto _ : state)
    {
        auto err = hybrid_table->removeKey(fmt::format("key+++++++++++++++++_{}", distrib(gen)));
        failed += err;
        ++i;
    }

    std::cout << fmt::format("Random remove keys failed {}, iteration={}\n", failed, i);
}

BENCHMARK_DEFINE_F(HybridTableFixture, RemoveKeysRandom)(benchmark::State & state)
{
    size_t key_count = state.range(3);
    size_t batch_size = state.range(4);

    std::random_device rd; // a seed source for the random number engine
    std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<size_t> distrib(1, key_count);

    size_t failed = 0;
    size_t i = 0;
    std::vector<std::string> keys;
    keys.reserve(batch_size);
    for (auto _ : state)
    {
        keys.emplace_back(fmt::format("key+++++++++++++++++_{}", distrib(gen)));
        if (keys.size() >= batch_size)
        {
            auto err = hybrid_table->removeKeys(keys);
            failed += err;
            keys.clear();
        }
        ++i;
    }

    std::cout << fmt::format("Random remove keys failed {}, iteration={}\n", failed, i);
}
}

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ true,
        /*batch_size*/ 10,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ true,
        /*batch_size*/ 100,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ true,
        /*batch_size*/ 1000,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ true,
        /*batch_size*/ 10'000,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ true,
        /*batch_size*/ 100'000,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ false,
        /*batch_size*/ 10,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ false,
        /*batch_size*/ 100,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ false,
        /*batch_size*/ 1000,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ false,
        /*batch_size*/ 10'000,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ false,
        /*batch_size*/ 100'000,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, Upsert)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ true,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, Upsert)
    ->Args({
        /*case_id*/ 1,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ false,
        /*new_key*/ false,
    })
    ->Iterations(10'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, ForEachKeyValue)
    ->Args(
        {/*case_id*/ 1,
         /*max_hot_keys_count_*/ 100'000,
         /*clean_up_on_disk_data_*/ false})
    ->Iterations(1);

BENCHMARK_REGISTER_F(HybridTableFixture, ForKeyValueSequential)
    ->Args(
        {/*case_id*/ 1,
         /*max_hot_keys_count_*/ 100'000,
         /*clean_up_on_disk_data_*/ false,
         /*key_count*/ 10'000'000})
    ->Iterations(100'000);

BENCHMARK_REGISTER_F(HybridTableFixture, ForKeyValueRandom)
    ->Args(
        {/*case_id*/ 1,
         /*max_hot_keys_count_*/ 100'000,
         /*clean_up_on_disk_data_*/ true,
         /*key_count*/ 10'000'000})
    ->Iterations(100'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 2,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ true,
        /*batch_size*/ 100,
    })
    ->Iterations(300'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 2,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ true,
        /*batch_size*/ 1'000,
    })
    ->Iterations(300'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 2,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ false,
        /*batch_size*/ 100,
    })
    ->Iterations(300'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, UpsertBatch)
    ->Args({
        /*case_id*/ 2,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ false,
        /*batch_size*/ 1'000,
    })
    ->Iterations(300'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, Upsert)
    ->Args({
        /*case_id*/ 2,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ true,
        /*new_key*/ true,
        /*batch_size*/ 100,
    })
    ->Iterations(300'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, Upsert)
    ->Args({
        /*case_id*/ 2,
        /*max_hot_keys_count_*/ 100'000,
        /*clean_up_on_disk_data_*/ false,
        /*new_key*/ false,
    })
    ->Iterations(300'000'000);

BENCHMARK_REGISTER_F(HybridTableFixture, ForKeyValueSequential)
    ->Args(
        {/*case_id*/ 2,
         /*max_hot_keys_count_*/ 100'000,
         /*clean_up_on_disk_data_*/ false,
         /*key_count*/ 300'000'000})
    ->Iterations(100'000);

BENCHMARK_REGISTER_F(HybridTableFixture, ForKeyValueRandom)
    ->Args(
        {/*case_id*/ 2,
         /*max_hot_keys_count_*/ 100'000,
         /*clean_up_on_disk_data_*/ false,
         /*key_count*/ 300'000'000})
    ->Iterations(100'000);

BENCHMARK_REGISTER_F(HybridTableFixture, ForEachKeyValue)
    ->Args(
        {/*case_id*/ 2,
         /*max_hot_keys_count_*/ 100'000,
         /*clean_up_on_disk_data_*/ false})
    ->Iterations(1);

BENCHMARK_REGISTER_F(HybridTableFixture, RemoveKeysRandom)
    ->Args(
        {/*case_id*/ 2,
         /*max_hot_keys_count_*/ 100'000,
         /*clean_up_on_disk_data_*/ false,
         /*key_count*/ 100'000,
         /*batch_size*/ 1000})
    ->Iterations(100'000);

BENCHMARK_REGISTER_F(HybridTableFixture, RemoveKeyRandom)
    ->Args(
        {/*case_id*/ 2,
         /*max_hot_keys_count_*/ 100'000,
         /*clean_up_on_disk_data_*/ true,
         /*key_count*/ 300'000'000})
    ->Iterations(100'000);
