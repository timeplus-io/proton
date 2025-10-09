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
        bool use_hash_index = state.range(3);

        DB::HybridHashTableConfig config;
        config.spill_dir_path = fmt::format("{}_{}", db_path, case_id);
        config.max_hot_key_count = max_hot_keys_count;
        config.cleanup_on_disk_data = clean_up_on_disk_data;
        config.use_hash_index = use_hash_index;
        config.db_options = "max_background_jobs=4";
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

        if (!hybrid_table->persistentPartInited())
            upsertBatch(state);
    }

    void TearDown(benchmark::State & /*state*/) override { hybrid_table.reset(); }

    void upsertBatch(benchmark::State & state)
    {
        size_t number_keys = state.range(4);
        std::vector<char> data(256, 'v');
        std::string_view value{data.data(), data.size()};

        size_t inserted = 0;

        std::vector<std::string> keys;

        auto batch_insert = [&]() {
            DB::HybridEmplaceResults results = hybrid_table->emplaceNewKeys(keys.begin(), keys.end(), /*disable_spill=*/false);
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

        for (size_t i = 0; i < number_keys; ++i)
        {
            keys.emplace_back(fmt::format("key+++++++++++++++++_{}", i));
            if (keys.size() >= 10'000)
                batch_insert();
        }

        if (!keys.empty())
            batch_insert();

        std::cerr << fmt::format("Finished inserting i={} keys\n", number_keys);

        if (inserted != number_keys)
            std::cerr << fmt::format("Look like some inserts failed. inserted = {}, number_keys={}\n", inserted, number_keys);
    }

protected:
    const std::string db_path = "./hybrid_table";
    std::unique_ptr<DB::HybridHashTable<std::string>> hybrid_table;
};

BENCHMARK_DEFINE_F(HybridTableFixture, KeyLookupRandom)(benchmark::State & state)
{
    size_t key_count = state.range(4);

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

BENCHMARK_DEFINE_F(HybridTableFixture, KeyLookupRandomBatch)(benchmark::State & state)
{
    size_t key_count = state.range(4);
    size_t batch_size = state.range(5);

    std::random_device rd; // a seed source for the random number engine
    std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<size_t> distrib(1, key_count);

    std::vector<std::string> key_batch;
    key_batch.reserve(batch_size);

    size_t found = 0;
    size_t i = 0;
    for (auto _ : state)
    {
        for (size_t j = 0; j < batch_size; ++j)
        {
            key_batch.emplace_back(fmt::format("key+++++++++++++++++_{}", distrib(gen)));
            ++i;
        }

        auto results = hybrid_table->findKeys(key_batch.begin(), key_batch.end(), /*disable_spill=*/false);
        for (const auto & result : results.results)
            found += result.isFound();
    }

    std::cout << fmt::format("Random found {} keys, iteration={}\n", found, i);
}

}

BENCHMARK_REGISTER_F(HybridTableFixture, KeyLookupRandom)
    ->Args(
        {/*case_id*/ 1,
         /*max_hot_keys_count_*/ 0,
         /*clean_up_on_disk_data_*/ false,
         /*use_hash_index*/ true,
         /*key_count*/ 300'000'000})
    ->Iterations(100'000);

BENCHMARK_REGISTER_F(HybridTableFixture, KeyLookupRandom)
    ->Args(
        {/*case_id*/ 1,
         /*max_hot_keys_count_*/ 10'000,
         /*clean_up_on_disk_data_*/ false,
         /*use_hash_index*/ true,
         /*key_count*/ 300'000'000})
    ->Iterations(100'000);

BENCHMARK_REGISTER_F(HybridTableFixture, KeyLookupRandomBatch)
    ->Args(
        {/*case_id*/ 1,
         /*max_hot_keys_count_*/ 0,
         /*clean_up_on_disk_data_*/ false,
         /*use_hash_index*/ true,
         /*key_count*/ 300'000'000,
         /*batch_size*/ 1000})
    ->Iterations(100);


BENCHMARK_REGISTER_F(HybridTableFixture, KeyLookupRandomBatch)
    ->Args(
        {/*case_id*/ 1,
         /*max_hot_keys_count_*/ 10'000,
         /*clean_up_on_disk_data_*/ true,
         /*use_hash_index*/ true,
         /*key_count*/ 300'000'000,
         /*batch_size*/ 1000})
    ->Iterations(100);

BENCHMARK_REGISTER_F(HybridTableFixture, KeyLookupRandom)
    ->Args(
        {/*case_id*/ 2,
         /*max_hot_keys_count_*/ 0,
         /*clean_up_on_disk_data_*/ false,
         /*use_hash_index*/ false,
         /*key_count*/ 300'000'000})
    ->Iterations(100'000);

BENCHMARK_REGISTER_F(HybridTableFixture, KeyLookupRandom)
    ->Args(
        {/*case_id*/ 2,
         /*max_hot_keys_count_*/ 10'000,
         /*clean_up_on_disk_data_*/ false,
         /*use_hash_index*/ false,
         /*key_count*/ 300'000'000})
    ->Iterations(100'000);

BENCHMARK_REGISTER_F(HybridTableFixture, KeyLookupRandomBatch)
    ->Args(
        {/*case_id*/ 2,
         /*max_hot_keys_count_*/ 0,
         /*clean_up_on_disk_data_*/ false,
         /*use_hash_index*/ false,
         /*key_count*/ 300'000'000,
         /*batch_size*/ 1000})
    ->Iterations(100);

BENCHMARK_REGISTER_F(HybridTableFixture, KeyLookupRandomBatch)
    ->Args(
        {/*case_id*/ 2,
         /*max_hot_keys_count_*/ 10'000,
         /*clean_up_on_disk_data_*/ true,
         /*use_hash_index*/ false,
         /*key_count*/ 300'000'000,
         /*batch_size*/ 1000})
    ->Iterations(100);
