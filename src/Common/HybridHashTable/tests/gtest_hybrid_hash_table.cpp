
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/HybridHashTable/HybridHashTable.h>
#include <Common/Rocks/RocksDB.h>
#include <Common/Stopwatch.h>

#include <gtest/gtest.h>

#include <ranges>

/// We like to test 4 combinations of key-value types here
/// key_type, value_type
/// string, string
/// uint64_t, string
/// uint64_t, uint64_t
/// string, uint64_t

namespace
{

struct CalledCounts
{
    size_t k_ser_called = 0;
    size_t k_des_called = 0;
    size_t v_ctor_called = 0;
    size_t v_dtor_called = 0;
    size_t v_ser_called = 0;
    size_t v_des_called = 0;

    void assertEqual(const CalledCounts & other) const
    {
        ASSERT_EQ(k_ser_called, other.k_ser_called);
        ASSERT_EQ(k_des_called, other.k_des_called);
        ASSERT_EQ(v_ctor_called, other.v_ctor_called);
        ASSERT_EQ(v_dtor_called, other.v_dtor_called);
        ASSERT_EQ(v_ser_called, other.v_ser_called);
        ASSERT_EQ(v_des_called, other.v_des_called);
    }
};

template <typename K, typename V>
std::unique_ptr<DB::HybridHashTable<K>> createHashTable(CalledCounts & counts, size_t max_hot_key_count)
{
    auto value_ser = [&](const void * p, DB::WriteBuffer & wb) -> int {
        ++counts.v_ser_called;
        DB::writeBinary(*reinterpret_cast<const V *>(p), wb);
        return DB::ErrorCodes::OK;
    };

    auto value_des = [&](void * p, DB::ReadBuffer & rb) -> int {
        ++counts.v_des_called;
        DB::readBinary(*reinterpret_cast<V *>(p), rb);
        return DB::ErrorCodes::OK;
    };

    std::filesystem::remove_all("/tmp/hybrid-ht");

    DB::HybridHashTableConfig config{
        .base_conf = {
            .spill_dir_path = "/tmp/hybrid-ht",
            .kv_options = "",
            .max_hot_key_count = max_hot_key_count,
            .cf_handle_id = "",
            .rocks_cf_handler_getter = {},
        },
        .value_object_size = sizeof(V),
        .align_value_object_size = alignof(V),
        .value_constructor =
            [&](void * p) {
                ++counts.v_ctor_called;
                new (p) V;
            },
        .value_destructor =
            [&](void * p) {
                ++counts.v_dtor_called;
                reinterpret_cast<V *>(p)->~V();
            },
        .value_serializer = std::move(value_ser),
        .value_deserializer = std::move(value_des),
    };


    config.validate();

    auto key_serializer = [&](const K & key, DB::WriteBuffer & wb) -> int {
        ++counts.k_ser_called;
        DB::writeBinary(key, wb);
        return DB::ErrorCodes::OK;
    };

    auto key_deserializer = [&](K & key, DB::ReadBuffer & rb) -> int {
        ++counts.k_des_called;
        DB::readBinary(key, rb);
        return DB::ErrorCodes::OK;
    };

    return std::make_unique<DB::HybridHashTable<K>>(
        std::move(config), std::move(key_serializer), std::move(key_deserializer), getLogger("hybrid_unit"));
}

struct ExpectedResult
{
    std::unordered_map<std::string, std::string> expected_hot_key_values;
    std::list<std::string> expected_recent_keys;
    CalledCounts expected_counts;

    template <typename K>
    void validate(
        const DB::HybridHashTable<K>::InmemoryHashMap & hot_key_values_got,
        const std::list<K> & recent_keys_got,
        const CalledCounts & counts_got) const
    {
        ASSERT_EQ(hot_key_values_got.size(), recent_keys_got.size());

        std::unordered_map<std::string, std::string> key_values_got;
        for (const auto & [_, entry] : hot_key_values_got)
            key_values_got.emplace(*entry.recent_keys_position, *reinterpret_cast<const std::string *>(entry.getMapped()));

        ASSERT_EQ(key_values_got, expected_hot_key_values);
        ASSERT_EQ(recent_keys_got.size(), expected_recent_keys.size());

        for (std::tuple<const std::string &, const std::string &> elem : std::views::zip(recent_keys_got, expected_recent_keys))
            ASSERT_EQ(std::get<0>(elem), std::get<1>(elem));

        counts_got.assertEqual(expected_counts);
    }
};

enum class Action : uint8_t
{
    EmplaceKey,
    EmplaceNewKey,
    EmplaceKeys,
    EmplaceNewKeys,
    FindKey,
    FindKeys,
    RemoveKey,
    RemoveKeys,
};

struct ExecuteAction
{
    Action action;

    std::vector<std::string> keys;
    std::vector<uint8_t> new_keys;

    ExpectedResult expected_result;
};

void executeActionsAndValidateResults(const std::vector<ExecuteAction> & execute_actions, size_t max_hot_key_count)
{
    CalledCounts counts;
    {
        auto table = createHashTable<std::string, std::string>(counts, max_hot_key_count);

        for (const auto & execute_action : execute_actions)
        {
            switch (execute_action.action)
            {
                case Action::EmplaceKey:
                    [[fallthrough]];
                case Action::EmplaceNewKey:
                {
                    const auto & k = execute_action.keys[0];
                    auto is_new_key = execute_action.new_keys[0];

                    auto result = execute_action.action == Action::EmplaceKey ? table->emplaceKey(k, /*disable_spill=*/false)
                                                                              : table->emplaceNewKey(k, /*disable_spill=*/false);
                    ASSERT_TRUE(!result.hasError());

                    if (is_new_key)
                        ASSERT_TRUE(result.isInserted());
                    else
                        ASSERT_FALSE(result.isInserted());

                    auto * mapped = result.getMutableMapped();
                    ASSERT_TRUE(mapped != nullptr);
                    auto & v = *reinterpret_cast<std::string *>(mapped);

                    if (is_new_key)
                    {
                        ASSERT_TRUE(v.empty());
                        v = k;
                    }

                    const auto & hot_key_values = table->hotKeyValues();
                    auto iter = hot_key_values.find(k);
                    ASSERT_TRUE(iter != hot_key_values.end());
                    ASSERT_EQ(*reinterpret_cast<const std::string *>(iter->second.getMapped()), k);
                    ASSERT_EQ(*iter->second.recent_keys_position, k);

                    execute_action.expected_result.validate(hot_key_values, table->recentKeys(), counts);

                    break;
                }
                case Action::EmplaceKeys:
                    [[fallthrough]];
                case Action::EmplaceNewKeys:
                {
                    auto results = execute_action.action == Action::EmplaceKeys ? table->emplaceKeys(execute_action.keys)
                                                                                : table->emplaceNewKeys(execute_action.keys);
                    ASSERT_FALSE(results.hasError());
                    ASSERT_EQ(results.results.size(), execute_action.keys.size());

                    for (auto elem : std::views::zip(results.results, execute_action.keys, execute_action.new_keys))
                    {
                        auto & result = std::get<0>(elem);
                        const auto & key = std::get<1>(elem);
                        auto is_new_key = std::get<2>(elem);
                        if (is_new_key)
                        {
                            ASSERT_TRUE(result.isInserted());
                            *reinterpret_cast<std::string *>(result.getMutableMapped()) = key;
                        }
                        else
                        {
                            ASSERT_FALSE(result.isInserted());
                            /// Get mutable mapped here indicating that the key has been changed
                            ASSERT_EQ(*reinterpret_cast<const std::string *>(result.getMutableMapped()), key);
                        }
                    }

                    execute_action.expected_result.validate(table->hotKeyValues(), table->recentKeys(), counts);

                    break;
                }
                case Action::FindKey:
                {
                    const auto & k = execute_action.keys[0];
                    auto is_new_key = execute_action.new_keys[0];
                    auto result = table->findKey(k, /*disable_spill=*/false);

                    ASSERT_FALSE(result.hasError());
                    if (is_new_key)
                    {
                        ASSERT_FALSE(result.isFound());
                    }
                    else
                    {
                        ASSERT_TRUE(result.isFound());
                        ASSERT_EQ(*reinterpret_cast<const std::string *>(result.getMapped()), k);
                    }

                    execute_action.expected_result.validate(table->hotKeyValues(), table->recentKeys(), counts);

                    break;
                }
                case Action::FindKeys:
                {
                    auto results = table->findKeys(execute_action.keys);
                    ASSERT_FALSE(results.hasError());

                    ASSERT_EQ(results.results.size(), execute_action.keys.size());
                    for (auto elem : std::views::zip(results.results, execute_action.keys, execute_action.new_keys))
                    {
                        const auto & result = std::get<0>(elem);
                        const auto & key = std::get<1>(elem);
                        auto is_new_key = std::get<2>(elem);

                        if (is_new_key)
                        {
                            ASSERT_FALSE(result.isFound());
                        }
                        else
                        {
                            ASSERT_TRUE(result.isFound());
                            ASSERT_EQ(*reinterpret_cast<const std::string *>(result.getMapped()), key);
                        }
                    }

                    execute_action.expected_result.validate(table->hotKeyValues(), table->recentKeys(), counts);

                    break;
                }
                case Action::RemoveKeys:
                {
                    auto result = table->removeKeys(execute_action.keys);
                    ASSERT_TRUE(result == DB::ErrorCodes::OK);
                    execute_action.expected_result.validate(table->hotKeyValues(), table->recentKeys(), counts);
                    break;
                }
                case Action::RemoveKey:
                {
                    auto result = table->removeKey(execute_action.keys[0]);
                    ASSERT_TRUE(result == DB::ErrorCodes::OK);
                    break;
                }
            }
        }
    }

    ASSERT_EQ(counts.v_ctor_called, counts.v_dtor_called);
}

}

TEST(HybridHashTable, EmplaceKey_StrStr)
{
    /// A sequence of action and a sequence of results for the actions
    std::vector<ExecuteAction> execute_actions = {
        {.action = Action::EmplaceKey,
         .keys = {"a"}, /// New key
         .new_keys = {1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}},
            .expected_recent_keys = {"a"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 1, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceKey,
         .keys = {"b"}, /// New key
         .new_keys = {1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}},
            .expected_recent_keys = {"a", "b"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 2, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceKey,
         .keys = {"c"}, /// New key
         .new_keys = {1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"a", "b", "c"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceKey,
         .keys = {"a"}, /// Existing key, cause changes of the order of recent keys
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"b", "c", "a"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceKey,
         .keys = {"c"}, /// Existing key, cause changes of the order of recent keys
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"b", "a", "c"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceKey,
         .keys = {"d"}, /// New key, cause 1 key spill to disk
         .new_keys = {1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"d", "d"}, {"c", "c"}},
            .expected_recent_keys = {"a", "c", "d"},
            .expected_counts
            = CalledCounts{.k_ser_called = 1, .k_des_called = 0, .v_ctor_called = 4, .v_dtor_called = 1, .v_ser_called = 1, .v_des_called = 0}}},
        {.action = Action::EmplaceKey,
         .keys = {"b"}, /// Load 1 cold key, which caused key "a" to spill
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"d", "d"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"c", "d", "b"},
            .expected_counts
            = CalledCounts{.k_ser_called = 3, .k_des_called = 0, .v_ctor_called = 5, .v_dtor_called = 2, .v_ser_called = 2, .v_des_called = 1}}},
        {.action = Action::EmplaceKey,
         .keys = {"d"}, /// Existing hot key, cause changes of the order of recent keys
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"d", "d"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"c", "b", "d"},
            .expected_counts
            = CalledCounts{.k_ser_called = 3, .k_des_called = 0, .v_ctor_called = 5, .v_dtor_called = 2, .v_ser_called = 2, .v_des_called = 1}}},
        {.action = Action::FindKey,
         .keys = {"c"}, /// Existing hot key, cause changes of the order of recent keys
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"d", "d"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"b", "d", "c"},
            .expected_counts
            = CalledCounts{.k_ser_called = 3, .k_des_called = 0, .v_ctor_called = 5, .v_dtor_called = 2, .v_ser_called = 2, .v_des_called = 1}}},
        {.action = Action::FindKey,
         .keys = {"a"}, /// Load 1 cold key, cause changes of the order of recent keys and cause key "b" to spill
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"d", "d"}, {"c", "c"}},
            .expected_recent_keys = {"d", "c", "a"},
            .expected_counts
            = CalledCounts{.k_ser_called = 5, .k_des_called = 0, .v_ctor_called = 6, .v_dtor_called = 3, .v_ser_called = 3, .v_des_called = 2}}},
        {.action = Action::FindKey,
         .keys = {"x"}, /// Not exist
         .new_keys = {1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"d", "d"}, {"c", "c"}},
            .expected_recent_keys = {"d", "c", "a"},
            .expected_counts
            = CalledCounts{.k_ser_called = 5 + 1, .k_des_called = 0, .v_ctor_called = 6, .v_dtor_called = 3, .v_ser_called = 3, .v_des_called = 2}}},
    };

    executeActionsAndValidateResults(execute_actions, /*max_hot_key_count=*/3);
}

TEST(HybridHashTable, EmplaceNewKey_StrStr)
{
    /// A sequence of action and a sequence of results for the actions
    std::vector<ExecuteAction> execute_actions = {
        {.action = Action::EmplaceNewKey,
         .keys = {"a"}, /// New key
         .new_keys = {1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}},
            .expected_recent_keys = {"a"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 1, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceNewKey,
         .keys = {"b"}, /// New key
         .new_keys = {1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}},
            .expected_recent_keys = {"a", "b"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 2, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceNewKey,
         .keys = {"c"}, /// New key
         .new_keys = {1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"a", "b", "c"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceKey,
         .keys = {"a"}, /// Existing key, cause changes of the order of recent keys
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"b", "c", "a"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceKey,
         .keys = {"c"}, /// Existing key, cause changes of the order of recent keys
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"b", "a", "c"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceNewKey,
         .keys = {"d"}, /// New key, cause spill to disk
         .new_keys = {1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"d", "d"}, {"c", "c"}},
            .expected_recent_keys = {"a", "c", "d"},
            .expected_counts
            = CalledCounts{.k_ser_called = 1, .k_des_called = 0, .v_ctor_called = 4, .v_dtor_called = 1, .v_ser_called = 1, .v_des_called = 0}}},
        {.action = Action::EmplaceKey,
         .keys = {"b"}, /// Bring the spill key back, which caused key "a" to spill
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"d", "d"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"c", "d", "b"},
            .expected_counts
            = CalledCounts{.k_ser_called = 3, .k_des_called = 0, .v_ctor_called = 5, .v_dtor_called = 2, .v_ser_called = 2, .v_des_called = 1}}},
        {.action = Action::EmplaceKey,
         .keys = {"d"}, /// Existing hot key, cause changes of the order of recent keys
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"d", "d"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"c", "b", "d"},
            .expected_counts
            = CalledCounts{.k_ser_called = 3, .k_des_called = 0, .v_ctor_called = 5, .v_dtor_called = 2, .v_ser_called = 2, .v_des_called = 1}}},
        {.action = Action::FindKey,
         .keys = {"c"}, /// Existing hot key, cause changes of the order of recent keys
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"d", "d"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"b", "d", "c"},
            .expected_counts
            = CalledCounts{.k_ser_called = 3, .k_des_called = 0, .v_ctor_called = 5, .v_dtor_called = 2, .v_ser_called = 2, .v_des_called = 1}}},
        {.action = Action::FindKey,
         .keys = {"a"}, /// Existing cold key, cause changes of the order of recent keys and cause key "b" to spill
         .new_keys = {0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"d", "d"}, {"c", "c"}},
            .expected_recent_keys = {"d", "c", "a"},
            .expected_counts
            = CalledCounts{.k_ser_called = 5, .k_des_called = 0, .v_ctor_called = 6, .v_dtor_called = 3, .v_ser_called = 3, .v_des_called = 2}}},
    };

    executeActionsAndValidateResults(execute_actions, /*max_hot_key_count=*/3);
}

TEST(HybridHashTable, EmplaceNewKeys_StrStr)
{
    /// A sequence of action and a sequence of results for the actions
    std::
        vector<ExecuteAction>
            execute_actions = {
                {.action = Action::EmplaceNewKeys,
                 .keys = {"a", "b", "c"}, /// New key
                 .new_keys = {1, 1, 1},
                 .expected_result
                 = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
                    .expected_recent_keys = {"a", "b", "c"},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
                {.action = Action::EmplaceKeys,
                 .keys = {"a", "b"}, /// Existing keys, cause changes of the order of recent keys
                 .new_keys = {0, 0},
                 .expected_result
                 = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
                    .expected_recent_keys = {"c", "a", "b"},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
                {.action = Action::EmplaceNewKeys,
                 .keys = {"d", "e"}, /// New keys, cause spill 2 keys to disk
                 .new_keys = {1, 1},
                 .expected_result
                 = {.expected_hot_key_values = {{"d", "d"}, {"e", "e"}, {"b", "b"}},
                    .expected_recent_keys = {"b", "d", "e"},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 2, .k_des_called = 0, .v_ctor_called = 5, .v_dtor_called = 2, .v_ser_called = 2, .v_des_called = 0}}},
                {.action = Action::EmplaceKeys,
                 .keys = {"a", "c"}, /// Existing cold keys, cause changes of the order of recent keys and spill another 2 keys
                 .new_keys = {0, 0},
                 .expected_result
                 = {.expected_hot_key_values = {{"a", "a"}, {"c", "c"}, {"e", "e"}},
                    .expected_recent_keys = {"e", "a", "c"},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 6, .k_des_called = 0, .v_ctor_called = 7, .v_dtor_called = 4, .v_ser_called = 4, .v_des_called = 2}}},
                {.action = Action::EmplaceKeys,
                 .keys = {"d", "e"}, /// Existing hot and cold keys, cause changes of the order of recent keys and spill another 1 keys
                 .new_keys = {0, 0},
                 .expected_result
                 = {.expected_hot_key_values = {{"d", "d"}, {"c", "c"}, {"e", "e"}},
                    .expected_recent_keys = {"c", "e", "d"}, /// e is accessed first via hot keys
                    .expected_counts
                    = CalledCounts{.k_ser_called = 8, .k_des_called = 0, .v_ctor_called = 8, .v_dtor_called = 5, .v_ser_called = 5, .v_des_called = 3}}},
                {.action = Action::EmplaceNewKeys,
                 .keys = {"f", "g", "h", "i"}, /// New keys exceeding max hot keys size, causing all prev 3 hot keys spill to disk
                 .new_keys = {1, 1, 1, 1},
                 .expected_result
                 = {.expected_hot_key_values = {{"f", "f"}, {"g", "g"}, {"h", "h"}, {"i", "i"}},
                    .expected_recent_keys = {"f", "g", "h", "i"},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 11, .k_des_called = 0, .v_ctor_called = 12, .v_dtor_called = 8, .v_ser_called = 8, .v_des_called = 3}}},
                {.action = Action::EmplaceKeys,
                 .keys = {"j", "a"}, /// Mixed new and cold keys, load 1 old, insert 1 new and spill 3
                 .new_keys = {1, 0},
                 .expected_result
                 = {.expected_hot_key_values = {{"a", "a"}, {"j", "j"}, {"i", "i"}},
                    .expected_recent_keys = {"i", "a", "j"},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 11 + /*spill 3 + load 1 old + insert 1 new*/ 3 + 1 + 1, .k_des_called = 0, .v_ctor_called = 12 + /*load 1 old + insert 1 new*/ 2, .v_dtor_called = 8 + 3, .v_ser_called = 8 + 3, .v_des_called = 3 + 1}}},
                {.action = Action::EmplaceKeys,
                 .keys = {"k", "a"}, /// Mixed new and hot keys, insert 1 new and spill 1
                 .new_keys = {1, 0},
                 .expected_result
                 = {.expected_hot_key_values = {{"a", "a"}, {"j", "j"}, {"k", "k"}},
                    .expected_recent_keys = {"j", "a", "k"},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 16 + /*spill 1 + insert 1*/ 1 + 1, .k_des_called = 0, .v_ctor_called = 14 + /*insert 1 new*/ 1, .v_dtor_called = 11 + 1, .v_ser_called = 11 + 1, .v_des_called = 4}}},
                {.action = Action::EmplaceKeys,
                 .keys = {"m", "a", "b", "c"}, /// Mixed new, hot and cold keys, load 2, insert 1 new and spill 2
                 .new_keys = {1, 0, 0, 0},
                 .expected_result
                 = {.expected_hot_key_values = {{"a", "a"}, {"m", "m"}, {"b", "b"}, {"c", "c"}}, .expected_recent_keys = {"a", "b", "c", "m"}, .expected_counts = CalledCounts{.k_ser_called = 18 + /*spill 2 + load 2 old + insert 1 new*/ 2 + 2 + 1, .k_des_called = 0, .v_ctor_called = 15 + /*load 2 old + insert 1 new*/ 3, .v_dtor_called = 12 + 2, .v_ser_called = 12 + 2, .v_des_called = 4 + 2}}},
                {.action = Action::EmplaceKeys,
                 .keys = {"n", "a", "n", "p"}, /// Duplicate new keys, insert 2, spill 2
                 .new_keys = {1, 0, /*n occurs already, so old key*/ 0, 1},
                 .expected_result = {.expected_hot_key_values = {{"n", "n"}, {"m", "m"}, {"p", "p"}, {"a", "a"}}, .expected_recent_keys = {"m", "a", "n", "p"}, .expected_counts = CalledCounts{.k_ser_called = 23 + /*insert 2 new + spill 2*/ 3 + 2, .k_des_called = 0, .v_ctor_called = 18 + /*insert 2 new*/ 2, .v_dtor_called = 14 + 2, .v_ser_called = 14 + 2, .v_des_called = 6}}},
                {.action = Action::FindKeys,
                 .keys = {"a", "b", "c", "d", "e"}, /// Find all existing key, 1 hot, 4 cold, spill 3
                 .new_keys = {0, 0, 0, 0, 0},
                 .expected_result
                 = {.expected_hot_key_values = {{"b", "b"}, {"c", "c"}, {"d", "d"}, {"e", "e"}, {"a", "a"}},
                    .expected_recent_keys = {"a", "b", "c", "d", "e"},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 28 + /*load 4 new + spill 3*/ 4 + 3, .k_des_called = 0, .v_ctor_called = 20 + /*load 4 cold*/ 4, .v_dtor_called = 16 + 3, .v_ser_called = 16 + 3, .v_des_called = 6 + 4}}},
                {.action = Action::RemoveKeys,
                 .keys = {"a", "b", "c", "d", "e"},
                 .new_keys = {0, 0, 0, 0, 0},
                 .expected_result
                 = {.expected_hot_key_values = {},
                    .expected_recent_keys = {},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 35 + 5, .k_des_called = 0, .v_ctor_called = 24, .v_dtor_called = 19 + 5, .v_ser_called = 19, .v_des_called = 10}}},
                {.action = Action::FindKeys,
                 .keys = {"a", "b", "c", "d", "e"}, /// Find all non-existing keys
                 .new_keys = {1, 1, 1, 1, 1},
                 .expected_result
                 = {.expected_hot_key_values = {},
                    .expected_recent_keys = {},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 40 + 5, .k_des_called = 0, .v_ctor_called = 24, .v_dtor_called = 24, .v_ser_called = 19, .v_des_called = 10}}},
                {.action = Action::FindKeys,
                 .keys = {"f", "g", "h", "i", "j"}, /// Find all existing keys, load 5 cold keys
                 .new_keys = {0, 0, 0, 0, 0},
                 .expected_result
                 = {.expected_hot_key_values = {{"f", "f"}, {"g", "g"}, {"h", "h"}, {"i", "i"}, {"j", "j"}},
                    .expected_recent_keys = {"f", "g", "h", "i", "j"},
                    .expected_counts
                    = CalledCounts{.k_ser_called = 45 + 5, .k_des_called = 0, .v_ctor_called = 24 + 5, .v_dtor_called = 24, .v_ser_called = 19, .v_des_called = 10 + 5}}},
            };

    executeActionsAndValidateResults(execute_actions, /*max_hot_key_count=*/3);
}

TEST(HybridHashTable, EmplaceKeys_StrStr)
{
    /// A sequence of action and a sequence of results for the actions
    std::vector<ExecuteAction> execute_actions = {
        {.action = Action::EmplaceKeys,
         .keys = {"a", "b", "c"}, /// New key
         .new_keys = {1, 1, 1},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"a", "b", "c"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceKeys,
         .keys = {"a", "b"}, /// Existing keys, cause changes of the order of recent keys
         .new_keys = {0, 0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"c", "a", "b"},
            .expected_counts
            = CalledCounts{.k_ser_called = 0, .k_des_called = 0, .v_ctor_called = 3, .v_dtor_called = 0, .v_ser_called = 0, .v_des_called = 0}}},
        {.action = Action::EmplaceKeys,
         .keys = {"d", "e"}, /// New keys, cause spill 2 keys to disk
         .new_keys = {1, 1},
         .expected_result
         = {.expected_hot_key_values = {{"d", "d"}, {"e", "e"}, {"b", "b"}},
            .expected_recent_keys = {"b", "d", "e"},
            .expected_counts
            = CalledCounts{.k_ser_called = 2, .k_des_called = 0, .v_ctor_called = 5, .v_dtor_called = 2, .v_ser_called = 2, .v_des_called = 0}}},
        {.action = Action::EmplaceKeys,
         .keys = {"a", "c"}, /// Load 2 cold keys, cause changes of the order of recent keys and spill another 2 keys
         .new_keys = {0, 0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"c", "c"}, {"e", "e"}},
            .expected_recent_keys = {"e", "a", "c"},
            .expected_counts
            = CalledCounts{.k_ser_called = 2 + 2 + 2, .k_des_called = 0, .v_ctor_called = 7, .v_dtor_called = 4, .v_ser_called = 4, .v_des_called = 2}}},
        {.action = Action::EmplaceKeys,
         .keys = {"d", "e"}, /// Existing 1 hot and 1 cold keys, cause changes of the order of recent keys and spill another 1 keys
         .new_keys = {0, 0},
         .expected_result
         = {.expected_hot_key_values = {{"d", "d"}, {"c", "c"}, {"e", "e"}},
            .expected_recent_keys = {"c", "e", "d"}, /// e is accessed first via hot keys
            .expected_counts
            = CalledCounts{.k_ser_called = 6 + 1 + 1, .k_des_called = 0, .v_ctor_called = 8, .v_dtor_called = 5, .v_ser_called = 5, .v_des_called = 3}}},
        {.action = Action::EmplaceKeys,
         .keys = {"f", "g", "h", "i"}, /// New keys exceeding max hot keys size, causing all prev 3 hot keys spill to disk
         .new_keys = {1, 1, 1, 1},
         .expected_result
         = {.expected_hot_key_values = {{"f", "f"}, {"g", "g"}, {"h", "h"}, {"i", "i"}},
            .expected_recent_keys = {"f", "g", "h", "i"},
            .expected_counts
            = CalledCounts{.k_ser_called = 8 + 4 + 3, .k_des_called = 0, .v_ctor_called = 12, .v_dtor_called = 8, .v_ser_called = 8, .v_des_called = 3}}},
        {.action = Action::EmplaceKeys,
         .keys = {"j", "a"}, /// Mixed new and cold keys, load 1 old, insert 1 new and spill 3
         .new_keys = {1, 0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"j", "j"}, {"i", "i"}},
            .expected_recent_keys = {"i", "a", "j"},
            .expected_counts
            = CalledCounts{.k_ser_called = 15 + /*spill 3 + load 1 old + insert 1 new*/ 3 + 1 + 1, .k_des_called = 0, .v_ctor_called = 12 + /*load 1 old + insert 1 new*/ 2, .v_dtor_called = 8 + 3, .v_ser_called = 8 + 3, .v_des_called = 3 + 1}}},
        {.action = Action::EmplaceKeys,
         .keys = {"k", "a"}, /// Mixed new and hot keys, insert 1 new and spill 1
         .new_keys = {1, 0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"j", "j"}, {"k", "k"}},
            .expected_recent_keys = {"j", "a", "k"},
            .expected_counts
            = CalledCounts{.k_ser_called = 20 + /*spill 1 + insert 1*/ 1 + 1, .k_des_called = 0, .v_ctor_called = 14 + /*insert 1 new*/ 1, .v_dtor_called = 11 + 1, .v_ser_called = 11 + 1, .v_des_called = 4}}},
        {.action = Action::EmplaceKeys,
         .keys = {"m", "a", "b", "c"}, /// Mixed new, hot and cold keys, load 2, insert 1 new and spill 2
         .new_keys = {1, 0, 0, 0},
         .expected_result
         = {.expected_hot_key_values = {{"a", "a"}, {"m", "m"}, {"b", "b"}, {"c", "c"}},
            .expected_recent_keys = {"a", "b", "c", "m"},
            .expected_counts
            = CalledCounts{.k_ser_called = 22 + /*spill 2 + load 2 old + insert 1 new*/ 2 + 2 + 1, .k_des_called = 0, .v_ctor_called = 15 + /*load 2 old + insert 1 new*/ 3, .v_dtor_called = 12 + 2, .v_ser_called = 12 + 2, .v_des_called = 4 + 2}}},
        {.action = Action::EmplaceKeys,
         .keys = {"n", "a", "n", "p"}, /// Duplicate new keys, insert 2, spill 2
         .new_keys = {1, 0, /*n occurs already, so old key*/ 0, 1},
         .expected_result
         = {.expected_hot_key_values = {{"n", "n"}, {"m", "m"}, {"p", "p"}, {"a", "a"}},
            .expected_recent_keys = {"m", "a", "n", "p"},
            .expected_counts
            = CalledCounts{.k_ser_called = 27 + /*insert 2 new + spill 2*/ 3 + 2, .k_des_called = 0, .v_ctor_called = 18 + /*insert 2 new*/ 2, .v_dtor_called = 14 + 2, .v_ser_called = 14 + 2, .v_des_called = 6}}},
        {.action = Action::FindKeys,
         .keys = {"a", "b", "c", "d", "e"}, /// Find all existing key, 1 hot, 4 cold, spill 3
         .new_keys = {0, 0, 0, 0, 0},
         .expected_result
         = {.expected_hot_key_values = {{"b", "b"}, {"c", "c"}, {"d", "d"}, {"e", "e"}, {"a", "a"}},
            .expected_recent_keys = {"a", "b", "c", "d", "e"},
            .expected_counts
            = CalledCounts{.k_ser_called = 32 + /*load 4 new + spill 3*/ 4 + 3, .k_des_called = 0, .v_ctor_called = 20 + /*load 4 cold*/ 4, .v_dtor_called = 16 + 3, .v_ser_called = 16 + 3, .v_des_called = 6 + 4}}},
        {.action = Action::RemoveKeys,
         .keys = {"a", "b", "c", "d", "e"},
         .new_keys = {0, 0, 0, 0, 0},
         .expected_result
         = {.expected_hot_key_values = {},
            .expected_recent_keys = {},
            .expected_counts
            = CalledCounts{.k_ser_called = 39 + 5, .k_des_called = 0, .v_ctor_called = 24, .v_dtor_called = 19 + 5, .v_ser_called = 19, .v_des_called = 10}}},
        {.action = Action::FindKeys,
         .keys = {"a", "b", "c", "d", "e"}, /// Find all non-existing keys
         .new_keys = {1, 1, 1, 1, 1},
         .expected_result
         = {.expected_hot_key_values = {},
            .expected_recent_keys = {},
            .expected_counts
            = CalledCounts{.k_ser_called = 44 + 5, .k_des_called = 0, .v_ctor_called = 24, .v_dtor_called = 24, .v_ser_called = 19, .v_des_called = 10}}},
        {.action = Action::FindKeys,
         .keys = {"f", "g", "h", "i", "j"}, /// Find all existing keys, load 5 cold keys
         .new_keys = {0, 0, 0, 0, 0},
         .expected_result
         = {.expected_hot_key_values = {{"f", "f"}, {"g", "g"}, {"h", "h"}, {"i", "i"}, {"j", "j"}},
            .expected_recent_keys = {"f", "g", "h", "i", "j"},
            .expected_counts
            = CalledCounts{.k_ser_called = 49 + 5, .k_des_called = 0, .v_ctor_called = 24 + 5, .v_dtor_called = 24, .v_ser_called = 19, .v_des_called = 10 + 5}}},
    };

    executeActionsAndValidateResults(execute_actions, /*max_hot_key_count=*/3);
}

TEST(HybridHashTable, RandomStress)
{
    CalledCounts counts;
    {
        auto table = createHashTable<std::string, std::string>(counts, 10000);

        size_t max_key = 1'000'000;

        std::random_device rd; // a seed source for the random number engine
        std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
        std::uniform_int_distribution<size_t> distrib(1, max_key);
        std::uniform_int_distribution<size_t> batch_distrib(1, 10000);

        Stopwatch stopwatch;
        for (; stopwatch.elapsedSeconds() < 10;)
        {
            auto rand_keys = [&]() {
                auto batch_size = batch_distrib(gen);
                std::vector<std::string> keys;
                keys.reserve((batch_size));
                for (size_t k = 0; k < batch_size; ++k)
                    keys.push_back(fmt::format("key_{}", distrib(gen)));

                return keys;
            };

            /// emplaceKeys
            {
                auto keys = rand_keys();
                auto results = table->emplaceKeys(keys);
                ASSERT_FALSE(results.hasError());
                ASSERT_EQ(results.results.size(), keys.size());

                for (size_t j = 0; auto & result : results.results)
                {
                    auto * mapped = result.getMutableMapped();
                    ASSERT_TRUE(mapped != nullptr);
                    auto & v = *reinterpret_cast<std::string *>(mapped);

                    if (result.isInserted())
                    {
                        ASSERT_TRUE(v.empty());
                        v = keys[j];
                    }
                    else
                    {
                        ASSERT_EQ(v, keys[j]);
                    }

                    ++j;
                }
            }

            /// findKeys
            {
                auto keys = rand_keys();
                auto results = table->findKeys(keys);
                ASSERT_FALSE(results.hasError());
                for (size_t j = 0; const auto & result : results.results)
                {
                    if (result.isFound())
                    {
                        const auto * mapped = result.getMapped();
                        ASSERT_TRUE(mapped != nullptr);
                        const auto & v = *reinterpret_cast<const std::string *>(mapped);
                        ASSERT_EQ(v, keys[j]);
                    }

                    ++j;
                }
            }

            /// removeKeys
            {
                auto keys = rand_keys();
                auto errcode = table->removeKeys(keys);
                ASSERT_EQ(errcode, DB::ErrorCodes::OK);
            }

            /// findKey
            {
                auto keys = rand_keys();
                auto key_count = std::min<size_t>(keys.size(), 37);
                for (size_t i = 0; i < key_count; ++i)
                {
                    auto result = table->findKey(keys[i], /*disable_spill=*/false);
                    ASSERT_FALSE(result.hasError());
                    if (result.isFound())
                    {
                        const auto * mapped = result.getMapped();
                        ASSERT_TRUE(mapped != nullptr);
                        const auto & v = *reinterpret_cast<const std::string *>(mapped);
                        ASSERT_EQ(v, keys[i]);
                    }
                }
            }

            /// removeKey
            {
                auto keys = rand_keys();
                auto key_count = std::min<size_t>(keys.size(), 37);
                for (size_t i = 0; i < key_count; ++i)
                {
                    auto errcode = table->removeKey(keys[i]);
                    ASSERT_EQ(errcode, DB::ErrorCodes::OK);
                }
            }
        }
    }

    ASSERT_EQ(counts.v_ctor_called, counts.v_dtor_called);
}

TEST(HybridHashTable, EmplaceDupNewKeys)
{
    CalledCounts counts;
    auto table = createHashTable<std::string, std::string>(counts, 10000);
    std::vector<std::string> keys = {"k1", "k2", "k1"};
    auto results = table->emplaceKeys(keys);
    ASSERT_FALSE(results.hasError());
    ASSERT_EQ(results.results.size(), keys.size());
    ASSERT_TRUE(results.results[0].isInserted());
    ASSERT_TRUE(results.results[0].isInsertedInBatch());
    ASSERT_TRUE(results.results[1].isInserted());
    ASSERT_TRUE(results.results[1].isInsertedInBatch());
    ASSERT_FALSE(results.results[2].isInserted());
    ASSERT_TRUE(results.results[2].isInsertedInBatch());
}

TEST(HybridHashTable, Serde)
{
    CalledCounts counts;
    auto table = createHashTable<std::string, std::string>(counts, 3);
    std::vector<std::string> keys = {"k1", "k2", "k3", "k4", "k5"};
    auto results = table->emplaceKeys(keys);
    ASSERT_FALSE(results.hasError());

    for (auto elem : std::views::zip(results.results, keys))
    {
        auto & result = std::get<0>(elem);
        const auto & key = std::get<1>(elem);

        auto * mapped = result.getMutableMapped();
        ASSERT_TRUE(mapped != nullptr);
        *reinterpret_cast<std::string *>(mapped) = key;
    }

    /// Serialize and deserialize
    DB::WriteBufferFromOwnString wb1;
    table->write(wb1);
    table.reset();

    DB::ReadBufferFromString rb(wb1.str());
    auto recovered_table = createHashTable<std::string, std::string>(counts, 3);
    recovered_table->read(rb);

    ASSERT_TRUE(recovered_table->contains("k1"));
    ASSERT_TRUE(recovered_table->contains("k2"));
    ASSERT_TRUE(recovered_table->contains("k3"));
    ASSERT_TRUE(recovered_table->contains("k4"));
    ASSERT_TRUE(recovered_table->contains("k5"));

    recovered_table->forEachKeyValue(
        [](const auto & key, auto value) { ASSERT_EQ(key, *reinterpret_cast<const std::string *>(value.getMapped())); });

    /// Serialize and deserialize again
    DB::WriteBufferFromOwnString wb2;
    recovered_table->write(wb2);
    recovered_table.reset();

    DB::ReadBufferFromString rb2(wb2.str());
    auto recovered_table2 = createHashTable<std::string, std::string>(counts, 3);
    recovered_table2->read(rb2);

    ASSERT_TRUE(recovered_table2->contains("k1"));
    ASSERT_TRUE(recovered_table2->contains("k2"));
    ASSERT_TRUE(recovered_table2->contains("k3"));
    ASSERT_TRUE(recovered_table2->contains("k4"));
    ASSERT_TRUE(recovered_table2->contains("k5"));

    recovered_table2->forEachKeyValue(
        [](const auto & key, auto value) { ASSERT_EQ(key, *reinterpret_cast<const std::string *>(value.getMapped())); });
}

TEST(HybridHashTable, Flush)
{
    CalledCounts counts;
    auto table = createHashTable<std::string, std::string>(counts, 3);
    const auto & metrics = table->getMetrics();
    std::vector<std::string> keys = {"k1", "k2", "k3", "k4"};
    auto results = table->emplaceKeys(keys);
    ASSERT_FALSE(results.hasError());
    for (auto elem : std::views::zip(results.results, keys))
    {
        auto & result = std::get<0>(elem);
        const auto & key = std::get<1>(elem);

        auto * mapped = result.getMutableMapped();
        ASSERT_TRUE(mapped != nullptr);
        *reinterpret_cast<std::string *>(mapped) = key;
    }

    EXPECT_EQ(metrics.spilled, 0);
    EXPECT_EQ(counts.k_ser_called, 0);

    table->spillIfNecessary();
    EXPECT_EQ(metrics.spilled, 1);
    EXPECT_EQ(counts.k_ser_called, 1); /// spill 1 keys (k1)

    table->flush();
    EXPECT_EQ(metrics.spilled, 2);
    EXPECT_EQ(counts.k_ser_called, 1 + 3); /// spill other 3 keys

    ASSERT_FALSE(table->findKeys({"k1", "k2", "k3"}).hasError());
    table->flush();
    EXPECT_EQ(metrics.spilled, 2); /// No changed keys, so no spill
    EXPECT_EQ(counts.k_ser_called, 4 + 1); /// Find cold key (k1)

    results = table->emplaceKeys({"k2", "k3"});
    ASSERT_FALSE(results.hasError());
    for (auto elem : std::views::zip(results.results, keys))
    {
        auto & result = std::get<0>(elem);
        const auto & key = std::get<1>(elem);

        auto * mapped = result.getMutableMapped();
        ASSERT_TRUE(mapped != nullptr);
        *reinterpret_cast<std::string *>(mapped) = key;
    }

    ASSERT_FALSE(results.hasError());
    table->flush();
    EXPECT_EQ(metrics.spilled, 3); /// spill 2 changed key
    EXPECT_EQ(counts.k_ser_called, 5 + 2);
}

TEST(HybridHashTable, ClearRemovesSharedPersistentData)
{
    CalledCounts counts;
    auto spill_dir = std::filesystem::absolute("tmp/gtest-hybrid-ht-clear").string();
    std::filesystem::remove_all(spill_dir);
    std::filesystem::create_directories(std::filesystem::path(spill_dir).parent_path());

    auto value_ser = [&](const void * p, DB::WriteBuffer & wb) -> int {
        ++counts.v_ser_called;
        DB::writeBinary(*reinterpret_cast<const std::string *>(p), wb);
        return DB::ErrorCodes::OK;
    };

    auto value_des = [&](void * p, DB::ReadBuffer & rb) -> int {
        ++counts.v_des_called;
        DB::readBinary(*reinterpret_cast<std::string *>(p), rb);
        return DB::ErrorCodes::OK;
    };

    auto key_serializer = [&](const std::string & key, DB::WriteBuffer & wb) -> int {
        ++counts.k_ser_called;
        DB::writeBinary(key, wb);
        return DB::ErrorCodes::OK;
    };

    auto key_deserializer = [&](std::string & key, DB::ReadBuffer & rb) -> int {
        ++counts.k_des_called;
        DB::readBinary(key, rb);
        return DB::ErrorCodes::OK;
    };

    DB::HybridConfig rocks_config;
    rocks_config.spill_dir_path = spill_dir;
    rocks_config.cleanup_on_disk_data = true;
    rocks_config.validate();

    auto shared_rocks = DB::RocksDB::createOrLoadIfExists(
        rocks_config.getRocksOptions(),
        rocks_config.spill_dir_path,
        rocks_config.ttl,
        rocks_config.cleanup_on_disk_data,
        getLogger("hybrid_unit"));

    auto make_shared_table = [&]() {
        DB::HybridHashTableConfig config{
            .base_conf = {
                .spill_dir_path = spill_dir,
                .kv_options = "",
                .max_hot_key_count = 1,
                .cleanup_on_disk_data = true,
                .cf_handle_id = "updates",
                .rocks_cf_handler_getter = [shared_rocks](const DB::HybridConfig & rocks_sub_config) {
                    return shared_rocks->getOrCreateColumnFamilyHandler(rocks_sub_config.cf_handle_id, rocks_sub_config.ttl);
                },
            },
            .value_object_size = sizeof(std::string),
            .align_value_object_size = alignof(std::string),
            .value_constructor = [&](void * p) {
                ++counts.v_ctor_called;
                new (p) std::string;
            },
            .value_destructor = [&](void * p) {
                ++counts.v_dtor_called;
                reinterpret_cast<std::string *>(p)->~basic_string();
            },
            .value_serializer = value_ser,
            .value_deserializer = value_des,
        };

        config.validate();

        return DB::HybridHashTable<std::string>(config, key_serializer, key_deserializer, getLogger("hybrid_unit"));
    };

    {
        auto table = make_shared_table();
        auto result_a = table.emplaceKey("a", /*disable_spill=*/false);
        ASSERT_FALSE(result_a.hasError());
        *reinterpret_cast<std::string *>(result_a.getMutableMapped()) = "a";

        auto result_b = table.emplaceKey("b", /*disable_spill=*/false);
        ASSERT_FALSE(result_b.hasError());
        *reinterpret_cast<std::string *>(result_b.getMutableMapped()) = "b";

        EXPECT_GT(table.getMetrics().spilled, 0);
        table.clear();

        auto result_c = table.emplaceKey("c", /*disable_spill=*/false);
        ASSERT_FALSE(result_c.hasError());
        *reinterpret_cast<std::string *>(result_c.getMutableMapped()) = "c";

        auto result_d = table.emplaceKey("d", /*disable_spill=*/false);
        ASSERT_FALSE(result_d.hasError());
        *reinterpret_cast<std::string *>(result_d.getMutableMapped()) = "d";

        EXPECT_GT(table.getMetrics().spilled, 0);
        EXPECT_FALSE(table.contains("a"));
        EXPECT_FALSE(table.contains("b"));
    }

    auto recovered = make_shared_table();
    EXPECT_FALSE(recovered.contains("a"));
    EXPECT_FALSE(recovered.contains("b"));
    EXPECT_EQ(recovered.approximateCount(), 0);
}
