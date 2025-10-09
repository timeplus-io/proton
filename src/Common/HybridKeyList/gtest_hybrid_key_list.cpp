#include <IO/WriteHelpers.h>
#include <Common/HybridKeyList/HybridKeyList.h>

#include <gtest/gtest.h>

TEST(HybridKeyList, Ops)
{
    DB::HybridKeyListConfig config{
        .spill_dir_path = "/tmp/hybrid_key_list_ops",
        .db_options = "",
        .max_hot_key_count = 3,
    };

    auto key_serializer = [](const std::string & k, DB::WriteBuffer & wb) {
        DB::writeStringBinary(k, wb);
        return DB::ErrorCodes::OK;
    };

    auto key_deserializer = [](std::string & k, DB::ReadBuffer & rb) {
        DB::readStringBinary(k, rb);
        return DB::ErrorCodes::OK;
    };

    DB::HybridKeyList<std::string> key_list(
        std::move(config), std::move(key_serializer), std::move(key_deserializer), getLogger("HybridKeyList"));

    auto for_each = [&](const std::vector<std::string> & keys) {
        std::vector<std::string> all_keys;
        std::vector<int64_t> all_key_timestamps;
        auto errcode = key_list.forEach([&](const std::string & k, int64_t ts) {
            all_keys.push_back(k);
            all_key_timestamps.push_back(ts);
        });

        ASSERT_EQ(errcode, DB::ErrorCodes::OK);
        ASSERT_EQ(all_keys.size(), keys.size());

        for (size_t i = 0; i < all_keys.size(); ++i)
            ASSERT_EQ(keys[i], all_keys[i]);

        for (size_t i = 0; i < all_key_timestamps.size() - 1; ++i)
            ASSERT_LE(all_key_timestamps[i], all_key_timestamps[i + 1]);
    };

    auto for_each_persistent = [&](const std::vector<std::string> & keys) {
        std::vector<std::string> all_keys;
        std::vector<int64_t> all_key_timestamps;
        auto errcode = key_list.forEachPersistent([&](const std::string & k, int64_t ts) {
            all_keys.push_back(k);
            all_key_timestamps.push_back(ts);
        });

        ASSERT_EQ(errcode, DB::ErrorCodes::OK);
        ASSERT_EQ(all_keys.size(), keys.size());

        for (size_t i = 0; i < all_keys.size(); ++i)
            ASSERT_EQ(keys[i], all_keys[i]);

        for (size_t i = 0; i < all_key_timestamps.size() - 1; ++i)
            ASSERT_LE(all_key_timestamps[i], all_key_timestamps[i + 1]);
    };

    {
        key_list.emplace("key5");
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        key_list.emplace("key4");
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        key_list.emplace("key3");
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        key_list.emplace("key2");
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
        key_list.emplace("key1");

        for_each(std::vector<std::string>{"key5", "key4", "key3", "key2", "key1"});
        for_each_persistent(std::vector<std::string>{"key2", "key1"});

        std::this_thread::sleep_for(std::chrono::milliseconds{20});

        absl::flat_hash_set<std::string> handled_expires{"key4"};
        auto remove_result = key_list.removeExpiredKeys(10, handled_expires);
        ASSERT_EQ(remove_result.second, DB::ErrorCodes::OK);
        ASSERT_TRUE(key_list.empty());
        ASSERT_EQ(remove_result.first.size(), 4);
        ASSERT_EQ(remove_result.first[0], "key5");
        ASSERT_EQ(remove_result.first[1], "key3");
        ASSERT_EQ(remove_result.first[2], "key2");
        ASSERT_EQ(remove_result.first[3], "key1");

        key_list.forEach([](const std::string & /*k*/, int64_t /*ts*/) { ASSERT_TRUE(false); });
    }

    {
        std::vector<std::string> keys = {"key9", "key8", "key7", "key6"};
        std::vector<std::string> keys2 = keys;
        key_list.emplace(keys2);

        auto remove_result = key_list.removeExpiredKeys(10'000, absl::flat_hash_set<std::string>{});
        ASSERT_EQ(remove_result.second, DB::ErrorCodes::OK);
        ASSERT_TRUE(!key_list.empty());
        ASSERT_TRUE(remove_result.first.empty());

        for_each(keys);
        for_each_persistent(std::vector<std::string>{"key6"});

        ASSERT_EQ(key_list.flush(), DB::ErrorCodes::OK);

        for_each(keys);

        /// Since the batch of the keys have the same timestamp, after flush, the persistent layer will sort it
        std::ranges::sort(keys);
        for_each_persistent(keys);
    }
}
