#include <gtest/gtest.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_NURAFT

#include <Coordination/KVRequest.h>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/LoggerWrapper.h>
#include <Coordination/MetaSnapshotManager.h>
#include <Coordination/MetaStateMachine.h>
#include <Coordination/MetaStateManager.h>

#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Common/logger_useful.h>

#include <libnuraft/nuraft.hxx>

#include <filesystem>
#include <thread>


namespace fs = std::filesystem;
struct ChangelogDirTest
{
    std::string path;
    bool drop;
    explicit ChangelogDirTest(std::string path_, bool drop_ = true) : path(path_), drop(drop_)
    {
        if (fs::exists(path))
        {
            EXPECT_TRUE(false) << "Path " << path << " already exists, remove it to run test";
        }
        fs::create_directory(path);
    }

    ~ChangelogDirTest()
    {
        if (fs::exists(path) && drop)
            fs::remove_all(path);
    }
};

TEST(CoordinationTest1, BufferSerde)
{
    Coordination::KVRequestPtr request = Coordination::KVRequestFactory::instance().get(Coordination::KVOpNum::MULTIPUT);
    dynamic_cast<Coordination::KVMultiPutRequest &>(*request).kv_pairs = {{"/path/value", "1.0"}};

    DB::WriteBufferFromNuraftBuffer wbuf;
    request->write(wbuf);
    auto nuraft_buffer = wbuf.getBuffer();

    DB::ReadBufferFromNuraftBuffer rbuf(nuraft_buffer);
    Coordination::KVRequestPtr request_read = Coordination::KVRequest::read(rbuf);

    EXPECT_EQ(request_read->getOpNum(), Coordination::KVOpNum::MULTIPUT);
    EXPECT_EQ(dynamic_cast<Coordination::KVMultiPutRequest &>(*request_read).kv_pairs.front().first, "/path/value");
    EXPECT_EQ(dynamic_cast<Coordination::KVMultiPutRequest &>(*request_read).kv_pairs.front().second, "1.0");
}

struct MetaRaftServer
{
    MetaRaftServer(int server_id_, const std::string & hostname_, int port_, const std::string & logs_path)
        : server_id(server_id_)
        , hostname(hostname_)
        , port(port_)
        , endpoint(hostname + ":" + std::to_string(port))
        , state_manager(nuraft::cs_new<DB::MetaStateManager>(server_id, hostname, port, logs_path))
    {
        Coordination::MetaSnapshotsQueue snapshots_queue{10};
        Coordination::CoordinationSettingsPtr settings = std::make_shared<Coordination::CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        state_machine = std::make_shared<Coordination::MetaStateMachine>(snapshots_queue, "./snapshots", "./meta", settings);
        state_machine->init();
        state_manager->loadLogStore(state_machine->last_commit_index() + 1, 0);
        nuraft::raft_params params;
        params.heart_beat_interval_ = 100;
        params.election_timeout_lower_bound_ = 200;
        params.election_timeout_upper_bound_ = 400;
        params.reserved_log_items_ = 5;
        params.snapshot_distance_ = 10; /// forcefully send snapshots
        params.client_req_timeout_ = 3000;
        params.return_method_ = nuraft::raft_params::blocking;

        raft_instance = launcher.init(
            state_machine,
            state_manager,
            nuraft::cs_new<DB::LoggerWrapper>("ToyRaftLogger", DB::LogsLevel::trace),
            port,
            nuraft::asio_service::options{},
            params);

        if (!raft_instance)
        {
            std::cerr << "Failed to initialize launcher" << std::endl;
            exit(-1);
        }

        std::cout << "init Raft instance " << server_id;
        for (size_t ii = 0; ii < 20; ++ii)
        {
            if (raft_instance->is_initialized())
            {
                std::cout << " done" << std::endl;
                break;
            }
            std::cout << ".";
            fflush(stdout);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // Server ID.
    int server_id;

    // Server address.
    std::string hostname;

    // Server port.
    int port;

    std::string endpoint;

    // State machine.
    nuraft::ptr<DB::MetaStateMachine> state_machine;

    // State manager.
    nuraft::ptr<DB::MetaStateManager> state_manager;

    // Raft launcher.
    nuraft::raft_launcher launcher;

    // Raft server instance.
    nuraft::ptr<nuraft::raft_server> raft_instance;
};

nuraft::ptr<nuraft::buffer> getBufferFromKVRequest(const Coordination::KVRequestPtr & request)
{
    DB::WriteBufferFromNuraftBuffer buf;
    request->write(buf);
    return buf.getBuffer();
}

nuraft::ptr<nuraft::log_entry> getLogEntryFromKVRequest(size_t term, const Coordination::KVRequestPtr & request)
{
    auto buffer = getBufferFromKVRequest(request);
    return nuraft::cs_new<nuraft::log_entry>(term, buffer);
}

TEST(CoordinationTest1, TestMetaRaft1)
{
    using namespace Coordination;

    ChangelogDirTest test("./logs");
    ChangelogDirTest snapshots("./snapshots");

    MetaRaftServer s1(1, "localhost", 44444, "./logs");

    /// Single node is leader
    EXPECT_EQ(s1.raft_instance->get_leader(), 1);

    std::shared_ptr<KVMultiPutRequest> request = std::make_shared<KVMultiPutRequest>();
    request->kv_pairs = {{"version", "1.0.0"}};
    auto entry = getBufferFromKVRequest(request);
    auto ret = s1.raft_instance->append_entries({entry});
    EXPECT_TRUE(ret->get_accepted()) << "failed to replicate: entry 1" << ret->get_result_code();
    EXPECT_EQ(ret->get_result_code(), nuraft::cmd_result_code::OK) << "failed to replicate: entry 1" << ret->get_result_code();

    std::string value;
    do
    {
        s1.state_machine->getByKey("version", &value);
        std::cout << "Waiting s1 to apply entry\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (value != "1.0.0");

    EXPECT_EQ(value, "1.0.0");

    s1.launcher.shutdown(5);
}

void testCreateRestoreSnapshot(Coordination::CoordinationSettingsPtr settings, uint64_t total_logs)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest logs("./logs");

    ChangelogDirTest snapshots1("./snapshots1");

    MetaSnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<MetaStateMachine>(snapshots_queue, "./snapshots", "./meta", settings);
    state_machine->init();
    DB::KeeperLogStore changelog("./logs", settings->rotate_log_storage_interval, true, false);

    MetaSnapshotsQueue snapshots_queue1{1};
    auto restore_machine = std::make_shared<MetaStateMachine>(snapshots_queue1, "./snapshots1", "./meta1", settings);
    restore_machine->init();

    uint64_t start_idx = state_machine->last_commit_index();
    uint64_t end_idx = start_idx + total_logs;
    changelog.init(state_machine->last_commit_index() + 1, settings->reserved_log_items);
    for (size_t i = 1; i < total_logs + 1; ++i)
    {
        std::shared_ptr<KVMultiPutRequest> request = std::make_shared<KVMultiPutRequest>();
        request->kv_pairs = {{"version", std::to_string(i)}};
        auto entry = getLogEntryFromKVRequest(0, request);
        uint64_t idx = changelog.append(entry);
        changelog.end_of_append_batch(0, 0);

        state_machine->commit(idx, changelog.entry_at(idx)->get_buf());
        bool snapshot_created = false;
        nuraft::snapshot s(idx, 0, std::make_shared<nuraft::cluster_config>());
        if (idx % settings->snapshot_distance == 0)
        {
            nuraft::async_result<bool>::handler_type when_done
                = [&snapshot_created](bool & ret, nuraft::ptr<std::exception> & /*exception*/) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                      snapshot_created = ret;
                      std::cerr << "Snapshot finised\n";
                  };

            state_machine->create_snapshot(s, when_done);
            CreateMetaSnapshotTask snapshot_task;
            (void)snapshots_queue.pop(snapshot_task);
            snapshot_task.create_snapshot(std::move(snapshot_task.snapshot));
        }
        if (snapshot_created)
        {
            if (changelog.size() > settings->reserved_log_items)
            {
                uint64_t c_idx = idx - settings->reserved_log_items;
                changelog.compact(c_idx);
            }

            // simulate the transfer of snapshot
            bool is_last_snp = false;
            bool is_first_snp = true;
            uint64_t obj_id = 0;

            do
            {
                nuraft::ptr<nuraft::buffer> buffer = nuraft::buffer::alloc(100);
                void * ctx;
                state_machine->read_logical_snp_obj(s, ctx, obj_id, buffer, is_last_snp);
                is_first_snp = obj_id == 0 ? true : false;
                restore_machine->save_logical_snp_obj(s, obj_id, *buffer, is_first_snp, is_last_snp);
            } while (!is_last_snp);
        }
    }


    // restore from last snapshot
    std::cerr << "Start Index of restore_machine is: " << start_idx << ", End Index of restore_machine should be: " << end_idx << std::endl;
    nuraft::snapshot s(restore_machine->last_snapshot()->get_last_log_idx(), 0, std::make_shared<nuraft::cluster_config>());
    restore_machine->apply_snapshot(s);
    EXPECT_EQ(restore_machine->last_commit_index(), end_idx - end_idx % settings->snapshot_distance);

    DB::KeeperLogStore restore_changelog("./logs", settings->rotate_log_storage_interval, true, false);
    restore_changelog.init(restore_machine->last_commit_index() + 1, settings->reserved_log_items);

    EXPECT_EQ(restore_changelog.size(), std::min(settings->reserved_log_items + end_idx % settings->snapshot_distance, total_logs));
    EXPECT_EQ(restore_changelog.next_slot(), end_idx + 1);
    if (total_logs > settings->reserved_log_items + 1)
        EXPECT_EQ(restore_changelog.start_index(), end_idx - end_idx % settings->snapshot_distance - settings->reserved_log_items + 1);
    else
        EXPECT_EQ(restore_changelog.start_index(), 1);

    for (size_t i = restore_machine->last_commit_index() + 1; i < restore_changelog.next_slot(); ++i)
    {
        restore_machine->commit(i, restore_changelog.entry_at(i)->get_buf());
    }

    std::string ver;
    restore_machine->getByKey("version", &ver);
    EXPECT_EQ(ver, std::to_string(total_logs));
}

TEST(CoordinationTest1, TestCreateAndRestoreSnapshots)
{
    using namespace Coordination;
    using namespace DB;

    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        testCreateRestoreSnapshot(settings, 37);
    }
}

void testLogAndStateMachine1(Coordination::CoordinationSettingsPtr settings, uint64_t total_logs)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest logs("./logs");

    //    ResponsesQueue queue;
    MetaSnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<MetaStateMachine>(snapshots_queue, "./snapshots", "./meta", settings);
    state_machine->init();
    uint64_t start_idx = state_machine->last_commit_index();
    uint64_t end_idx = start_idx + total_logs;
    DB::KeeperLogStore changelog("./logs", settings->rotate_log_storage_interval, true, false);
    changelog.init(state_machine->last_commit_index() + 1, settings->reserved_log_items);
    for (size_t i = 1; i < total_logs + 1; ++i)
    {
        std::shared_ptr<KVMultiPutRequest> request = std::make_shared<KVMultiPutRequest>();
        request->kv_pairs = {{"version", std::to_string(i)}};
        auto entry = getLogEntryFromKVRequest(0, request);
        uint64_t idx = changelog.append(entry);
        changelog.end_of_append_batch(0, 0);

        state_machine->commit(idx, changelog.entry_at(idx)->get_buf());
        bool snapshot_created = false;
        if (idx % settings->snapshot_distance == 0)
        {
            nuraft::snapshot s(idx, 0, std::make_shared<nuraft::cluster_config>());
            nuraft::async_result<bool>::handler_type when_done
                = [&snapshot_created](bool & ret, nuraft::ptr<std::exception> & /*exception*/) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                      snapshot_created = ret;
                      std::cerr << "Snapshot finised\n";
                  };

            state_machine->create_snapshot(s, when_done);
            CreateMetaSnapshotTask snapshot_task;
            (void)snapshots_queue.pop(snapshot_task);
            snapshot_task.create_snapshot(std::move(snapshot_task.snapshot));
        }
        if (snapshot_created)
        {
            if (changelog.size() > settings->reserved_log_items)
            {
                uint64_t c_idx = idx - settings->reserved_log_items;
                changelog.compact(c_idx);
            }
        }
    }

    MetaSnapshotsQueue snapshots_queue1{1};
    auto restore_machine = std::make_shared<MetaStateMachine>(snapshots_queue1, "./snapshots", "./meta1", settings);
    restore_machine->init();

    // restore from last snapshot
    std::cerr << "Start Index of restore_machine is: " << start_idx << ", End Index of restore_machine should be: " << end_idx << std::endl;
    nuraft::snapshot s(restore_machine->last_snapshot()->get_last_log_idx(), 0, std::make_shared<nuraft::cluster_config>());
    restore_machine->apply_snapshot(s);
    EXPECT_EQ(restore_machine->last_commit_index(), end_idx - end_idx % settings->snapshot_distance);

    DB::KeeperLogStore restore_changelog("./logs", settings->rotate_log_storage_interval, true, false);
    restore_changelog.init(restore_machine->last_commit_index() + 1, settings->reserved_log_items);

    EXPECT_EQ(restore_changelog.size(), std::min(settings->reserved_log_items + end_idx % settings->snapshot_distance, total_logs));
    EXPECT_EQ(restore_changelog.next_slot(), end_idx + 1);

    for (size_t i = restore_machine->last_commit_index() + 1; i < restore_changelog.next_slot(); ++i)
    {
        restore_machine->commit(i, restore_changelog.entry_at(i)->get_buf());
    }

    std::string ver;
    restore_machine->getByKey("version", &ver);
    EXPECT_EQ(ver, std::to_string(total_logs));
}

TEST(CoordinationTest1, TestStateMachineAndLogStore)
{
    using namespace Coordination;
    using namespace DB;

    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine1(settings, 37);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine1(settings, 11);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine1(settings, 40);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 20;
        settings->rotate_log_storage_interval = 30;
        testLogAndStateMachine1(settings, 40);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 0;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine1(settings, 40);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 1;
        settings->reserved_log_items = 1;
        settings->rotate_log_storage_interval = 32;
        testLogAndStateMachine1(settings, 32);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 7;
        settings->rotate_log_storage_interval = 1;
        testLogAndStateMachine1(settings, 33);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 37;
        settings->reserved_log_items = 1000;
        settings->rotate_log_storage_interval = 5000;
        testLogAndStateMachine1(settings, 33);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 37;
        settings->reserved_log_items = 1000;
        settings->rotate_log_storage_interval = 5000;
        testLogAndStateMachine1(settings, 45);
    }
}


//int main(int argc, char ** argv)
//{
//    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
//    Poco::Logger::root().setChannel(channel);
//    Poco::Logger::root().setLevel("trace");
//    testing::InitGoogleTest(&argc, argv);
//    return RUN_ALL_TESTS();
//}

#endif
