#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Streaming/StreamingEmitInterpreter.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>

using namespace DB;
class StreamingEmitInterpreterTest : public ::testing::Test
{
public:
    static void SetUpTestSuite() { registerAggregateFunctions(); }
};

/// Collect Tree AST elems recursively
class TreeElemCollector
{
public:
    struct Data
    {
        std::unordered_map<std::string, ASTPtr> elems;
    };

    static void visit(const ASTPtr & node, Data & data) { data.elems.emplace(node->getID(), node); }
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

static bool checkLastXRule(const String & last_x_query, const String & check_query, UInt64 max_keep_windows = 100)
{
    Settings settings;
    settings.set("max_keep_windows", max_keep_windows);

    bool tail = false;
    Int64 last_interval_seconds = 0;

    ParserQuery last_x_parser(last_x_query.end().base());
    ASTPtr last_x_ast = parseQuery(last_x_parser, last_x_query.begin().base(), last_x_query.end().base(), "", 0, 0);
    StreamingEmitInterpreter::handleRules(
        last_x_ast->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0), StreamingEmitInterpreter::LastXRule(settings, last_interval_seconds, tail));

    ParserQuery check_parser(check_query.end().base());
    ASTPtr check_ast = parseQuery(check_parser, check_query.begin().base(), check_query.end().base(), "", 0, 0);

    TreeElemCollector::Data last_x_data, check_data;
    ConstInDepthNodeVisitor<TreeElemCollector, true>(last_x_data).visit(last_x_ast);
    ConstInDepthNodeVisitor<TreeElemCollector, true>(check_data).visit(check_ast);

    if (last_x_data.elems.size() != check_data.elems.size())
        std::cerr << fmt::format(
            "> > > Last X Tree elems size({}) != Check Tree elems size({})\n", last_x_data.elems.size(), check_data.elems.size());
    for (const auto & elem : last_x_data.elems)
    {
        if (check_data.elems.count(elem.first) == 0)
        {
            std::cerr << fmt::format("> > > Last X Tree has extra elem(ID={}): {}\n", elem.first, serializeAST(*elem.second));
            continue;
        }

        auto ast = check_data.elems[elem.first];
        if (elem.second->getTreeHash() != ast->getTreeHash())
        {
            std::cerr << fmt::format(
                "> > > Dismatched elem(ID={}), Last X Tree '{}' and Check Tree '{}'\n",
                elem.first,
                serializeAST(*elem.second),
                serializeAST(*ast));
            continue;
        }
    }

    return last_x_ast->getTreeHash() == check_ast->getTreeHash();
}

static bool check_func = false;

static void handle(ASTPtr &)
{
    check_func = true;
}

struct Foo
{
    bool & check_pseudo_func;
    Foo(bool & check_pseudo_func_) : check_pseudo_func(check_pseudo_func_) { }
    void operator()(ASTPtr &) { check_pseudo_func = true; }
    void handle(ASTPtr &, bool * check_bind_func) { *check_bind_func = true; }
};

TEST_F(StreamingEmitInterpreterTest, HandleRules)
{
    bool check_pseudo_func = false;
    bool check_lambada_func = false;
    bool check_bind_func = false;
    Foo obj(check_pseudo_func);
    ASTPtr query;
    StreamingEmitInterpreter::handleRules(
        query,
        /* check_func */ handle,
        /* check_pseudo_func */ obj,
        /* check_lambada_func */ [&check_lambada_func](ASTPtr &) { check_lambada_func = true; },
        /* check_bind_func */ std::bind(&Foo::handle, &obj, std::placeholders::_1, &check_bind_func));

    EXPECT_TRUE(check_func);
    EXPECT_TRUE(check_pseudo_func);
    EXPECT_TRUE(check_lambada_func);
    EXPECT_TRUE(check_bind_func);
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleTumbleWindow)
{
    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM tumble(default.devices, now(), interval 5 second) group by device, "
            "window_end emit stream LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM tumble(default.devices, now(), interval 5 second) group by device, window_end emit "
            "stream settings keep_windows=12, seek_to='-60s'"),
        true)
        << "Last-X Tumble Window";
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleHopWindow)
{
    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 10 second, interval 1 minute) "
            "group by device, window_end emit stream LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 10 second, interval 1 minute) group by device, "
            "window_end emit stream settings keep_windows=6, seek_to='-60s'"),
        true)
        << "Last-X Hop Window";
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleWindowError1)
{
    /// [Error] slide and hop interval scale is inconsistent
    EXPECT_THROW(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 10 second, interval 1 minute) "
            "group by device, window_end emit stream LAST 1m",
            /* check query */
            ""),
        DB::Exception);
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleWindowError2)
{
    /// [Error] conflicts settings keep_windows
    EXPECT_THROW(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 10 second, interval 60 second) "
            "group by device, window_end emit stream LAST 1m settings keep_windows=2",
            /* check query */
            ""),
        DB::Exception);
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleMaxKeepWindowsError1)
{
    /// [Error] keep_windows=120 over 100
    EXPECT_THROW(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 1 second, interval 10 second) "
            "group by device, window_end emit stream LAST 2m",
            /* check query */
            ""),
        DB::Exception);
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleMaxKeepWindows)
{
    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM default.devices group by device emit stream LAST 2m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 1 second, interval 120 second) group by device, "
            "window_end emit stream settings seek_to='-120s'",
            /* max_keep_windows */
            120),
        true);
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleGlobalAggr)
{
    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM default.devices group by device emit stream PERIODIC interval 5 second "
            "AND LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 5 second, interval 60 second) group by device, "
            "window_end emit stream settings seek_to='-60s'"),
        true)
        << "Last-X Global Aggregation for table";

    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM hist(default.devices) group by device emit stream PERIODIC interval 5 second "
            "AND LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(hist(default.devices), now(), interval 5 second, interval 60 second) group by device, "
            "window_end emit stream settings seek_to='-60s'"),
        true)
        << "Last-X Global Aggregation for table_function";

    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM (select * from default.devices) group by device emit stream PERIODIC interval 5 second "
            "AND LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop((select * from default.devices), now(), interval 5 second, interval 60 second) group by device, "
            "window_end emit stream settings seek_to='-60s'"),
        true)
        << "Last-X Global Aggregation for subquery";
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleTailMode)
{
    /// Last-X For Tail (Exception)
    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */ "SELECT * FROM default.devices emit stream LAST 1h",
            /* check query */ "SELECT * FROM default.devices emit stream settings seek_to='-3600s'"),
        true)
        << "Last-X Tail";
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleTailModeWithWhere)
{
    /// Last-X For Tail (Exception)
    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */ "SELECT * FROM default.devices WHERE device='dev1' emit stream LAST 1h",
            /* check query */ "SELECT * FROM default.devices WHERE device='dev1' emit stream settings seek_to='-3600s'"),
        true)
        << "Last-X Tail with where";
}
