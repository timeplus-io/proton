#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/Streaming/EmitInterpreter.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>

/// Note gtest_graphite.cpp also need registers aggregation functions
/// to avoid conflicts, register only once
extern bool regAggregateFunctions;

class StreamingEmitInterpreterTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        if (!regAggregateFunctions)
        {
            DB::registerAggregateFunctions();
            regAggregateFunctions = true;
        }
    }
};

/// Collect Tree AST elems recursively
class TreeElemCollector
{
public:
    struct Data
    {
        std::unordered_map<std::string, DB::ASTPtr> elems;
    };

    static void visit(const DB::ASTPtr & node, Data & data) { data.elems.emplace(node->getID(), node); }
    static bool needChildVisit(const DB::ASTPtr &, const DB::ASTPtr &) { return true; }
};

static bool checkLastXRule(const String & last_x_query, const String & check_query, UInt64 max_keep_windows = 100)
{
    DB::Settings settings;
    settings.set("max_keep_windows", max_keep_windows);

    DB::ParserQuery last_x_parser(last_x_query.end().base());
    DB::ASTPtr last_x_ast = parseQuery(last_x_parser, last_x_query.begin().base(), last_x_query.end().base(), "", 0, 0);
    DB::Streaming::EmitInterpreter::handleRules(
        last_x_ast->as<DB::ASTSelectWithUnionQuery &>().list_of_selects->children.at(0),
        DB::Streaming::EmitInterpreter::LastXRule(settings));

    DB::ParserQuery check_parser(check_query.end().base());
    DB::ASTPtr check_ast = parseQuery(check_parser, check_query.begin().base(), check_query.end().base(), "", 0, 0);

    TreeElemCollector::Data last_x_data, check_data;
    DB::ConstInDepthNodeVisitor<TreeElemCollector, true>(last_x_data).visit(last_x_ast);
    DB::ConstInDepthNodeVisitor<TreeElemCollector, true>(check_data).visit(check_ast);

    if (last_x_data.elems.size() != check_data.elems.size())
        std::cerr << fmt::format(
            "> > > Last X Tree elems size({}) != Check Tree elems size({})\n", last_x_data.elems.size(), check_data.elems.size());

    const String special_key_1 = "TableIdentifier_default.devices";
    const String special_key_2 = "Identifier_default.devices";

    for (const auto & elem : last_x_data.elems)
    {
        if (!check_data.elems.contains(elem.first))
        {
            /// Skip special key
            if ((elem.first == special_key_1 && check_data.elems.contains(special_key_2))
                || (elem.first == special_key_2 && check_data.elems.contains(special_key_1)))
            continue;

            std::cerr << fmt::format("> > > Last X Tree has extra elem(ID={}): {}\n", elem.first, serializeAST(*elem.second));
            continue;
        }

        auto ast = check_data.elems[elem.first];
        auto tree_str = serializeAST(*elem.second);
        auto check_str = serializeAST(*ast);
        if (tree_str != check_str)
        {
            std::cerr << fmt::format("> > > Dismatched elem(ID={}):\nLast X Tree '{}'\nCheck Tree '{}'\n", elem.first, tree_str, check_str);
            continue;
        }
    }

    return serializeAST(*last_x_ast) == serializeAST(*check_ast);
}

static bool check_func = false;

static void handle(DB::ASTPtr &)
{
    check_func = true;
}

struct Foo
{
    bool & check_pseudo_func;
    Foo(bool & check_pseudo_func_) : check_pseudo_func(check_pseudo_func_) { }
    void operator()(DB::ASTPtr &) { check_pseudo_func = true; }
    void handle(DB::ASTPtr &, bool * check_bind_func) { *check_bind_func = true; }
};

TEST_F(StreamingEmitInterpreterTest, HandleRules)
{
    bool check_pseudo_func = false;
    bool check_lambada_func = false;
    bool check_bind_func = false;
    Foo obj(check_pseudo_func);
    DB::ASTPtr query;
    DB::Streaming::EmitInterpreter::handleRules(
        query,
        /* check_func */ handle,
        /* check_pseudo_func */ obj,
        /* check_lambada_func */ [&check_lambada_func](DB::ASTPtr &) { check_lambada_func = true; },
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
            "SELECT device, avg(temperature) FROM tumble(default.devices, now(), interval 5 second) where 1=1 group by device, "
            "window_end emit stream LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM tumble(default.devices, now(), interval 5 second) where 1=1 and _tp_time >= (now64(3, "
            "'UTC') -1m) group by device, window_end emit stream"),
        true)
        << "Last-X Tumble Window";
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleHopWindow)
{
    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 10 second, interval 1 minute) where 1=1 group by "
            "device, window_end emit stream LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 10 second, interval 1 minute) where 1=1 and "
            "_tp_time >= (now64(3, 'UTC') -1m) group by device, window_end emit stream"),
        true)
        << "Last-X Hop Window";
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleWindowError1)
{
    /// [Error] conflicts settings keep_windows
    EXPECT_THROW(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 10 second, interval 60 second) group by device, "
            "window_end emit stream LAST 1m settings keep_windows=2",
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
            "SELECT device, avg(temperature) FROM hop(default.devices, now(), interval 1 second, interval 10 second) group by device, "
            "window_end emit stream LAST 2m",
            /* check query */
            ""),
        DB::Exception);
}

/// note: the following 2 queries will diff in AST (Identifier_default.devices and TableIdentifier_default.devices)
TEST_F(StreamingEmitInterpreterTest, LastXRuleMaxKeepWindows)
{
    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM default.devices where 1=1 group by device emit stream LAST 2m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(default.devices, _tp_time, to_interval_second(2), to_interval_second(120)) where 1=1 "
            "and _tp_time >= (now64(3, 'UTC') - to_interval_second(120)) group by device, window_end emit stream",
            /* max_keep_windows */
            120),
        true);
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleGlobalAggr)
{
    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM default.devices where 1=1 group by device emit stream PERIODIC interval 5 second AND "
            "LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(default.devices, _tp_time, interval 5 second, to_interval_second(60)) where 1=1 and "
            "_tp_time >= (now64(3, 'UTC') - to_interval_second(60)) group by device, window_end emit stream"),
        true)
        << "Last-X Global Aggregation for table";

    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM table(default.devices) where 1=1 group by device emit stream PERIODIC interval 5 second "
            "AND LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(table(default.devices), _tp_time, interval 5 second, to_interval_second(60)) where 1=1 "
            "and "
            "_tp_time >= (now64(3, 'UTC') - to_interval_second(60)) group by device, window_end emit stream"),
        true)
        << "Last-X Global Aggregation for table_function";

    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM (select * from default.devices) where 1=1 group by device emit stream PERIODIC interval "
            "5 second AND LAST 1m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop((select * from default.devices), _tp_time, interval 5 second, to_interval_second(60)) "
            "where 1=1 and _tp_time >= (now64(3, 'UTC') - to_interval_second(60)) group by device, window_end emit stream"),
        true)
        << "Last-X Global Aggregation for subquery";
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleGlobalAggrOnProcTime)
{
    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM default.devices where 1=1 group by device emit stream PERIODIC interval 5 second AND "
            "LAST 1m on proctime",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(default.devices, now('UTC'), interval 5 second, to_interval_second(60)) where 1=1 and "
            "_tp_time >= (now64(3, 'UTC') - to_interval_second(60)) group by device, window_end emit stream"),
        true)
        << "Last-X Global Aggregation for table";

    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM table(default.devices) where 1=1 group by device emit stream PERIODIC interval 5 second "
            "AND LAST 1m on proctime",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(table(default.devices), now('UTC'), interval 5 second, to_interval_second(60)) where 1=1 "
            "and "
            "_tp_time >= (now64(3, 'UTC') - to_interval_second(60)) group by device, window_end emit stream"),
        true)
        << "Last-X Global Aggregation for table_function";

    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM (select * from default.devices) where 1=1 group by device emit stream PERIODIC interval "
            "500 millisecond AND LAST 1m on proctime",
            /* check query */
            "SELECT device, avg(temperature) FROM hop((select * from default.devices), now64(1, 'UTC'), interval 500 millisecond, to_interval_millisecond(60000)) "
            "where 1=1 and _tp_time >= (now64(3, 'UTC') - to_interval_millisecond(60000)) group by device, window_end emit stream"),
        true)
        << "Last-X Global Aggregation for subquery";
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleGlobalAggr2)
{
    /// Different interval scale
    EXPECT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM default.devices where 1=1 group by device emit stream LAST 2m",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(default.devices, _tp_time, to_interval_second(2), to_interval_second(120)) where 1=1 "
            "and _tp_time >= (now64(3, 'UTC') - to_interval_second(120)) group by device, window_end emit stream",
            /* max_keep_windows */
            120),
        true);

    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */
            "SELECT device, avg(temperature) FROM default.devices where 1=1 group by device emit stream LAST 1y and periodic 1q",
            /* check query */
            "SELECT device, avg(temperature) FROM hop(default.devices, _tp_time, 1q, to_interval_quarter(4)) where 1=1 and _tp_time >= "
            "(now64(3, 'UTC') - to_interval_quarter(4)) group by device, window_end emit stream"),
        true);
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleTailMode)
{
    /// Last-X For Tail (Exception)
    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */ "SELECT * FROM default.devices where 1=1 emit stream LAST 1h",
            /* check query */
            "SELECT * FROM default.devices where 1=1 and _tp_time >= (now64(3, 'UTC') - 1h) emit stream"),
        true)
        << "Last-X Tail";
}

TEST_F(StreamingEmitInterpreterTest, LastXRuleTailModeWithWhere)
{
    /// Last-X For Tail (Exception)
    ASSERT_EQ(
        checkLastXRule(
            /* lastX query */ "SELECT * FROM default.devices WHERE device='dev1' emit stream LAST 1h",
            /* check query */
            "SELECT * FROM default.devices WHERE device='dev1' and _tp_time >= (now64(3, 'UTC') - 1h) emit stream"),
        true)
        << "Last-X Tail with where";
}