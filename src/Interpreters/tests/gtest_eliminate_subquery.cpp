#include <Interpreters/UnnestSubqueryVisitor.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

using namespace DB;

static String optimizeSubquery(const String & query)
{
    const char * start = query.data();
    const char * end = start + query.size();
    ParserQuery parser(end);
    ASTPtr ast = parseQuery(parser, start, end, "", 0, 0);
    if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
    {
        for (auto & select : select_with_union_query->list_of_selects->children)
        {
            UnnestSubqueryVisitorData data;
            UnnestSubqueryVisitor(data).visit(select);
        }
    }

    return serializeAST(*ast);
}

TEST(EliminateSubquery, OptimizedQuery)
{
    EXPECT_EQ(optimizeSubquery("SELECT a FROM (SELECT b AS a FROM product)"), "SELECT b AS a FROM product");
    EXPECT_EQ(optimizeSubquery("SELECT a AS c FROM (SELECT b AS a FROM product)"), "SELECT b AS c FROM product");
    EXPECT_EQ(optimizeSubquery("SELECT a FROM (SELECT * FROM product)"), "SELECT a FROM product");
    EXPECT_EQ(optimizeSubquery("SELECT count(*) FROM (SELECT * FROM access1)"), "SELECT count(*) FROM access1");
    EXPECT_EQ(optimizeSubquery("SELECT count(1) FROM (SELECT * FROM access1 a)"), "SELECT count(1) FROM access1 AS a");
    EXPECT_EQ(
        optimizeSubquery("SELECT count(*) from (SELECT a.* FROM infoflow_url_data_dws a)"),
        "SELECT count(*) FROM infoflow_url_data_dws AS a");
    EXPECT_EQ(
        optimizeSubquery("SELECT count(*) from (SELECT a.sign FROM infoflow_url_data_dws a)"),
        "SELECT count(*) FROM infoflow_url_data_dws AS a");
    EXPECT_EQ(
        optimizeSubquery("SELECT count(*) FROM (SELECT a.sign FROM infoflow_url_data_dws a WHERE a.sign = 'abcd')"),
        "SELECT count(*) FROM infoflow_url_data_dws AS a WHERE a.sign = 'abcd'");
    EXPECT_EQ(
        optimizeSubquery("SELECT count(*) FROM (SELECT a.sign, a.channel FROM infoflow_url_data_dws a WHERE a.sign = 'abcd') WHERE "
                         "channel = 'online'"),
        "SELECT count(*) FROM infoflow_url_data_dws AS a WHERE (a.sign = 'abcd') AND (channel = 'online')");
    EXPECT_EQ(
        optimizeSubquery("SELECT product_name, Code, sum(asBaseName0) asBaseName0, sum(asBaseName01) asBaseName01 FROM (SELECT "
                         "price asBaseName0, "
                         "sale_price asBaseName01, product_name product_name, Code Code FROM price) GROUP BY product_name, Code"),
        "SELECT product_name AS product_name, Code AS Code, sum(price) AS asBaseName0, sum(sale_price) AS asBaseName01 FROM price GROUP BY "
        "product_name, "
        "Code");
    EXPECT_EQ(
        optimizeSubquery("SELECT count(a.productid) FROM (SELECT a.*, b.* FROM access1 a, db2.product_info b WHERE (a.productid = "
                         "b.productid))"),
        "SELECT count(a.productid) FROM access1 AS a , db2.product_info AS b WHERE a.productid = b.productid");
    EXPECT_EQ(
        optimizeSubquery("SELECT count(*) FROM (SELECT SIGN FROM (SELECT SIGN, query FROM tablea WHERE query_id='mock-1') WHERE "
                         "query = "
                         "'query_sql') WHERE SIGN = 'external'"),
        "SELECT count(*) FROM tablea WHERE ((query_id = 'mock-1') AND (query = 'query_sql')) AND (SIGN = 'external')");
    EXPECT_EQ(
        optimizeSubquery("SELECT sum(a.price), sum(b.sale) FROM (SELECT a.*, b.* FROM access1 a, product_info b WHERE (a.productid "
                         "= "
                         "b.productid)) GROUP BY a.name, b.sign"),
        "SELECT sum(a.price), sum(b.sale) FROM access1 AS a , product_info AS b WHERE a.productid = b.productid GROUP BY a.name, "
        "b.sign");
    EXPECT_EQ(
        optimizeSubquery("SELECT sum(a.price), sum(b.sale) FROM (SELECT a.*, b.* FROM access1 a JOIN product_info b ON a.productid "
                         "= "
                         "b.productid) WHERE a.name = 'foo' GROUP BY a.name, b.sign"),
        "SELECT sum(a.price), sum(b.sale) FROM access1 AS a INNER JOIN product_info AS b ON a.productid = b.productid WHERE a.name = "
        "'foo' "
        "GROUP BY a.name, b.sign");
    EXPECT_EQ(optimizeSubquery("SELECT a, b FROM (SELECT * FROM (SELECT 1)) WHERE y < 1"), "SELECT a, b FROM (SELECT 1) WHERE y < 1");
    EXPECT_EQ(
        optimizeSubquery("SELECT a, ssp FROM (SELECT sum(show_pv) ssp, a FROM (SELECT * FROM infoflow_url_data_dws))"),
        "SELECT a, ssp FROM (SELECT sum(show_pv) AS ssp, a FROM infoflow_url_data_dws)");
    EXPECT_EQ(
        optimizeSubquery("SELECT a, b from (select * from users1) UNION SELECT c, d from (select * from users2)"),
        "SELECT a, b FROM users1 UNION  SELECT c, d FROM users2");
    EXPECT_EQ(
        optimizeSubquery("SELECT * FROM (SELECT avg(price) FROM (SELECT price, product_name FROM product)  GROUP BY product_name)"),
        "SELECT * FROM (SELECT avg(price) FROM product GROUP BY product_name)");
    EXPECT_EQ(
        optimizeSubquery(
            "SELECT count(b.code) FROM (SELECT a.code, b.code FROM product_info a, product_info b WHERE (a.productid = b.productid))"),
        "SELECT count(b.code) FROM product_info AS a , product_info AS b WHERE a.productid = b.productid");
    EXPECT_EQ(
        optimizeSubquery(
            "SELECT count(code) FROM (SELECT a.code, b.code FROM product_info a, product_info b WHERE (a.productid = b.productid))"),
        "SELECT count(code) FROM product_info AS a , product_info AS b WHERE a.productid = b.productid");
    EXPECT_EQ(
        optimizeSubquery("SELECT * FROM (SELECT _raw AS c, t._raw FROM default.frontier_integration_test AS t)"),
        "SELECT _raw AS c, t._raw FROM default.frontier_integration_test AS t");
    EXPECT_EQ(
        optimizeSubquery("SELECT * FROM (SELECT _raw AS c, _raw FROM default.frontier_integration_test)"),
        "SELECT _raw AS c, _raw FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT * from (SELECT sourcetype, _raw AS temp FROM default.frontier_integration_test)"),
        "SELECT sourcetype, _raw AS temp FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT temp, * from (SELECT sourcetype, _raw AS temp FROM default.frontier_integration_test)"),
        "SELECT _raw AS temp, sourcetype, temp FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT temp AS d, * from (SELECT sourcetype, _raw AS temp FROM default.frontier_integration_test)"),
        "SELECT _raw AS d, sourcetype, _raw AS temp FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT *, temp from (SELECT sourcetype, _raw AS temp FROM default.frontier_integration_test)"),
        "SELECT sourcetype, _raw AS temp, temp FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT *, temp from (SELECT sourcetype, _raw AS temp FROM default.frontier_integration_test)"),
        "SELECT sourcetype, _raw AS temp, temp FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT * FROM (SELECT _raw, _time FROM default.frontier_integration_test)"),
        "SELECT _raw, _time FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT _raw, _raw, * FROM (SELECT _raw, * FROM default.frontier_integration_test)"),
        "SELECT _raw, _raw, * FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT _raw, _raw, * FROM (SELECT _raw, t.* FROM default.frontier_integration_test AS t)"),
        "SELECT _raw, _raw, t.* FROM default.frontier_integration_test AS t");
    EXPECT_EQ(
        optimizeSubquery("SELECT * FROM (SELECT _raw AS c, *, sourcetype AS d FROM default.frontier_integration_test)"),
        "SELECT _raw AS c, *, sourcetype AS d FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT * FROM (SELECT _raw AS c, *, sourcetype AS d, * FROM default.frontier_integration_test)"),
        "SELECT _raw AS c, *, sourcetype AS d FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT * FROM (SELECT _raw AS c, *, sourcetype AS d, *, _time AS e FROM default.frontier_integration_test)"),
        "SELECT _raw AS c, *, sourcetype AS d, _time AS e FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT *, _raw FROM (SELECT _raw FROM default.frontier_integration_test)"),
        "SELECT _raw, _raw FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT *, sourcetype FROM (SELECT _raw, _time, * from default.frontier_integration_test)"),
        "SELECT *, sourcetype FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT *, sourcetype FROM (SELECT _raw, _time, sourcetype FROM default.frontier_integration_test)"),
        "SELECT _raw, _time, sourcetype, sourcetype FROM default.frontier_integration_test");

    EXPECT_EQ(
        optimizeSubquery(
            "SELECT *, sourcetype FROM (SELECT _raw, _time, sourcetype, sourcetype, * FROM default.frontier_integration_test)"),
        "SELECT *, sourcetype FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT *, sourcetype FROM (SELECT sourcetype, sourcetype FROM default.frontier_integration_test)"),
        "SELECT sourcetype, sourcetype FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT *, sourcetype FROM (SELECT _raw, _time, sourcetype, * FROM default.frontier_integration_test)"),
        "SELECT *, sourcetype FROM default.frontier_integration_test");
    EXPECT_EQ(
        optimizeSubquery("SELECT *, sourcetype FROM (SELECT * EXCEPT _time FROM default.frontier_integration_test)"),
        "SELECT * EXCEPT _time, sourcetype FROM default.frontier_integration_test");
}

TEST(EliminateSubquery, FailedOptimizedQuery)
{
    /// Subquery has func call in selects
    EXPECT_EQ(
        optimizeSubquery(
            "SELECT count(*) FROM (SELECT runningDifference(i) AS diff FROM (SELECT * FROM test_query ORDER BY i DESC) WHERE diff > 0)"),
        "SELECT count(*) FROM (SELECT runningDifference(i) AS diff FROM (SELECT * FROM test_query ORDER BY i DESC) WHERE diff > 0)");
    EXPECT_EQ(optimizeSubquery("SELECT * FROM (SELECT avg(price) FROM product)"), "SELECT * FROM (SELECT avg(price) FROM product)");

    /// Subquery has alias
    EXPECT_EQ(
        optimizeSubquery("SELECT t.c FROM (SELECT a AS c, b FROM product) AS t"), "SELECT t.c FROM (SELECT a AS c, b FROM product) AS t");
    EXPECT_EQ(
        optimizeSubquery("SELECT t.c AS d FROM (SELECT a AS c, b FROM product) AS t"),
        "SELECT t.c AS d FROM (SELECT a AS c, b FROM product) AS t");
    EXPECT_EQ(optimizeSubquery("SELECT t.a FROM (SELECT a, b FROM product) AS t"), "SELECT t.a FROM (SELECT a, b FROM product) AS t");
    EXPECT_EQ(optimizeSubquery("SELECT t.a FROM (SELECT * FROM product) AS t"), "SELECT t.a FROM (SELECT * FROM product) AS t");
    EXPECT_EQ(
        optimizeSubquery(
            "SELECT count(t.code) FROM (SELECT a.code, b.code FROM product_info a , product_info b WHERE a.productid = b.productid) AS t"),
        "SELECT count(t.code) FROM (SELECT a.code, b.code FROM product_info AS a , product_info AS b WHERE a.productid = b.productid) AS "
        "t");
    EXPECT_EQ(
        optimizeSubquery("SELECT count(t.b.code) FROM (SELECT a.code, b.code FROM product_info a , product_info b WHERE a.productid = "
                         "b.productid) AS t"),
        "SELECT count(t.b.code) FROM (SELECT a.code, b.code FROM product_info AS a , product_info AS b WHERE a.productid = b.productid) AS "
        "t");
    EXPECT_EQ(
        optimizeSubquery(
            "SELECT count(t.code) FROM (SELECT code, code FROM product_info a , product_info b WHERE a.productid = b.productid) AS t"),
        "SELECT count(t.code) FROM (SELECT code, code FROM product_info AS a , product_info AS b WHERE a.productid = b.productid) AS t");
}
