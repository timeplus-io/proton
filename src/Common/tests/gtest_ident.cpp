#include <iomanip>
#include <iostream>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <gtest/gtest.h>

using namespace DB;

String convertQueryToFormat(String query,bool one_line)
{
    DB::ASTPtr ast;
    const char * begin = query.data();
    const char * end = query.data() + query.size();
    ParserQuery parser(end, false);
    ast = parseQuery(parser, begin, end, "", 262144, 1000);
    return queryToString(ast, one_line);
}

TEST(indent, stream_name)
{
    /// test format 'one_line'=false;
    String query("with it1 as (with it2 as (select * from numbers(10)) select * from it2) select user.id from user,it1 where it1.number=user.id");
    String expected_ans("WITH it1 AS\n  (\n    WITH it2 AS\n      (\n        SELECT\n          *\n        FROM\n          numbers(10)\n      )\n    SELECT\n      *\n    FROM\n      it2\n  )\nSELECT\n  user.id\nFROM\n  user, it1\nWHERE\n  it1.number = user.id");
    String ans = convertQueryToFormat(query,false);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

    /// test DISTINCT  JOIN  WHERE  PARTITION BY  GROUP BY  HAVING ORDER BY  LIMIT  EMIT  SETTINGS
    query = "with it1 as (select * from numbers(10)) select distinct user.id from user join it1 on user.id=it1.number group by user.id having id>0 order by id limit 10 emit last 1s settings query_mode='stream'";
    expected_ans = "WITH it1 AS\n  (\n    SELECT\n      *\n    FROM\n      numbers(10)\n  )\nSELECT DISTINCT\n  user.id\nFROM\n  user\nINNER JOIN it1 ON user.id = it1.number\nGROUP BY\n  user.id\nHAVING\n  id > 0\nORDER BY\n  id ASC\nLIMIT 10\nEMIT STREAM LAST 1s\nSETTINGS\n  query_mode = 'stream'";
    ans = convertQueryToFormat(query,false);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

    /// test format 'one_line'=true;
    query = "with it1 as (with it2 as (select * from numbers(10)) select * from it2) select user.id from user,it1 where it1.number=user.id";
    expected_ans = "WITH it1 AS (WITH it2 AS (SELECT * FROM numbers(10)) SELECT * FROM it2) SELECT user.id FROM user, it1 WHERE it1.number = user.id";
    ans = convertQueryToFormat(query,true);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

    query = "with it1 as (select * from numbers(10)) select distinct user.id from user join it1 on user.id=it1.number group by user.id having id>0 order by id limit 10 emit last 1s settings query_mode='stream'";
    expected_ans = "WITH it1 AS (SELECT * FROM numbers(10)) SELECT DISTINCT user.id FROM user INNER JOIN it1 ON user.id = it1.number GROUP BY user.id HAVING id > 0 ORDER BY id ASC LIMIT 10 EMIT STREAM LAST 1s SETTINGS query_mode = 'stream'";
    ans = convertQueryToFormat(query,true);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

}

TEST(indent, complex_query)
{
    /// Covering: WITH  SELECT  DISTINCT  FROM  JOIN  WHERE  PARTITION BY  SHUFFLE BY  GROUP BY  HAVING  ORDER BY  LIMIT  EMIT  SETTINGS UNION
    /// test format 'one_line'=false;
    String query = "with it1 as (select * from numbers(10)), it2 as (select * from it1) select distinct user.id from user join it1 on user.id=it1.number array join it2.number as number where number > 1 or number < 5 partition by user.id group by user.id having id>0 order by id limit 10 emit last 1s settings query_mode='stream' union select * from it2";
    String expected_ans = """WITH it1 AS\n  (\n    SELECT\n      *\n    FROM\n      numbers(10)\n  ), it2 AS\n  (\n    SELECT\n      *\n    FROM\n      it1\n  )\nSELECT DISTINCT\n  user.id\nFROM\n  user\nINNER JOIN it1 ON user.id = it1.number\nARRAY JOIN it2.number AS number\nWHERE\n  (number > 1) OR (number < 5)\nPARTITION BY\n  user.id\nGROUP BY\n  user.id\nHAVING\n  id > 0\nORDER BY\n  id ASC\nLIMIT 10\nEMIT STREAM LAST 1s\nSETTINGS\n  query_mode = 'stream'\nUNION\nSELECT\n  *\nFROM\n  it2";
    String ans = convertQueryToFormat(query,false);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

    /// test format 'one_line'=true;
    expected_ans = "WITH it1 AS (SELECT * FROM numbers(10)), it2 AS (SELECT * FROM it1) SELECT DISTINCT user.id FROM user INNER JOIN it1 ON user.id = it1.number ARRAY JOIN it2.number AS number WHERE (number > 1) OR (number < 5) PARTITION BY user.id GROUP BY user.id HAVING id > 0 ORDER BY id ASC LIMIT 10 EMIT STREAM LAST 1s SETTINGS query_mode = 'stream' UNION SELECT * FROM it2";
    ans = convertQueryToFormat(query,true);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());
}

TEST(indent, join_query)
{
    /// Covering: JOIN, INNER JOIN, LEFT JOIN, ASOF JOIN, LEFT ASOF JOIN, INNER ASOF JOIN, LATEST JOIN, LEFT LATEST JOIN, INNER LATEST JOIN, ARRAY JOIN
    /// test format 'one_line'=false;
    String query = "with tmp as (select * from t1 join t2 using(id) inner join t3 on t1.id=t3.id left join t4 on t1.id = t4.id asof join t as t5 on t1.id=t5.id and t1.time < t5.time array join t1.time as time, t2.time as time2 inner asof join (select * from t6) as t6 on t1.id=t2.id and t1.time < t6.time left asof join t7 on t1.id=t7.id and t1.time > t7.time latest join t8 using(id) inner latest join t9 on t1.id=t9.id left latest join cte as t10 on t1.id=t10.id) select * from tmp";
    String expected_ans = "WITH tmp AS\n  (\n    SELECT\n      *\n    FROM\n      t1\n    INNER JOIN t2 USING (id)\n    INNER JOIN t3 ON t1.id = t3.id\n    LEFT JOIN t4 ON t1.id = t4.id\n    ASOF INNER JOIN t AS t5 ON (t1.id = t5.id) AND (t1.time < t5.time)\n    ARRAY JOIN t1.time AS time, t2.time AS time2\n    ASOF INNER JOIN (\n        SELECT\n          *\n        FROM\n          t6\n      ) AS t6 ON (t1.id = t2.id) AND (t1.time < t6.time)\n    ASOF LEFT JOIN t7 ON (t1.id = t7.id) AND (t1.time > t7.time)\n    LATEST INNER JOIN t8 USING (id)\n    LATEST INNER JOIN t9 ON t1.id = t9.id\n    LATEST LEFT JOIN cte AS t10 ON t1.id = t10.id\n  )\nSELECT\n  *\nFROM\n  tmp";
    String ans = convertQueryToFormat(query,false);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

    /// test format 'one_line'=true;
    expected_ans = "WITH tmp AS (SELECT * FROM t1 INNER JOIN t2 USING (id) INNER JOIN t3 ON t1.id = t3.id LEFT JOIN t4 ON t1.id = t4.id ASOF INNER JOIN t AS t5 ON (t1.id = t5.id) AND (t1.time < t5.time) ARRAY JOIN t1.time AS time, t2.time AS time2 ASOF INNER JOIN (SELECT * FROM t6) AS t6 ON (t1.id = t2.id) AND (t1.time < t6.time) ASOF LEFT JOIN t7 ON (t1.id = t7.id) AND (t1.time > t7.time) LATEST INNER JOIN t8 USING (id) LATEST INNER JOIN t9 ON t1.id = t9.id LATEST LEFT JOIN cte AS t10 ON t1.id = t10.id) SELECT * FROM tmp";
    ans = convertQueryToFormat(query,true);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());
}