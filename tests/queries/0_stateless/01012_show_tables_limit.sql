-- Tags: no-parallel

DROP DATABASE IF EXISTS test_show_limit;

CREATE DATABASE test_show_limit;

create stream test_show_limit.test1 (test uint8) ;
create stream test_show_limit.test2 (test uint8) ;
create stream test_show_limit.test3 (test uint8) ;
create stream test_show_limit.test4 (test uint8) ;
create stream test_show_limit.test5 (test uint8) ;
create stream test_show_limit.test6 (test uint8) ;

SELECT '*** Should show 6: ***';
SHOW TABLES FROM test_show_limit;
SELECT '*** Should show 2: ***';
SHOW TABLES FROM test_show_limit LIMIT 2;
SELECT '*** Should show 4: ***';
SHOW TABLES FROM test_show_limit LIMIT 2 * 2;

DROP DATABASE test_show_limit;

