DROP STREAM IF EXISTS join_test;

CREATE STREAM join_test (id uint16, num uint16) engine = Join(ANY, LEFT, id);
SELECT join_get_or_null('join_test', 'num', 500);
DROP STREAM join_test;

CREATE STREAM join_test (id uint16, num nullable(uint16)) engine = Join(ANY, LEFT, id);
SELECT join_get_or_null('join_test', 'num', 500);
DROP STREAM join_test;

CREATE STREAM join_test (id uint16, num array(uint16)) engine = Join(ANY, LEFT, id);
SELECT join_get_or_null('join_test', 'num', 500);
DROP STREAM join_test;

drop stream if exists test;
create stream test (x Date, y string) engine Join(ANY, LEFT, x);
insert into test values ('2017-04-01', '1396-01-12') ,('2017-04-02', '1396-01-13');

WITH
    A as (SELECT rowNumberInAllBlocks() R, addDays(toDate('2017-04-01'), R) TVV from numbers(5)),
    B as (SELECT rowNumberInAllBlocks() R, to_datetime(NULL) TVV from numbers(1))
SELECT
    join_get_or_null('test', 'y', toDate(A.TVV) ) TV1
from A LEFT JOIN B USING (R) order by TV1;
