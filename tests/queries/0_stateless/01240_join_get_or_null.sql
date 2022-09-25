SET query_mode = 'table';
DROP STREAM IF EXISTS join_test;

create stream join_test (id uint16, num uint16) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500);
DROP STREAM join_test;

create stream join_test (id uint16, num nullable(uint16)) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500);
DROP STREAM join_test;

create stream join_test (id uint16, num array(uint16)) engine = Join(ANY, LEFT, id);
SELECT joinGetOrNull('join_test', 'num', 500); -- { serverError 43 }
DROP STREAM join_test;

drop stream if exists test;
create stream test (x date, y string) engine Join(ANY, LEFT, x);
insert into test values ('2017-04-01', '1396-01-12') ,('2017-04-02', '1396-01-13');

WITH
    A as (SELECT rowNumberInAllBlocks() R, addDays(to_date('2017-04-01'), R) TVV from numbers(5)),
    B as (SELECT rowNumberInAllBlocks() R, to_datetime(NULL) TVV from numbers(1))
SELECT
    joinGetOrNull('test', 'y', to_date(A.TVV) ) TV1
from A LEFT JOIN B USING (R) order by TV1;
