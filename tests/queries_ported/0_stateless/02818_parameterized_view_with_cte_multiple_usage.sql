drop view if exists test_param_view;
drop view if exists test_param_view2;

create view test_param_view as
with {param_test_val:uint8} as param_test_val
select param_test_val,
       array_count((a)->(a < param_test_val), t.arr) as cnt1
from (select [1,2,3,4,5] as arr) t;

SELECT sleep(1) FORMAT Null;

select * from test_param_view(param_test_val = 3);

create view test_param_view2 as
with {param_test_val:uint8} as param_test_val
select param_test_val,
       array_count((a)->(a < param_test_val), t.arr) as cnt1,
       array_count((a)->(a < param_test_val+1), t.arr) as cnt2
from (select [1,2,3,4,5] as arr) t;

SELECT sleep(1) FORMAT Null;

select * from test_param_view2(param_test_val = 3);

drop view if exists test_param_view;
drop view if exists test_param_view2;
