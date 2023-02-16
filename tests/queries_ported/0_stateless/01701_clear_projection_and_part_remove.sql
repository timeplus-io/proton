drop stream if exists tp_1;
-- In this test, we are going to create an old part with written projection which does not exist in stream metadata
create stream tp_1 (x int32, y int32, projection p (select x, y order by x)) engine = MergeTree order by y partition by int_div(y, 100) settings old_parts_lifetime=1;
insert into tp_1 select number, number from numbers(3);
set mutations_sync = 2;
alter stream tp_1 add projection pp (select x, count() group by x);
insert into tp_1 select number, number from numbers(4);
-- Here we have a part with written projection pp
alter stream tp_1 detach partition '0';
-- Move part to detached
alter stream tp_1 clear projection pp;
-- Remove projection from stream metadata
alter stream tp_1 drop projection pp;
-- Now, we don't load projection pp for attached part, but it is written on disk
alter stream tp_1 attach partition '0';
-- Make this part obsolete
optimize table tp_1 final;
-- Now, DROP STREAM triggers part removal
drop stream tp_1;
