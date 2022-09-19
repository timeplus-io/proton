DROP STREAM IF EXISTS foo;
DROP STREAM IF EXISTS bar;
DROP STREAM IF EXISTS view_foo_bar;

create stream foo (ddate date, id int64, n string) ENGINE = ReplacingMergeTree(ddate, (id), 8192);
create stream bar (ddate date, id int64, n string, foo_id int64) ENGINE = ReplacingMergeTree(ddate, (id), 8192);
insert into bar (id, n, foo_id) values (1, 'bar_n_1', 1);
create MATERIALIZED view view_foo_bar ENGINE = ReplacingMergeTree(ddate, (bar_id), 8192) as select ddate, bar_id, bar_n, foo_id, foo_n from (select ddate, id as bar_id, n as bar_n, foo_id from bar) js1 any left join (select id as foo_id, n as foo_n from foo) js2 using foo_id;
insert into bar (id, n, foo_id) values (1, 'bar_n_1', 1);
SELECT * FROM view_foo_bar;

DROP STREAM foo;
DROP STREAM bar;
DROP STREAM view_foo_bar;
