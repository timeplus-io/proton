drop stream if exists test;

create stream test(dim1 string, dim2 string, projection p1 (select dim1, dim2, count() group by dim1, dim2)) engine MergeTree order by dim1;

insert into test values ('a', 'x') ('a', 'y') ('b', 'x') ('b', 'y');

select dim1, dim2, count() from test group by grouping sets ((dim1, dim2), dim1) order by dim1, dim2, count();

select dim1, dim2, count() from test group by dim1, dim2 with rollup order by dim1, dim2, count();

select dim1, dim2, count() from test group by dim1, dim2 with cube order by dim1, dim2, count();

select dim1, dim2, count() from test group by dim1, dim2 with totals order by dim1, dim2, count();

drop stream test;
