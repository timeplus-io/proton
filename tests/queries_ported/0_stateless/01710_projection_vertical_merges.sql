-- Tags: long, no-parallel

drop stream if exists t;

create stream t (c1 int64, c2 string, c3 DateTime, c4 int8, c5 string, c6 string, c7 string, c8 string, c9 string, c10 string, c11 string, c12 string, c13 int8, c14 int64, c15 string, c16 string, c17 string, c18 int64, c19 int64, c20 int64) engine MergeTree order by c18;

insert into t (c1, c18) select number, -number from numbers(2000000);

alter stream t add projection p_norm (select * order by c1);

optimize table t final;

alter stream t materialize projection p_norm settings mutations_sync = 1;

set allow_experimental_projection_optimization = 1, max_rows_to_read = 3;

select c18 from t where c1 < 0;

drop stream t;
