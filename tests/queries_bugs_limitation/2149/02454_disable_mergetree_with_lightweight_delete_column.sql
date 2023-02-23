drop stream if exists t_row_exists;

create stream t_row_exists(a int, _row_exists int) engine=MergeTree order by a; --{serverError 44}

create stream t_row_exists(a int, b int) engine=MergeTree order by a;
alter stream t_row_exists add column _row_exists int; --{serverError ILLEGAL_COLUMN}
alter stream t_row_exists rename column b to _row_exists; --{serverError ILLEGAL_COLUMN}
alter stream t_row_exists rename column _row_exists to c; --{serverError NOT_FOUND_COLUMN_IN_BLOCK}
alter stream t_row_exists drop column _row_exists; --{serverError NOT_FOUND_COLUMN_IN_BLOCK}
alter stream t_row_exists drop column unknown_column; --{serverError NOT_FOUND_COLUMN_IN_BLOCK}
drop stream t_row_exists;

create stream t_row_exists(a int, _row_exists int) engine=Memory;
insert into t_row_exists values(1,1);
select * from t_row_exists;
drop stream t_row_exists;

create stream t_row_exists(a int, b int) engine=Memory;
alter stream t_row_exists add column _row_exists int; --{serverError NOT_IMPLEMENTED}
alter stream t_row_exists rename column b to _row_exists; --{serverError NOT_IMPLEMENTED}
drop stream t_row_exists;
