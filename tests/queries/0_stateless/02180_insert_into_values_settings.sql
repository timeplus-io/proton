SET query_mode = 'table';
drop stream if exists t;
create stream t (x Bool) engine=Memory();
insert into t values settings bool_true_representation='да' ('да');
drop stream t;
