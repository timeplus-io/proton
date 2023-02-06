drop stream if exists t;
create stream t (x bool) engine=Memory();
insert into t settings bool_true_representation='да' values ('да');
drop stream t;
