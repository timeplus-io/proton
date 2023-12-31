SET query_mode = 'table';
drop stream if exists t_00712_2;
create stream t_00712_2 (date date, counter uint64, sampler uint64, alias_col alias date + 1) engine = MergeTree(date, intHash32(sampler), (counter, date, intHash32(sampler)), 8192);
insert into t_00712_2 values ('2018-01-01', 1, 1);
select alias_col from t_00712_2 sample 1 / 2 where date = '2018-01-01' and counter = 1 and sampler = 1;
drop stream if exists t_00712_2;

