drop stream if exists n;

create stream n(nc nullable(int)) engine = MergeTree order by (tuple()) partition by (nc) settings allow_nullable_key = 1;

insert into n values (null);

select * from n where nc is null;

drop stream n;
