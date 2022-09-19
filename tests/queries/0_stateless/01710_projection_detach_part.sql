set allow_experimental_projection_optimization = 1;
SET query_mode = 'table';

drop stream if exists t;

create stream t (i int, j int, projection x (select * order by j)) engine MergeTree partition by i order by i;

insert into t values (1, 2);

alter stream t detach partition 1;

alter stream t attach partition 1;

select count() from system.projection_parts where database = currentDatabase() and table = 't';

drop stream t;
