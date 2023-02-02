drop stream if exists enum;
create stream enum engine MergeTree order by enum as select cast(1, 'enum8(\'zero\'=0, \'one\'=1)') AS enum;
select * from enum where enum = 1;
drop stream if exists enum;
