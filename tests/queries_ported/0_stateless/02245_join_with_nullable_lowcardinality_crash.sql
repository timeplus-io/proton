drop stream if exists with_nullable;
drop stream if exists without_nullable;

CREATE STREAM with_nullable
( timestamp uint32,
  country low_cardinality(nullable(string)) ) ENGINE = Memory;

CREATE STREAM  without_nullable
( timestamp uint32,
  country low_cardinality(string)) ENGINE = Memory;

insert into with_nullable values(0,'f'),(0,'usa');
insert into without_nullable values(0,'usa'),(0,'us2a');

select if(t0.country is null ,t2.country,t0.country) as "country" 
from without_nullable as t0 right outer join with_nullable as t2 on t0.country=t2.country;

drop stream with_nullable;
drop stream without_nullable;

