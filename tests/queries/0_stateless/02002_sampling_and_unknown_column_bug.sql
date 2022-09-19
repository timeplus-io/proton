SET query_mode = 'table';
drop stream if exists sessions;
create stream sessions
(
  `user_id` uint64
)
ENGINE = MergeTree
ORDER BY user_id 
SAMPLE BY user_id;

insert into sessions values(1);

SELECT
    sum(user_id * _sample_factor) 
FROM sessions
SAMPLE 10000000;

SELECT
    uniq(user_id) a,  min(_sample_factor) x,  a*x
FROM sessions
SAMPLE 10000000;
