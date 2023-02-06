drop stream if exists sessions;
CREATE STREAM sessions
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
    uniq(user_id) as a,  min(_sample_factor) as x,  a*x
FROM sessions
SAMPLE 10000000;
