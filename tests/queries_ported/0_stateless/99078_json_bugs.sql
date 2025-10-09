--- https://github.com/timeplus-io/proton-enterprise/issues/9344
drop stream if exists 99078_stream;
create stream 99078_stream(actor string, created_at datetime, id string, payload json, repo string, type string);

select sleep(2) format Null;
insert into 99078_stream(actor, created_at, id, payload, repo, type) values('t1', now(), 'id', '{"a": 1}', 'repo', 'WatchEvent');
insert into 99078_stream(actor, created_at, id, payload, repo, type) values('t2', now(), 'id', '{"b": 2}', 'repo2', 'WatchEvent');
insert into 99078_stream(actor, created_at, id, payload, repo, type) values('t3', now(), 'id', '{}', 'repo3', 'WatchEvent');

SELECT
  repo, count_distinct(actor) AS new_followers, emit_version()
FROM
  99078_stream
WHERE
  (type = 'WatchEvent') AND (_tp_time > (now() - 1h))
GROUP BY
  repo
ORDER BY
  repo
LIMIT 3
SETTINGS
  seek_to = 'earliest';

drop stream if exists 99078_stream;
