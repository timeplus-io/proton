drop view if exists 99070_mv;
drop view if exists 99070_mv2;
drop stream if exists 99070_car;

create stream 99070_car(id int64, speed float64);
create materialized view 99070_mv as SELECT id, avg(speed) as avg_speed, window_start as win_start, window_end as win_end FROM session(99070_car, 1h, [speed >= 60, speed < 60)) GROUP BY id, window_start, window_end;
create materialized view 99070_mv2 as SELECT id, avg(speed) as avg_speed, window_start as win_start, window_end as win_end FROM session(99070_car, 1h, [speed >= 60, speed < 60)) PARTITION BY id GROUP BY id, window_start, window_end;

select sleep(2) FORMAT Null;
--- Open a session
INSERT INTO 99070_car(id, speed, _tp_time) values(10, 60, '2025-05-22 00:00:00.111');

--- Close/Open/Close multiple sessions with same timestamp
INSERT INTO 99070_car(id, speed, _tp_time) values(10, 63, '2025-05-22 00:00:00.222')(10, 50, '2025-05-22 00:00:00.222')(10, 62, '2025-05-22 00:00:00.222')(10, 55, '2025-05-22 00:00:00.222')(10, 66, '2025-05-22 00:00:00.222')(10, 59, '2025-05-22 00:00:00.222');

select sleep(3) FORMAT Null;
select * except _tp_time from table(99070_mv) order by _tp_time, avg_speed asc;
select * except _tp_time from table(99070_mv2) order by _tp_time, avg_speed asc;

drop view 99070_mv;
drop view 99070_mv2;
drop stream 99070_car;
