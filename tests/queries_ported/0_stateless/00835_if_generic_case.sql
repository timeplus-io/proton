SELECT to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, to_date('2000-01-02') AS y, x > y ? x : y AS z;
SELECT materialize(to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, to_date('2000-01-02') AS y, x > y ? x : y AS z;
SELECT to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, materialize(to_date('2000-01-02')) AS y, x > y ? x : y AS z;
SELECT materialize(to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, materialize(to_date('2000-01-02')) AS y, x > y ? x : y AS z;

SELECT to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, to_date('2000-01-02') AS y, 0 ? x : y AS z;
SELECT materialize(to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, to_date('2000-01-02') AS y, 0 ? x : y AS z;
SELECT to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, materialize(to_date('2000-01-02')) AS y, 0 ? x : y AS z;
SELECT materialize(to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, materialize(to_date('2000-01-02')) AS y, 0 ? x : y AS z;

SELECT to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, to_date('2000-01-02') AS y, 1 ? x : y AS z;
SELECT materialize(to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, to_date('2000-01-02') AS y, 1 ? x : y AS z;
SELECT to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul') AS x, materialize(to_date('2000-01-02')) AS y, 1 ? x : y AS z;
SELECT materialize(to_datetime('2000-01-01 00:00:00', 'Asia/Istanbul')) AS x, materialize(to_date('2000-01-02')) AS y, 1 ? x : y AS z;

SELECT rand() % 2 = 0 ? number : number FROM numbers(5);

SELECT rand() % 2 = 0 ? number : to_string(number) FROM numbers(5); -- { serverError 386 }
