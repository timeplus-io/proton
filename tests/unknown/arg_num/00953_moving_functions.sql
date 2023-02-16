DROP STREAM IF EXISTS moving_sum_num;
DROP STREAM IF EXISTS moving_sum_dec;

create stream moving_sum_num (
  k string,
  dt datetime,
  v uint64
)
ENGINE = MergeTree ORDER BY (k, dt);

INSERT INTO moving_sum_num
  SELECT 'b' as k, to_datetime('2001-02-03 00:00:00')+number as dt, number as v
  FROM system.numbers
  LIMIT 5
  UNION ALL
  SELECT 'a' as k, to_datetime('2001-02-03 00:00:00')+number as dt, number as v
  FROM system.numbers
  LIMIT 5;

INSERT INTO moving_sum_num
  SELECT 'b' as k, to_datetime('2001-02-03 01:00:00')+number as dt, 5+number as v
  FROM system.numbers
  LIMIT 5;

SELECT * FROM moving_sum_num ORDER BY k,dt FORMAT TabSeparatedWithNames;

SELECT k, group_array_moving_sum(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;
SELECT k, group_array_moving_sum(3)(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;

SELECT k, group_array_moving_avg(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;
SELECT k, group_array_moving_avg(3)(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;

create stream moving_sum_dec  AS
  SELECT k, dt, to_decimal64(v, 2) as v
  FROM moving_sum_num;

SELECT k, group_array_moving_sum(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;
SELECT k, group_array_moving_sum(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;

DROP STREAM moving_sum_dec;
DROP STREAM moving_sum_num;


