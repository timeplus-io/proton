DROP STREAM IF EXISTS repro_hits;

CREATE STREAM repro_hits ( date Date, metric float64) ENGINE = MergeTree() ORDER BY date;

-- From https://github.com/ClickHouse/ClickHouse/issues/12513#issue-657202535
SELECT date as period, 1 as having_check, min(date) as period_start, add_days(max(date), 1) as period_end, date_diff('second', period_start, period_end) as total_duration, sum(metric) as metric_ FROM repro_hits GROUP BY period HAVING having_check != -1;

SELECT min(number) as min_number FROM numbers(10) GROUP BY number HAVING 1 ORDER BY min_number;

DROP STREAM IF EXISTS repro_hits;
