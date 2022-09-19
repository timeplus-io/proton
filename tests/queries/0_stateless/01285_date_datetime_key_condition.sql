DROP STREAM IF EXISTS date_datetime_key_condition;

create stream date_datetime_key_condition (dt DateTime) ENGINE = MergeTree() ORDER BY dt;
INSERT INTO date_datetime_key_condition VALUES ('2020-01-01 00:00:00'), ('2020-01-01 10:00:00'), ('2020-01-02 00:00:00');

-- partial
SELECT group_array(dt) from date_datetime_key_condition WHERE dt > to_date('2020-01-01') AND dt < to_date('2020-01-02');
SELECT group_array(dt) from date_datetime_key_condition WHERE dt >= to_date('2020-01-02');
SELECT group_array(dt) from date_datetime_key_condition WHERE dt < to_date('2020-01-02');

-- inside
SELECT group_array(dt) from date_datetime_key_condition WHERE dt > to_date('2019-01-02');
SELECT group_array(dt) from date_datetime_key_condition WHERE dt < to_date('2021-01-02');
SELECT group_array(dt) from date_datetime_key_condition WHERE dt >= to_date('2019-01-02') AND dt < to_date('2021-01-02');
SELECT group_array(dt) from date_datetime_key_condition WHERE dt > to_date('2019-01-02') OR dt <= to_date('2021-01-02');

-- outside
SELECT group_array(dt) from date_datetime_key_condition WHERE dt < to_date('2019-01-02');
SELECT group_array(dt) from date_datetime_key_condition WHERE dt > to_date('2021-01-02');
SELECT group_array(dt) from date_datetime_key_condition WHERE dt < to_date('2019-01-02') OR dt > to_date('2021-01-02');

DROP STREAM date_datetime_key_condition;
