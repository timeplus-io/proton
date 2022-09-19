DROP STREAM IF EXISTS default;

create stream default (d date DEFAULT to_date(t), t DateTime) ENGINE = MergeTree(d, t, 8192);
INSERT INTO default (t) VALUES ('1234567890');
SELECT to_start_of_month(d), to_uint32(t) FROM default;

DROP STREAM default;
