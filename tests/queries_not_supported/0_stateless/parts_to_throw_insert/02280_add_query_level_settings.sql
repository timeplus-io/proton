DROP STREAM IF EXISTS stream_for_alter;

CREATE STREAM stream_for_alter (
  id uint64,
  Data string
) ENGINE = MergeTree() ORDER BY id SETTINGS parts_to_throw_insert = 1, parts_to_delay_insert = 1;

INSERT INTO stream_for_alter VALUES (1, '1');
INSERT INTO stream_for_alter VALUES (2, '2'); -- { serverError 252 }

INSERT INTO stream_for_alter settings parts_to_throw_insert = 100, parts_to_delay_insert = 100 VALUES (2, '2');

INSERT INTO stream_for_alter VALUES (3, '3'); -- { serverError 252 }

ALTER STREAM stream_for_alter MODIFY SETTING parts_to_throw_insert = 100, parts_to_delay_insert = 100;

INSERT INTO stream_for_alter VALUES (3, '3');
