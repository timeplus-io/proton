DROP STREAM IF EXISTS v_02964;
CREATE STREAM v_02964(id int, val int) settings flush_threshold_ms=1;
TRUNCATE STREAM v_02964;
DETACH STREAM v_02964; -- { serverError 48 }


ALTER STREAM v_02964 ADD COLUMN gen int32 FIRST; -- { clientError 62 }
ALTER STREAM v_02964 DROP COLUMN val; -- { serverError 48 }
ALTER STREAM v_02964 RENAME COLUMN val to new_val;

SELECT sleep(2) FORMAT Null;
INSERT INTO v_02964(id, new_val) values(2014, 17);

ALTER STREAM v_02964 MODIFY TTL to_datetime(now()) + INTERVAL 48 HOUR;
ALTER STREAM v_02964 REMOVE TTL;
ALTER STREAM v_02964 MODIFY SETTING logstore_retention_bytes=1000000;

INSERT INTO v_02964(id, new_val) values(2, 2);
SELECT sleep(2) FORMAT Null;
SELECT id, new_val FROM table(v_02964) where id=2014;
SELECT id, new_val FROM table(v_02964) where id=2;

alter stream v_02964 add projection p (select * order by id); -- {clientError SYNTAX_ERROR }
create stream projection_02964(id int, val projection p (select * order by id)) engine=MergeTree order by id; -- {clientError SYNTAX_ERROR }

DROP STREAM IF EXISTS v_02964;
