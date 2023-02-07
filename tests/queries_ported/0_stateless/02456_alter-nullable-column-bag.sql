DROP STREAM IF EXISTS column_modify_test;

CREATE STREAM column_modify_test (id uint64, val string, other_col uint64) engine=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=0;
INSERT INTO column_modify_test VALUES (1,'one',0);
INSERT INTO column_modify_test VALUES (2,'two',0);

-- on 21.9 that was done via mutations mechanism
ALTER STREAM column_modify_test MODIFY COLUMN val nullable(string);

-- but since 21.10 it only applies that to new part, so old parts keep the old schema
--SELECT * FROM system.mutations;

INSERT INTO column_modify_test VALUES (3,Null,0);

--select name, path, type, active, modification_time from system.parts_columns where stream='column_modify_test' and column='val';

-- till now everythings looks ok
--SELECT * FROM column_modify_test;

-- now we do mutation. It will affect one of the parts
-- and it what part it will update columns.txt to the latest 'correct' state w/o updating the column file!
alter stream column_modify_test update other_col=1 where id = 1 SETTINGS mutations_sync=1;

-- row 1 is damaged now: the column files of val columns & columns.txt is out of sync!
SELECT *, throw_if(val <> 'one') FROM column_modify_test WHERE id = 1 FORMAT CSV;

