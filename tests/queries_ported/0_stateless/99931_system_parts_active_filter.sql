-- Filtering system.parts on the 'active' column must work for streams.
-- Regression: StoragesInfo::getParts queried the StorageStream itself (which holds no
-- parts) instead of the shard's internal storage on the active-only path, so any WHERE
-- that filtered out the active=0 virtual row returned an empty result.

DROP STREAM IF EXISTS 99931_parts_active;
CREATE STREAM 99931_parts_active(v int);

INSERT INTO 99931_parts_active(v, _tp_time) VALUES (1, '2025-01-01 00:00:00');
SELECT sleep(3) FORMAT Null;

SELECT count() FROM system.parts WHERE database = current_database() AND table = '99931_parts_active';
SELECT count() FROM system.parts WHERE database = current_database() AND table = '99931_parts_active' AND active;
SELECT count() FROM system.parts WHERE database = current_database() AND table = '99931_parts_active' AND active = 1;
SELECT count() FROM system.parts WHERE database = current_database() AND table = '99931_parts_active' AND active = 0;

DROP STREAM 99931_parts_active;
