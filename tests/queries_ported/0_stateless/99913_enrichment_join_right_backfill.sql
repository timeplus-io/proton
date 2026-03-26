-- Regression test for issue #2818:
-- Right-side keyed storage (versioned_kv / changelog_kv) must be fully backfilled
-- into the join hash table BEFORE the left stream begins processing.

-- ============================================================
-- Case 1: Right-only backfill
-- Historical data exists only on the right side. Left rows are
-- inserted AFTER the MV is created.
-- ============================================================
DROP VIEW IF EXISTS 99913_mv;
DROP STREAM IF EXISTS 99913_events;
DROP STREAM IF EXISTS 99913_labels;

CREATE STREAM 99913_events(id int, event string) SETTINGS flush_threshold_count=1;
CREATE STREAM 99913_labels(id int, label string)
    PRIMARY KEY id
    SETTINGS mode='versioned_kv', flush_threshold_count=1;

INSERT INTO 99913_labels(id, label) VALUES (1, 'alpha')(2, 'beta')(3, 'gamma');

SELECT sleep(1) FORMAT Null;

CREATE MATERIALIZED VIEW 99913_mv AS
    SELECT e.id, e.event, l.label
    FROM 99913_events AS e
    INNER JOIN 99913_labels AS l ON e.id = l.id
    STORAGE_SETTINGS flush_threshold_count=1;

-- Insert left rows immediately after MV creation to stress-test the
-- backfill ordering guarantee: right historical data must be loaded
-- into the hash table before these left rows are joined.
INSERT INTO 99913_events(id, event) VALUES (1, 'e1')(2, 'e2')(3, 'e3');

SELECT sleep(3) FORMAT Null;

SELECT id, event, label FROM table(99913_mv) ORDER BY id, event;

DROP VIEW IF EXISTS 99913_mv;
DROP STREAM IF EXISTS 99913_events;
DROP STREAM IF EXISTS 99913_labels;

-- ============================================================
-- Case 2: Both-side backfill
-- Historical data exists on BOTH sides. The MV uses
-- seek_to='earliest' so the left side also backfills.
-- ============================================================
DROP VIEW IF EXISTS 99913_mv2;
DROP STREAM IF EXISTS 99913_events2;
DROP STREAM IF EXISTS 99913_labels2;

CREATE STREAM 99913_events2(id int, event string) SETTINGS flush_threshold_count=1;
CREATE STREAM 99913_labels2(id int, label string)
    PRIMARY KEY id
    SETTINGS mode='versioned_kv', flush_threshold_count=1;

INSERT INTO 99913_labels2(id, label) VALUES (1, 'alpha')(2, 'beta')(3, 'gamma');
INSERT INTO 99913_events2(id, event) VALUES (1, 'e1')(2, 'e2')(3, 'e3');

SELECT sleep(2) FORMAT Null;

CREATE MATERIALIZED VIEW 99913_mv2 AS
    SELECT e.id, e.event, l.label
    FROM 99913_events2 AS e
    INNER JOIN 99913_labels2 AS l ON e.id = l.id
    SETTINGS seek_to='earliest'
    STORAGE_SETTINGS flush_threshold_count=1;

SELECT sleep(3) FORMAT Null;

SELECT id, event, label FROM table(99913_mv2) ORDER BY id, event;

DROP VIEW IF EXISTS 99913_mv2;
DROP STREAM IF EXISTS 99913_events2;
DROP STREAM IF EXISTS 99913_labels2;
