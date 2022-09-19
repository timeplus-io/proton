-- Tags: no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;
SET query_mode = 'table';
drop stream IF EXISTS lv;

CREATE LIVE VIEW lv AS SELECT 1;

SELECT * FROM lv;

drop stream lv;
