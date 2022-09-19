-- No virtual columns should be output in DESC STREAM query.

DROP STREAM IF EXISTS upyachka;
create stream upyachka (x uint64) ;

-- Merge table has virtual column `_table`
DESC STREAM merge(currentDatabase(), 'upyachka');

DROP STREAM upyachka;
