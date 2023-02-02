DROP STREAM IF EXISTS file_engine_table;

CREATE STREAM file_engine_table (id uint32) ENGINE=File(TSV);

SELECT * FROM file_engine_table; --{ serverError 107 }

SET engine_file_empty_if_not_exists=0;

SELECT * FROM file_engine_table; --{ serverError 107 }

SET engine_file_empty_if_not_exists=1;

SELECT * FROM file_engine_table;

SET engine_file_empty_if_not_exists=0;
DROP STREAM file_engine_table;
