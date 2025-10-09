-- Tests for CREATE queries without ON CLUSTER (should succeed)
DROP STREAM IF EXISTS test_table;
select sleep(1) FORMAT Null;
CREATE STREAM IF NOT EXISTS test_table (id int32, name string);
select sleep(1) FORMAT Null;
CREATE DICTIONARY IF NOT EXISTS test_dict (id int32, name string) PRIMARY KEY id SOURCE(TIMEPLUS(HOST 'localhost' PORT 8463 STREAM 'source_table' USER 'default')) LIFETIME(MIN 0 MAX 1000) LAYOUT(FLAT());
select sleep(1) FORMAT Null;
CREATE VIEW IF NOT EXISTS test_view AS SELECT * FROM test_table;
select sleep(1) FORMAT Null;
CREATE MATERIALIZED VIEW IF NOT EXISTS test_mat_view AS SELECT * FROM test_table;
select sleep(1) FORMAT Null;

-- Tests for CREATE queries with ON CLUSTER (should fail with syntax error)
CREATE STREAM test_table_cluster ON CLUSTER 'test_cluster' (id int32, name string); -- { clientError SYNTAX_ERROR }
select sleep(1) FORMAT Null;
CREATE DATABASE IF NOT EXISTS test_db_cluster ON CLUSTER 'test_cluster'; -- { clientError SYNTAX_ERROR }
select sleep(1) FORMAT Null;
CREATE DICTIONARY test_dict_cluster ON CLUSTER 'test_cluster' (id int32) PRIMARY KEY id; -- { clientError SYNTAX_ERROR }
select sleep(1) FORMAT Null;
CREATE VIEW test_view_cluster ON CLUSTER 'test_cluster' AS SELECT * FROM test_table; -- { clientError SYNTAX_ERROR }
select sleep(1) FORMAT Null;

-- Tests for DROP queries without ON CLUSTER (should succeed)
DROP VIEW IF EXISTS test_view;
DROP VIEW IF EXISTS test_mat_view;
DROP STREAM IF EXISTS test_table_new;
DROP DATABASE IF EXISTS test_db CASCADE;
DROP DICTIONARY IF EXISTS test_dict;

-- Tests for DROP queries with ON CLUSTER (should fail with syntax error)
DROP STREAM test_table ON CLUSTER 'test_cluster'; -- { clientError SYNTAX_ERROR }
DROP DATABASE test_db ON CLUSTER 'test_cluster'; -- { clientError SYNTAX_ERROR }
DROP DICTIONARY test_dict ON CLUSTER 'test_cluster'; -- { clientError SYNTAX_ERROR }

-- Tests for ALTER queries with ON CLUSTER (should fail with syntax error)
ALTER STREAM test_table ON CLUSTER 'test_cluster' ADD COLUMN new_col int32; -- { clientError SYNTAX_ERROR }
select sleep(1) FORMAT Null;

-- Tests for ALTER queries without ON CLUSTER (should succeed)
ALTER STREAM test_table ADD COLUMN new_col int32;
select sleep(1) FORMAT Null;

-- Tests for OPTIMIZE queries without ON CLUSTER (should succeed)
OPTIMIZE STREAM test_table;
select sleep(1) FORMAT Null;

-- Tests for OPTIMIZE queries with ON CLUSTER (should fail with syntax error)
OPTIMIZE STREAM test_table ON CLUSTER 'test_cluster'; -- { clientError SYNTAX_ERROR }
select sleep(1) FORMAT Null;

-- Tests for RENAME queries without ON CLUSTER (should succeed)
RENAME STREAM test_table TO test_table_new;
select sleep(1) FORMAT Null;

-- Tests for RENAME queries with ON CLUSTER (should fail with syntax error)
RENAME STREAM test_table TO test_table_new ON CLUSTER 'test_cluster'; -- { clientError SYNTAX_ERROR }
select sleep(1) FORMAT Null;


-- Tests for KILL queries without/with ON CLUSTER
KILL QUERY WHERE query_id='test-id';
KILL QUERY ON CLUSTER 'test_cluster' WHERE query_id='test-id'; -- { clientError SYNTAX_ERROR }

-- Tests for lowercase type definitions (should succeed)
CREATE STREAM test_types (
    col1 int32,
    col2 int64,
    col3 string,
    col4 float32,
    col5 float64,
    col6 datetime,
    col7 date,
    col8 bool,
    col9 array(int32),
    col10 nullable(string)
);
