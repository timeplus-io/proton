-- Tags: no-replicated-database

DROP STREAM IF EXISTS null_;
DROP STREAM IF EXISTS buffer_;
DROP STREAM IF EXISTS aggregation_;

-- Each uint64  is 8    bytes
-- So 10e6 rows is 80e6 bytes
--
-- Use LIMIT max_rows+1 to force flush from the query context, and to avoid
-- flushing from the background thread, since in this case it can steal memory
-- the max_memory_usage may be exceeded during squashing other blocks.


create stream null_ (key uint64) Engine=Null();
create stream buffer_ (key uint64) Engine=Buffer(currentDatabase(), null_,
    1,    /* num_layers */
    10e6, /* min_time, placeholder */
    10e6, /* max_time, placeholder */
    0,    /* min_rows   */
    10e6, /* max_rows   */
    0,    /* min_bytes  */
    80e6  /* max_bytes  */
);

SET max_memory_usage=10e6;
SET max_block_size=100e3;

-- Check that max_memory_usage is ignored only on flush and not on squash
SET min_insert_block_size_bytes=9e6;
SET min_insert_block_size_rows=0;
INSERT INTO buffer_ SELECT to_uint64(number) FROM system.numbers LIMIT to_uint64(10e6+1); -- { serverError 241 }

OPTIMIZE STREAM buffer_; -- flush just in case

-- create complex aggregation to fail with Memory limit exceede error while writing to Buffer()
-- string over uint64 is enough to trigger the problem.
CREATE MATERIALIZED VIEW aggregation_ engine=Memory() AS SELECT to_string(key) FROM null_;

-- Check that max_memory_usage is ignored during write from StorageBuffer
SET min_insert_block_size_bytes=0;
SET min_insert_block_size_rows=100e3;
INSERT INTO buffer_ SELECT to_uint64(number) FROM system.numbers LIMIT to_uint64(10e6+1);
-- Check that 10e6 rows had been flushed from the query, not from the background worker.
SELECT count() FROM buffer_;

DROP STREAM null_;
DROP STREAM buffer_;
DROP STREAM aggregation_;
