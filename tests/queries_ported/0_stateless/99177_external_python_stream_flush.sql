-- Python external stream sink flush hook:
-- flush_function_name is called on every checkpoint and once on graceful close
-- (before deinit), so data buffered by the Python code is not lost.

DROP STREAM IF EXISTS py_flush_sink;

CREATE EXTERNAL STREAM py_flush_sink(val int32, flush_count int32) AS $$
import builtins

def init_99177():
    builtins._tp_99177_buffer = []
    builtins._tp_99177_flushed = getattr(builtins, '_tp_99177_flushed', [])
    builtins._tp_99177_flush_count = getattr(builtins, '_tp_99177_flush_count', 0)

def write_99177(val, flush_count):
    builtins._tp_99177_buffer.extend(val)

def flush_99177():
    builtins._tp_99177_flush_count += 1
    builtins._tp_99177_flushed.extend(builtins._tp_99177_buffer)
    builtins._tp_99177_buffer = []

def read_99177():
    flushed = getattr(builtins, '_tp_99177_flushed', [])
    count = getattr(builtins, '_tp_99177_flush_count', 0)
    return [(v, count) for v in flushed]
$$ SETTINGS type='python', read_function_name='read_99177', write_function_name='write_99177', init_function_name='init_99177', flush_function_name='flush_99177';

SHOW CREATE py_flush_sink;

-- Each finished INSERT must flush the buffered rows exactly once (before deinit)
INSERT INTO py_flush_sink(val, flush_count) VALUES (1, 0), (2, 0), (3, 0);

SELECT * FROM py_flush_sink ORDER BY val;

INSERT INTO py_flush_sink(val, flush_count) VALUES (4, 0);

SELECT * FROM py_flush_sink ORDER BY val;

DROP STREAM py_flush_sink;

-- A flush function missing from the Python module fails the INSERT
DROP STREAM IF EXISTS py_flush_missing;

CREATE EXTERNAL STREAM py_flush_missing(val int32) AS $$
def write_missing_99177(val):
    pass
$$ SETTINGS type='python', write_function_name='write_missing_99177', flush_function_name='no_such_flush_99177';

INSERT INTO py_flush_missing(val) VALUES (1); -- { serverError UDF_INTERNAL_ERROR }

DROP STREAM py_flush_missing;
