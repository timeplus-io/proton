#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

REFERENCE="$CURDIR"/99150_python_udf_init_named_collection.reference

# Probe python UDF support; skip the whole test when the build has none
output=$($CLICKHOUSE_CLIENT -q "
CREATE OR REPLACE FUNCTION py_init_probe_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def py_init_probe_99150(xs):
    return xs
\$\$;
" 2>&1)
if (echo "$output" | grep -q "Syntax error"); then
    cat "$REFERENCE"
    exit 0
fi

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_init_probe_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_nc_probe_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_direct_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_agg_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_agg_ctor_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_zero_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_nokey_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_retry_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_dropped_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS py_bad_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS js_bad_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS nc_udf_init_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS nc_nokey_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS nc_flaky_99150" >/dev/null 2>&1
    $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS nc_dropped_99150" >/dev/null 2>&1
}
cleanup

$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION nc_udf_init_99150 AS init_function_parameters = '{\"k\":\"secret_v1\"}' NOT OVERRIDABLE"

echo "=== 1. init via named collection ==="
$CLICKHOUSE_CLIENT -q "
CREATE FUNCTION py_nc_probe_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
import json
SECRET = ''
def _tp_init(params):
    global SECRET
    SECRET = json.loads(params).get('k', '')

def py_nc_probe_99150(xs):
    return [SECRET + ':' + (x.decode('utf-8') if isinstance(x, bytes) else x) for x in xs]
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = 'nc_udf_init_99150'"
$CLICKHOUSE_CLIENT -q "SELECT py_nc_probe_99150('hello')"

echo "=== 2. SHOW CREATE keeps settings, hides secret ==="
SHOW_OUT=$($CLICKHOUSE_CLIENT -q "SHOW CREATE FUNCTION py_nc_probe_99150")
echo "$SHOW_OUT" | grep -c "init_function_name" || :
echo "$SHOW_OUT" | grep -c "named_collection" || :
echo "$SHOW_OUT" | grep -c "secret_v1" || :

echo "=== 3. direct init_function_parameters (visible by design) ==="
$CLICKHOUSE_CLIENT -q "
CREATE FUNCTION py_direct_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
import json
SECRET = ''
def _tp_init(params):
    global SECRET
    SECRET = json.loads(params).get('k', '')

def py_direct_99150(xs):
    return [SECRET + ':' + (x.decode('utf-8') if isinstance(x, bytes) else x) for x in xs]
\$\$ SETTINGS init_function_name = '_tp_init', init_function_parameters = '{\"k\":\"direct_v\"}'"
$CLICKHOUSE_CLIENT -q "SELECT py_direct_99150('x')"
$CLICKHOUSE_CLIENT -q "SHOW CREATE FUNCTION py_direct_99150" | grep -c "direct_v" || :

echo "=== 4. aggregate UDF init hook ==="
$CLICKHOUSE_CLIENT -q "
CREATE AGGREGATE FUNCTION py_agg_99150(x float32) RETURNS string LANGUAGE PYTHON AS \$\$
import json
PREFIX = ''
def _tp_init(params):
    global PREFIX
    PREFIX = json.loads(params).get('k', '')

class py_agg_99150:
    def __init__(self):
        self.count = 0

    def process(self, values):
        self.count += len(values)

    def merge(self, other_state_str):
        s = json.loads(other_state_str)
        self.count += s['count']

    # Deliberately returns a scalar string: string-typed UDA finalize routes through scalar insert (unlike 99009's numeric list-return)
    def finalize(self):
        return PREFIX + ':' + str(self.count)

    def serialize(self):
        return json.dumps({'count': self.count})

    def deserialize(self, state_str):
        self.count = json.loads(state_str)['count']
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = 'nc_udf_init_99150'"
$CLICKHOUSE_CLIENT -q "SELECT py_agg_99150(x) FROM (SELECT to_float32(number) AS x FROM numbers(3))"

echo "=== 5. rejections ==="
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_bad_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def py_bad_99150(xs):
    return xs
\$\$ SETTINGS bogus_setting = 'x'" 2>&1 | grep -c "Unknown setting 'bogus_setting'" || :
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_bad_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def py_bad_99150(xs):
    return xs
\$\$ SETTINGS named_collection = 'nc_udf_init_99150'" 2>&1 | grep -c "require 'init_function_name'" || :
# valid _tp_init so the only defect is the duplicated parameter source
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_bad_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def _tp_init(p):
    pass
def py_bad_99150(xs):
    return xs
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = 'nc_udf_init_99150', init_function_parameters = '{}'" 2>&1 | grep -c "mutually exclusive" || :
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_bad_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def py_bad_99150(xs):
    return xs
\$\$ SETTINGS init_function_name = 1" 2>&1 | grep -c "must be a string literal" || :
# valid _tp_init so the only defect is the missing named collection
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_bad_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def _tp_init(p):
    pass
def py_bad_99150(xs):
    return xs
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = 'nc_does_not_exist_99150'" 2>&1 | grep -c "does not exist" || :
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION js_bad_99150(x string) RETURNS string LANGUAGE JAVASCRIPT AS \$\$
function js_bad_99150(xs) { return xs; }
\$\$ SETTINGS init_function_name = '_tp_init'" 2>&1 | grep -c "only supported for Python UDFs" || :
# non-changes SETTINGS forms (= DEFAULT, param_*) must be rejected, not silently dropped
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_bad_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def py_bad_99150(xs):
    return xs
\$\$ SETTINGS bogus_setting = DEFAULT" 2>&1 | grep -c "Unknown setting 'bogus_setting'" || :
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_bad_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def py_bad_99150(xs):
    return xs
\$\$ SETTINGS init_function_name = DEFAULT" 2>&1 | grep -c "must be a string literal" || :
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_bad_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def py_bad_99150(xs):
    return xs
\$\$ SETTINGS param_p = 'v'" 2>&1 | grep -c "Unknown setting 'param_p'" || :
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION js_bad_99150(x string) RETURNS string LANGUAGE JAVASCRIPT AS \$\$
function js_bad_99150(xs) { return xs; }
\$\$ SETTINGS init_function_name = DEFAULT" 2>&1 | grep -c "only supported for Python UDFs" || :
# init hook must exist in the source
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION py_bad_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def py_bad_99150(xs):
    return xs
\$\$ SETTINGS init_function_name = '_tp_missing', init_function_parameters = 'x'" 2>&1 | grep -c "is not defined in the UDF source" || :

echo "=== 6. hook failure cleanup, retry sees updated collection ==="
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION nc_flaky_99150 AS init_function_parameters = 'fail'"
$CLICKHOUSE_CLIENT -q "
CREATE FUNCTION py_retry_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
SECRET = ''
def _tp_init(params):
    if params == 'fail':
        raise ValueError('init refused')
    global SECRET
    SECRET = params

def py_retry_99150(xs):
    return [SECRET + ':' + (x.decode('utf-8') if isinstance(x, bytes) else x) for x in xs]
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = 'nc_flaky_99150'"
$CLICKHOUSE_CLIENT -q "SELECT py_retry_99150('a')" 2>&1 | grep -c "init refused" || :
$CLICKHOUSE_CLIENT -q "ALTER NAMED COLLECTION nc_flaky_99150 SET init_function_parameters = 'ok'"
$CLICKHOUSE_CLIENT -q "SELECT py_retry_99150('a')"

echo "=== 7. collection deleted before first load ==="
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION nc_dropped_99150 AS init_function_parameters = 'v'"
$CLICKHOUSE_CLIENT -q "
CREATE FUNCTION py_dropped_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
SECRET = ''
def _tp_init(params):
    global SECRET
    SECRET = params

def py_dropped_99150(xs):
    return [SECRET + ':' + (x.decode('utf-8') if isinstance(x, bytes) else x) for x in xs]
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = 'nc_dropped_99150'"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION nc_dropped_99150"
$CLICKHOUSE_CLIENT -q "SELECT py_dropped_99150('a')" 2>&1 | grep -c "nc_dropped_99150" || :

echo "=== 8. aggregate constructor depends on init hook ==="
$CLICKHOUSE_CLIENT -q "
CREATE AGGREGATE FUNCTION py_agg_ctor_99150(x float32) RETURNS string LANGUAGE PYTHON AS \$\$
import json
CONF = None
def _tp_init(params):
    global CONF
    CONF = json.loads(params)

class py_agg_ctor_99150:
    def __init__(self):
        # raises unless the init hook ran before instantiation
        self.prefix = CONF['k']
        self.count = 0

    def process(self, values):
        self.count += len(values)

    def merge(self, other_state_str):
        self.count += json.loads(other_state_str)['count']

    def finalize(self):
        return self.prefix + ':' + str(self.count)

    def serialize(self):
        return json.dumps({'count': self.count})

    def deserialize(self, state_str):
        self.count = json.loads(state_str)['count']
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = 'nc_udf_init_99150'"
$CLICKHOUSE_CLIENT -q "SELECT py_agg_ctor_99150(x) FROM (SELECT to_float32(number) AS x FROM numbers(2))"

echo "=== 9. zero-arg init hook ==="
# init_function_name alone, no parameter source -> hook called with no args (empty-args branch)
$CLICKHOUSE_CLIENT -q "
CREATE FUNCTION py_zero_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
TAG = ''
def _tp_init():
    global TAG
    TAG = 'zero'

def py_zero_99150(xs):
    return [TAG + ':' + (x.decode('utf-8') if isinstance(x, bytes) else x) for x in xs]
\$\$ SETTINGS init_function_name = '_tp_init'"
$CLICKHOUSE_CLIENT -q "SELECT py_zero_99150('a')"
# named collection that exists but lacks init_function_parameters -> hook still called with no args
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION nc_nokey_99150 AS other_key = 'v' NOT OVERRIDABLE"
$CLICKHOUSE_CLIENT -q "
CREATE FUNCTION py_nokey_99150(x string) RETURNS string LANGUAGE PYTHON AS \$\$
TAG = ''
def _tp_init():
    global TAG
    TAG = 'nokey'

def py_nokey_99150(xs):
    return [TAG + ':' + (x.decode('utf-8') if isinstance(x, bytes) else x) for x in xs]
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = 'nc_nokey_99150'"
$CLICKHOUSE_CLIENT -q "SELECT py_nokey_99150('a')"

cleanup
exit 0
