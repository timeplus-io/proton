#!/usr/bin/env bash
# Tags: no-parallel
#
# Regression test for the NAMED_COLLECTION authorization property of Python UDF
# init hooks (timeplus-io/proton#1195). A user with CREATE FUNCTION but no
# NAMED_COLLECTION grant must be rejected with ACCESS_DENIED when a Python UDF
# references a named collection. The access check must run BEFORE the existence
# check, so a nonexistent collection also yields ACCESS_DENIED (no collection-name
# probing) and the collection's contents never leak. After the grant, creation
# succeeds and the init hook resolves the collection at load time.
#
# The check lives at the shared creation choke point
# Streaming::createUserDefinedFunctionRequest (src/Functions/UserDefined/UDFHelper.cpp);
# functional coverage is in 99150. Mirrors 99120 for external streams.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

REFERENCE="$CUR_DIR"/99121_python_udf_init_named_collection_acl.reference

# Skip the whole test when the build has no Python UDF support.
probe=$(${CLICKHOUSE_CLIENT} -q "
CREATE OR REPLACE FUNCTION py_acl_probe_99121(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def py_acl_probe_99121(xs):
    return xs
\$\$;
" 2>&1)
if (echo "$probe" | grep -q "Syntax error"); then
    cat "$REFERENCE"
    exit 0
fi
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS py_acl_probe_99121" >/dev/null 2>&1

nc_name="nc_acl_99121"
user="u_acl_99121"
func="f_acl_99121"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${func}" >/dev/null 2>&1
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}" >/dev/null 2>&1
    ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${nc_name}" >/dev/null 2>&1
}
cleanup

# Admin: a secret-bearing collection and a low-privilege user that can create
# functions but has no NAMED_COLLECTION grant.
${CLICKHOUSE_CLIENT} -mn -q "
CREATE NAMED COLLECTION ${nc_name} AS init_function_parameters = '{\"k\":\"acl_secret_99121\"}' NOT OVERRIDABLE;
CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'pw';
GRANT CREATE FUNCTION, DROP FUNCTION ON *.* TO ${user};
"

# ${CLICKHOUSE_CLIENT} bakes in --user, so use the bare binary with an explicit
# port (run-sql-tests.sh runs on a non-default port) for the low-privilege user.
run_as_user() {
    ${CLICKHOUSE_CLIENT_BINARY} --port="${CLICKHOUSE_PORT_TCP}" --user="${user}" --password='pw' "$@"
}

echo "=== 1. ungranted user + existing collection -> ACCESS_DENIED, secret never leaks ==="
run_as_user -q "CREATE FUNCTION ${func}(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def _tp_init(p):
    pass
def ${func}(xs):
    return xs
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = '${nc_name}'" 2>&1 | grep -oE 'ACCESS_DENIED|acl_secret_99121' | sort -u

echo "=== 2. ungranted user + nonexistent collection -> ACCESS_DENIED, not 'does not exist' ==="
run_as_user -q "CREATE FUNCTION ${func}(x string) RETURNS string LANGUAGE PYTHON AS \$\$
def _tp_init(p):
    pass
def ${func}(xs):
    return xs
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = 'nc_absent_99121'" 2>&1 | grep -oE 'ACCESS_DENIED|does not exist' | sort -u

echo "=== 3. after GRANT NAMED COLLECTION, creation succeeds and the hook runs ==="
${CLICKHOUSE_CLIENT} -q "GRANT NAMED COLLECTION ON *.* TO ${user}"
run_as_user -q "CREATE FUNCTION ${func}(x string) RETURNS string LANGUAGE PYTHON AS \$\$
import json
SECRET = ''
def _tp_init(p):
    global SECRET
    SECRET = json.loads(p)['k']
def ${func}(xs):
    return [SECRET + ':' + (x.decode('utf-8') if isinstance(x, bytes) else x) for x in xs]
\$\$ SETTINGS init_function_name = '_tp_init', named_collection = '${nc_name}'"
${CLICKHOUSE_CLIENT} -q "SELECT ${func}('hi')"

cleanup
exit 0
