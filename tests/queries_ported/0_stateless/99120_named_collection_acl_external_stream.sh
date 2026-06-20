#!/usr/bin/env bash
# Tags: no-parallel
#
# Regression test: a user with CREATE TABLE on a database but no
# NAMED_COLLECTION grant must NOT be able to exfiltrate a named-collection's
# contents by referencing it from an external_stream they create.
#
# Before the fix, updateSettingsByNamedCollection() in
# src/Storages/NamedCollectionsHelpers.cpp skipped the
# context->checkAccess(AccessType::NAMED_COLLECTION, ...) that its sibling
# helpers in the same file already performed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

nc_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_creds"
user="${CLICKHOUSE_TEST_UNIQUE_NAME}_attacker"
stream="${CLICKHOUSE_TEST_UNIQUE_NAME}_exfil"

# Admin set-up: a secret-bearing collection and a low-privilege user.
# DROP STREAM on the bypass-probe names guards against a sticky leftover
# from a pre-fix binary; on the fixed binary none of them ever exist.
${CLICKHOUSE_CLIENT} -mn --query "
DROP STREAM IF EXISTS ${CLICKHOUSE_DATABASE}.${stream};
DROP STREAM IF EXISTS ${CLICKHOUSE_DATABASE}.${stream}_attach_es;
DROP STREAM IF EXISTS ${CLICKHOUSE_DATABASE}.${stream}_attach_tbl;
DROP STREAM IF EXISTS ${CLICKHOUSE_DATABASE}.${stream}_via_file;
DROP NAMED COLLECTION IF EXISTS ${nc_name};
DROP USER IF EXISTS ${user};

CREATE NAMED COLLECTION ${nc_name} AS
  init_function_parameters = '{\"access_key_id\":\"SHOULD_NOT_LEAK\",\"secret_access_key\":\"SHOULD_NOT_LEAK\"}'
  NOT OVERRIDABLE;

CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'pw';
GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${user};
"

# The attacker references the named collection from a Python external stream
# they create. The CREATE EXTERNAL STREAM call should be rejected with
# ACCESS_DENIED because the user has no NAMED_COLLECTION grant.
#
# Note: ${CLICKHOUSE_CLIENT} bakes in a --user from CLICKHOUSE_USER; using
# the bare ${CLICKHOUSE_CLIENT_BINARY} avoids "option '--user' cannot be
# specified more than once".
CLIENT_CONN_OPTS=(${CLICKHOUSE_HOST:+--host="${CLICKHOUSE_HOST}"} ${CLICKHOUSE_PORT_TCP:+--port="${CLICKHOUSE_PORT_TCP}"})
${CLICKHOUSE_CLIENT_BINARY} "${CLIENT_CONN_OPTS[@]}" --database="${CLICKHOUSE_DATABASE}" --user="${user}" --password='pw' --query "
CREATE EXTERNAL STREAM ${CLICKHOUSE_DATABASE}.${stream} (k_id string, k_secret string)
AS \$\$
import json
KID=''; KSEC=''
def _tp_init(p):
    global KID, KSEC
    c=json.loads(p); KID=c['access_key_id']; KSEC=c['secret_access_key']
def go():
    return [(KID, KSEC)]
\$\$
SETTINGS type='python', mode='batch', read_function_name='go',
         init_function_name='_tp_init', named_collection='${nc_name}';
" 2>&1 | grep -oE 'ACCESS_DENIED|SHOULD_NOT_LEAK' | sort -u

# Same vector via ATTACH EXTERNAL STREAM with UUID. The Atomic database engine
# refuses inline ATTACH-with-definition without UUID or FROM, but accepts it
# with one, so a low-priv user could otherwise re-attach a stream that
# references a secret named collection.
${CLICKHOUSE_CLIENT_BINARY} "${CLIENT_CONN_OPTS[@]}" --database="${CLICKHOUSE_DATABASE}" --user="${user}" --password='pw' --query "
ATTACH EXTERNAL STREAM ${CLICKHOUSE_DATABASE}.${stream}_attach_es UUID '11111111-1111-1111-1111-111111111111' (k string)
SETTINGS type='log', named_collection='${nc_name}';
" 2>&1 | grep -oE 'ACCESS_DENIED' | head -1

# Same vector via ATTACH EXTERNAL TABLE with UUID.
${CLICKHOUSE_CLIENT_BINARY} "${CLIENT_CONN_OPTS[@]}" --database="${CLICKHOUSE_DATABASE}" --user="${user}" --password='pw' --query "
ATTACH EXTERNAL TABLE ${CLICKHOUSE_DATABASE}.${stream}_attach_tbl UUID '22222222-2222-2222-2222-222222222222' (k string, v int32)
SETTINGS type='s3', named_collection='${nc_name}', bucket='b', write_to='x.json';
" 2>&1 | grep -oE 'ACCESS_DENIED' | head -1

# Same vector via SETTINGS config_file=… where the file supplies named_collection.
# The check runs against the merged settings so file-supplied named_collection
# cannot bypass query-AST scanning.
config_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nc.cfg"
cat > "${config_file}" <<EOF
type=log
named_collection=${nc_name}
EOF
${CLICKHOUSE_CLIENT_BINARY} "${CLIENT_CONN_OPTS[@]}" --database="${CLICKHOUSE_DATABASE}" --user="${user}" --password='pw' --query "
CREATE EXTERNAL STREAM ${CLICKHOUSE_DATABASE}.${stream}_via_file (k string)
SETTINGS config_file='${config_file}';
" 2>&1 | grep -oE 'ACCESS_DENIED' | head -1
rm -f "${config_file}"

# Confirm: granting the NAMED_COLLECTION privilege then makes the same query work.
# Proton's GRANT parser does not implement the upstream "global-with-parameter"
# branch, so per-collection grants (GRANT NAMED COLLECTION ON foo) are not
# accepted; only the global form (ON *.*) parses.
${CLICKHOUSE_CLIENT} --query "GRANT NAMED COLLECTION ON *.* TO ${user}"

${CLICKHOUSE_CLIENT_BINARY} "${CLIENT_CONN_OPTS[@]}" -mn --database="${CLICKHOUSE_DATABASE}" --user="${user}" --password='pw' --query "
CREATE EXTERNAL STREAM ${CLICKHOUSE_DATABASE}.${stream} (k_id string, k_secret string)
AS \$\$
import json
KID=''; KSEC=''
def _tp_init(p):
    global KID, KSEC
    c=json.loads(p); KID=c['access_key_id']; KSEC=c['secret_access_key']
def go():
    return [(KID, KSEC)]
\$\$
SETTINGS type='python', mode='batch', read_function_name='go',
         init_function_name='_tp_init', named_collection='${nc_name}';
SELECT * FROM ${CLICKHOUSE_DATABASE}.${stream};
"

# Cleanup
${CLICKHOUSE_CLIENT} -mn --query "
DROP STREAM IF EXISTS ${CLICKHOUSE_DATABASE}.${stream};
DROP STREAM IF EXISTS ${CLICKHOUSE_DATABASE}.${stream}_attach_es;
DROP STREAM IF EXISTS ${CLICKHOUSE_DATABASE}.${stream}_attach_tbl;
DROP STREAM IF EXISTS ${CLICKHOUSE_DATABASE}.${stream}_via_file;
DROP USER IF EXISTS ${user};
DROP NAMED COLLECTION IF EXISTS ${nc_name};
"
