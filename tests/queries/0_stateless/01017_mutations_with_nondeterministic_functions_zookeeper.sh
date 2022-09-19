#!/usr/bin/env bash
# Tags: zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

R1=table_1017_1
R2=table_1017_2
T1=table_1017_merge

${CLICKHOUSE_CLIENT} -n -q "
    DROP DICTIONARY IF EXISTS dict1;
    DROP STREAM IF EXISTS $R1;
    DROP STREAM IF EXISTS $R2;
    DROP STREAM IF EXISTS $T1;

    DROP STREAM IF EXISTS lookup_table;
    DROP STREAM IF EXISTS table_for_dict;

    create stream table_for_dict (y uint64, y_new uint32)  ;
    INSERT INTO table_for_dict VALUES (3, 3003),(4,4004);

    CREATE DICTIONARY dict1( y uint64 DEFAULT 0, y_new uint32 DEFAULT 0 ) PRIMARY KEY y
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB '${CLICKHOUSE_DATABASE}'))
    LIFETIME(MIN 1 MAX 10)
    LAYOUT(FLAT());

    create stream lookup_table (y uint32, y_new uint32) ENGINE = Join(ANY, LEFT, y);
    INSERT INTO lookup_table VALUES(1,1001),(2,1002);

    create stream $R1 (x uint32, y uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/table_1017', 'r1') ORDER BY x;
    create stream $R2 (x uint32, y uint32) ENGINE ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/table_1017', 'r2') ORDER BY x;
    create stream $T1 (x uint32, y uint32) ENGINE MergeTree() ORDER BY x;

    INSERT INTO $R1 VALUES (0, 1)(1, 2)(2, 3)(3, 4);
    INSERT INTO $T1 VALUES (0, 1)(1, 2)(2, 3)(3, 4);
"

# Check that in mutations of replicated tables predicates do not contain non-deterministic functions
${CLICKHOUSE_CLIENT} --query "ALTER STREAM $R1 DELETE WHERE ignore(rand())" 2>&1 \
| grep -F -q "must use only deterministic functions" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "ALTER STREAM $R1 UPDATE y = y + rand() % 1 WHERE not ignore()" 2>&1 \
| grep -F -q "must use only deterministic functions" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "ALTER STREAM $R1 UPDATE y = x + arrayCount(x -> (x + y) % 2, range(y)) WHERE not ignore()" > /dev/null 2>&1 \
&& echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "ALTER STREAM $R1 UPDATE y = x + arrayCount(x -> (rand() + x) % 2, range(y)) WHERE not ignore()" 2>&1 \
| grep -F -q "must use only deterministic functions" && echo 'OK' || echo 'FAIL'


# For regular tables we do not enforce deterministic functions
${CLICKHOUSE_CLIENT} --query "ALTER STREAM $T1 DELETE WHERE rand() = 0" > /dev/null 2>&1 \
&& echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "ALTER STREAM $T1 UPDATE y = y + rand() % 1 WHERE not ignore()" > /dev/null 2>&1 \
&& echo 'OK' || echo 'FAIL'

# hm... it looks like joinGet condidered determenistic
${CLICKHOUSE_CLIENT} --query "ALTER STREAM $R1 UPDATE y = joinGet('${CLICKHOUSE_DATABASE}.lookup_table', 'y_new', y) WHERE x=1" 2>&1 \
&& echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "ALTER STREAM $R1 DELETE WHERE dictHas('${CLICKHOUSE_DATABASE}.dict1', to_uint64(x))" 2>&1 \
| grep -F -q "must use only deterministic functions" && echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} --query "ALTER STREAM $R1 DELETE WHERE dictHas('${CLICKHOUSE_DATABASE}.dict1', to_uint64(x))" --allow_nondeterministic_mutations=1 2>&1 \
&& echo 'OK' || echo 'FAIL'

${CLICKHOUSE_CLIENT} -n -q "
    DROP DICTIONARY IF EXISTS dict1;
    DROP STREAM IF EXISTS $R2;
    DROP STREAM IF EXISTS $R1;
    DROP STREAM IF EXISTS $T1;
    DROP STREAM IF EXISTS lookup_table;
    DROP STREAM IF EXISTS table_for_dict;
"
