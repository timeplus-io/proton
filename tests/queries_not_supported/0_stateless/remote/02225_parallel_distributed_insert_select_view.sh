#!/usr/bin/env bash

# NOTE: sh test is required since view() does not have current database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
drop stream if exists dst_02225;
drop stream if exists src_02225;
create stream dst_02225 (key int) engine=Memory();
create stream src_02225 (key int) engine=Memory();
insert into src_02225 values (1);
"

$CLICKHOUSE_CLIENT --param_database=$CLICKHOUSE_DATABASE -nm -q "
truncate stream dst_02225;
insert into function remote('127.{1,2}', current_database(), dst_02225, key)
select * from remote('127.{1,2}', view(select * from {database:Identifier}.src_02225), key)
settings parallel_distributed_insert_select=2, max_distributed_depth=1;
select * from dst_02225;

-- w/o sharding key
truncate stream dst_02225;
insert into function remote('127.{1,2}', current_database(), dst_02225, key)
select * from remote('127.{1,2}', view(select * from {database:Identifier}.src_02225))
settings parallel_distributed_insert_select=2, max_distributed_depth=1;
select * from dst_02225;
"

$CLICKHOUSE_CLIENT -nm -q "
drop stream src_02225;
drop stream dst_02225;
"
