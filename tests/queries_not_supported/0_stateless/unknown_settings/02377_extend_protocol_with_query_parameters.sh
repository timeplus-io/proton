#!/usr/bin/env bash

# shellcheck disable=SC2154

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT \
  --param_num="42" \
  --param_str="hello" \
  --param_date="2022-08-04 18:30:53" \
  --param_map="{'2b95a497-3a5d-49af-bf85-15763318cde7': [1.2, 3.4]}" \
  -q "select {num:uint64}, {str:string}, {date:DateTime}, {map:map(uuid, array(float32))}"


$CLICKHOUSE_CLIENT \
  --param_num="42" \
  --param_str="hello" \
  --param_date="2022-08-04 18:30:53" \
  --param_map="{'2b95a497-3a5d-49af-bf85-15763318cde7': [1.2, 3.4]}" \
  -q "select to_type_name({num:uint64}), to_type_name({str:string}), to_type_name({date:DateTime}), to_type_name({map:map(uuid, array(float32))})"


table_name="t_02377_extend_protocol_with_query_parameters_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT -n -q "
  create stream $table_name(
    id int64,
    arr array(uint8),
    map map(string, uint8),
    mul_arr array(array(uint8)),
    map_arr map(uint8, array(uint8)),
    map_map_arr map(string, map(string, array(uint8))))
  engine = MergeTree
  order by (id)"


$CLICKHOUSE_CLIENT \
  --param_id="42" \
  --param_arr="[1, 2, 3]" \
  --param_map="{'abc': 22, 'def': 33}" \
  --param_mul_arr="[[4, 5, 6], [7], [8, 9]]" \
  --param_map_arr="{10: [11, 12], 13: [14, 15]}" \
  --param_map_map_arr="{'ghj': {'klm': [16, 17]}, 'nop': {'rst': [18]}}" \
  -q "insert into $table_name values({id: int64}, {arr: array(uint8)}, {map: map(string, uint8)}, {mul_arr: array(array(uint8))}, {map_arr: map(uint8, array(uint8))}, {map_map_arr: map(string, map(string, array(uint8)))})"


$CLICKHOUSE_CLIENT -q "select * from $table_name"


$CLICKHOUSE_CLIENT \
  --param_tbl="numbers" \
  --param_db="system" \
  --param_col="number" \
  -q "select {col:Identifier} from {db:Identifier}.{tbl:Identifier} limit 1 offset 5"


# it is possible to set parameter for the current session
$CLICKHOUSE_CLIENT -n -q "set param_n = 42; select {n: uint8}"
# and it will not be visible to other sessions
$CLICKHOUSE_CLIENT -n -q "select {n: uint8} -- { serverError 456 }"


# the same parameter could be set multiple times within one session (new value overrides the previous one)
$CLICKHOUSE_CLIENT -n -q "set param_n = 12; set param_n = 13; select {n: uint8}"


# multiple different parameters could be defined within each session
$CLICKHOUSE_CLIENT -n -q "
  set param_a = 13, param_b = 'str';
  set param_c = '2022-08-04 18:30:53';
  set param_d = '{\'10\': [11, 12], \'13\': [14, 15]}';
  select {a: uint32}, {b: string}, {c: DateTime}, {d: map(string, array(uint8))}"


# empty parameter name is not allowed
$CLICKHOUSE_CLIENT --param_="" -q "select 1" 2>&1 | grep -c 'Code: 36'
$CLICKHOUSE_CLIENT -q "set param_ = ''" 2>&1 | grep -c 'Code: 36'


# parameters are also supported for DESCRIBE STREAM queries
$CLICKHOUSE_CLIENT \
  --param_id="42" \
  --param_arr="[1, 2, 3]" \
  --param_map="{'abc': 22, 'def': 33}" \
  --param_mul_arr="[[4, 5, 6], [7], [8, 9]]" \
  --param_map_arr="{10: [11, 12], 13: [14, 15]}" \
  --param_map_map_arr="{'ghj': {'klm': [16, 17]}, 'nop': {'rst': [18]}}" \
  -q "describe stream(select {id: int64}, {arr: array(uint8)}, {map: map(string, uint8)}, {mul_arr: array(array(uint8))}, {map_arr: map(uint8, array(uint8))}, {map_map_arr: map(string, map(string, array(uint8)))})"

$CLICKHOUSE_CLIENT --param_p=42 -q "describe stream (select * from (select {p:int8} as a group by a) order by a)"
