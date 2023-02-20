#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP STREAM IF EXISTS insert_values_parametrized";
$CLICKHOUSE_CLIENT --query="CREATE STREAM insert_values_parametrized (n uint8, s string, a array(float32)) ENGINE = Memory";

$CLICKHOUSE_CLIENT --input_format_values_deduce_templates_of_expressions=1 --input_format_values_interpret_expressions=0 --param_p_n="-1" --param_p_s="param" --param_p_a="[0.2,0.3]" --query="INSERT INTO insert_values_parametrized  VALUES
(1 + {p_n:int8}, lower(concat('Hello', {p_s:string})), array_sort(array_intersect([],            {p_a:array(nullable(float32))}))),\
(2 + {p_n:int8}, lower(concat('world', {p_s:string})), array_sort(array_intersect([0.1,0.2,0.3], {p_a:array(nullable(float32))}))),\
(3 + {p_n:int8}, lower(concat('TEST',  {p_s:string})), array_sort(array_intersect([0.1,0.3,0.4], {p_a:array(nullable(float32))}))),\
(4 + {p_n:int8}, lower(concat('PaRaM', {p_s:string})), array_sort(array_intersect([0.5],         {p_a:array(nullable(float32))})))";

$CLICKHOUSE_CLIENT --input_format_values_deduce_templates_of_expressions=0 --input_format_values_interpret_expressions=1 --param_p_n="-1" --param_p_s="param" --param_p_a="[0.2,0.3]" --query="INSERT INTO insert_values_parametrized  VALUES \
(5 + {p_n:int8}, lower(concat('Evaluate', {p_s:string})), array_intersect([0, 0.2, 0.6], {p_a:array(nullable(float32))}))"

$CLICKHOUSE_CLIENT --param_p_n="5" --param_p_s="param" --param_p_a="[0.2,0.3]" --query="INSERT INTO insert_values_parametrized  VALUES \
({p_n:int8}, {p_s:string}, {p_a:array(nullable(float32))})"

$CLICKHOUSE_CLIENT --query="SELECT * FROM insert_values_parametrized ORDER BY n";

$CLICKHOUSE_CLIENT --query="DROP STREAM insert_values_parametrized";
