#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

output=$($CLICKHOUSE_CLIENT -q "
CREATE OR REPLACE FUNCTION py_func_1(value float32) RETURNS float32 LANGUAGE PYTHON AS \$\$
def py_func_1(values):
    for i in range(len(values)):
        values[i] = values[i] + 5
    return values
\$\$;
" 2>&1)

if (echo "$output" | grep -q "Syntax error"); then
    exit 0; # If python is not enabled, then we can't create python functions and skipped this test
fi

$CLICKHOUSE_CLIENT -q "
CREATE OR REPLACE AGGREGATE FUNCTION py_func_2(value float32) RETURNS float32 LANGUAGE PYTHON AS \$\$
class py_func_2:
    def __init__(self):
        self.max = -1.0
        self.sec = -1.0

    def process(self, values):
        for value in values:
            if value > self.max:
                self.sec = self.max
                self.max = value
            elif value < self.max and value > self.sec:
                self.sec = value

    def finalize(self):
        return [self.sec]

    def serialize(self):
        s = {'max': self.max, 'sec': self.sec}
        return json.dumps(s)

    def deserialize(self, state_str):
        s = json.loads(state_str)
        self.max = s['max']
        self.sec = s['sec']

    def merge(self, state_str):
        s = json.loads(state_str)
        if s['sec'] >= self.max:
            self.max = s['max']
            self.sec = s['sec']
        elif s['max'] >= self.max:
            self.sec = self.max
            self.max = s['max']
        elif s['max'] > self.sec:
            self.sec = s['max']
\$\$;
"

output=$($CLICKHOUSE_CLIENT -q "show create function py_func_1;" 2>&1)
expected_output="CREATE OR REPLACE FUNCTION py_func_1(value float32) RETURNS float32 LANGUAGE PYTHON AS \$\$\ndef py_func_1(values):\n    for i in range(len(values)):\n        values[i] = values[i] + 5\n    return values\n\$\$"
if [ "$output" != "$expected_output" ]; then
    echo "$output"
fi

output=$($CLICKHOUSE_CLIENT -q "show create function py_func_2;" 2>&1)
expected_output="CREATE OR REPLACE AGGREGATE FUNCTION py_func_2(value float32, _tp_delta int8) RETURNS float32 LANGUAGE PYTHON AS \$\$\nclass py_func_2:\n    def __init__(self):\n        self.max = -1.0\n        self.sec = -1.0\n\n    def process(self, values):\n        for value in values:\n            if value > self.max:\n                self.sec = self.max\n                self.max = value\n            elif value < self.max and value > self.sec:\n                self.sec = value\n\n    def finalize(self):\n        return [self.sec]\n\n    def serialize(self):\n        s = {\'max\': self.max, \'sec\': self.sec}\n        return json.dumps(s)\n\n    def deserialize(self, state_str):\n        s = json.loads(state_str)\n        self.max = s[\'max\']\n        self.sec = s[\'sec\']\n\n    def merge(self, state_str):\n        s = json.loads(state_str)\n        if s[\'sec\'] >= self.max:\n            self.max = s[\'max\']\n            self.sec = s[\'sec\']\n        elif s[\'max\'] >= self.max:\n            self.sec = self.max\n            self.max = s[\'max\']\n        elif s[\'max\'] > self.sec:\n            self.sec = s[\'max\']\n\$\$"
if [ "$output" != "$expected_output" ]; then
    echo "$output"
fi

output=$($CLICKHOUSE_CLIENT -q "show functions;" 2>&1)
expected_output="py_func_1	Function	Python
py_func_2	Aggregate Function	Python"
if [ "$output" != "$expected_output" ]; then
    echo "$output"
fi

$CLICKHOUSE_CLIENT -q "drop function if exists py_func_1;"
$CLICKHOUSE_CLIENT -q "drop function if exists py_func_2;"
