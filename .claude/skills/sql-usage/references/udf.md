# User-Defined Functions (UDF / UDAF)

Timeplus Proton supports four UDF types:

| Type | Language | 3rd-Party Libs | Scalar | Aggregate | Custom Emit | Performance |
|------|----------|---------------|--------|-----------|-------------|-------------|
| SQL UDF | SQL | No | Yes | No | No | Fastest |
| Python UDF | Python 3.10 | Yes | Yes | Yes | Yes | Fast |
| JavaScript UDF | JavaScript (V8) | No | Yes | Yes | Yes | Fast |
| Remote UDF | Any (webhook) | Yes | Yes | No | No | Slow |

---

## SQL UDF

Stateless lambda-style expressions. Best performance (runs in C++ engine).

```sql
CREATE OR REPLACE FUNCTION color_hex AS (r, g, b) -> '#' || hex(r) || hex(g) || hex(b);

SELECT color_hex(255, 128, 0) FROM my_stream;
```

Cannot be aggregate functions. No recursive calls allowed.

---

## Python UDF (Enterprise v2.7+ / Proton 3.0+)

Embedded Python 3.10 runtime. No external service required. Supports standard library + pre-installed packages (numpy, pandas, requests, openai, etc.).

### Scalar Python UDF

Receives batched inputs (list), returns list. Function name in SQL must match the Python `def` name.

```sql
CREATE OR REPLACE FUNCTION add_five(value uint16)
RETURNS int LANGUAGE PYTHON AS $$
def add_five(value):
    for i in range(len(value)):
        value[i] = value[i] + 5
    return value
$$;

SELECT add_five(temperature) FROM device_metrics;
```

### Python UDF with numpy

```sql
CREATE OR REPLACE FUNCTION add_five(value uint16)
RETURNS uint16 LANGUAGE PYTHON AS $$
import numpy as np
def add_five(value):
    np_arr = np.array(value)
    np_arr += 5
    return np_arr.tolist()
$$;
```

### Python UDF for data masking / redaction

```sql
CREATE OR REPLACE FUNCTION redact_email(text string)
RETURNS string LANGUAGE PYTHON AS $$
import re
def redact_email(texts):
    return [re.sub(r'[\w.-]+@[\w.-]+\.\w+', '[REDACTED]', t) for t in texts]
$$;

SELECT redact_email(message) AS safe_message FROM log_stream;
```

### Python UDF for sensitive data encryption

```sql
CREATE OR REPLACE FUNCTION mask_ssn(text string)
RETURNS string LANGUAGE PYTHON AS $$
import re
def mask_ssn(texts):
    return [re.sub(r'\d{3}-\d{2}-(\d{4})', r'***-**-\1', t) for t in texts]
$$;

SELECT mask_ssn(raw_text) FROM user_events;
```

### Python UDAF (Aggregate)

Class-based. Implements `process`, `finalize`, and optionally `serialize`/`deserialize`/`merge` for checkpointing and distributed execution.

```sql
CREATE OR REPLACE AGGREGATE FUNCTION getMax(value uint16)
RETURNS uint16 LANGUAGE PYTHON AS $$
import pickle
class getMax:
    def __init__(self):
        self.max = 0

    def serialize(self):
        return pickle.dumps({'max': self.max})

    def deserialize(self, data):
        data = pickle.loads(data)
        self.max = data['max']

    def merge(self, other):
        if other.max > self.max:
            self.max = other.max

    def process(self, values):
        for item in values:
            if item > self.max:
                self.max = item

    def finalize(self):
        return self.max
$$;

SELECT getMax(temperature) FROM tumble(device_metrics, 1m) GROUP BY window_start;
```

### Python data type mapping

| Timeplus Type | Python Type |
|---------------|-------------|
| bool | bool |
| uint8/16/32/64, int8/16/32/64 | int |
| float32, float64 | float |
| string, fixed_string | str |
| date, date32 | datetime.date |
| datetime, datetime64 | datetime.datetime |
| array | list |
| tuple | tuple |
| map | dict |
| ipv4 | int |

### Manage Python packages (3.0+)

```sql
SYSTEM INSTALL PYTHON PACKAGE 'requests';
SYSTEM INSTALL PYTHON PACKAGE 'requests==2.32.3';
SYSTEM LIST PYTHON PACKAGES;
SYSTEM UNINSTALL PYTHON PACKAGE 'requests';
```

Check install status:
```sql
SELECT status, error_code, error_message
FROM system.python_package_tasks
WHERE package_name = 'requests' AND operation = 'install'
ORDER BY created_at DESC LIMIT 1;
```

---

## JavaScript UDF (V8 engine)

Runs in-process. Receives batched arrays, returns array.

### Scalar JavaScript UDF

```sql
CREATE OR REPLACE FUNCTION add_five(value float32)
RETURNS float32
LANGUAGE JAVASCRIPT AS $$
    function add_five(values) {
        return values.map(v => v + 5);
    }
$$;

SELECT add_five(temperature) FROM device_metrics;
```

### JavaScript UDAF

Must implement 6 functions for distributed aggregation:

| Function | Purpose |
|----------|---------|
| `initialize()` | Set up initial state |
| `process(values)` | Handle incoming value batches |
| `finalize()` | Return aggregated result |
| `serialize()` | Convert state to JSON string (for checkpointing) |
| `deserialize(json)` | Reconstruct state from serialized data |
| `merge(state)` | Combine states from distributed processing |

```sql
CREATE OR REPLACE FUNCTION second_largest(value float32)
RETURNS float32
LANGUAGE JAVASCRIPT AS $$
{
    initialize: function() {
        this.max = -Infinity;
        this.sec = -Infinity;
    },
    process: function(values) {
        for (const v of values) {
            if (v > this.max) { this.sec = this.max; this.max = v; }
            else if (v > this.sec) { this.sec = v; }
        }
    },
    finalize: function() {
        return this.sec;
    },
    serialize: function() {
        return JSON.stringify({ max: this.max, sec: this.sec });
    },
    deserialize: function(json) {
        const s = JSON.parse(json);
        this.max = s.max;
        this.sec = s.sec;
    },
    merge: function(state) {
        if (state.max > this.max) { this.sec = this.max; this.max = state.max; }
        else if (state.max > this.sec) { this.sec = state.max; }
        if (state.sec > this.sec) { this.sec = state.sec; }
    }
}
$$;
```

JavaScript UDFs also support `has_customized_emit: true` for custom emit policies in aggregate functions.

Use `console.log(...)` for debugging — output goes to server logs.

---

## Remote UDF

Register an external webhook as a UDF. Supports any language/framework.

```sql
CREATE OR REPLACE FUNCTION remote_func(value string)
RETURNS string
URL 'https://my-endpoint.example.com/process'
EXECUTION_TIMEOUT 5000;
```

With authentication:
```sql
CREATE REMOTE FUNCTION ip_lookup(ip string)
RETURNS string
URL 'https://my-api.example.com/lookup'
AUTH_METHOD 'auth_header'
AUTH_HEADER 'Authorization'
AUTH_KEY 'Bearer my-token';
```

Slower than local UDFs. Does not support aggregate functions or custom emit policies.

---

## Drop / Show

```sql
DROP FUNCTION [IF EXISTS] <function_name>;
SHOW FUNCTIONS;
SHOW FUNCTIONS WHERE name LIKE 'get%';
SHOW CREATE FUNCTION <function_name>;
```
