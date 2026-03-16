# User-Defined Functions (UDF / UDAF)

Powered by V8 JavaScript engine. Runs in-process, no external service required.

## Scalar UDF

Receives batched arrays (multiple calls batched for performance), returns array.

```sql
CREATE OR REPLACE FUNCTION add_five(value float32)
RETURNS float32
LANGUAGE JAVASCRIPT AS $$
    function add_five(values) {
        return values.map(v => v + 5);
    }
$$;
```

Usage:
```sql
SELECT add_five(temperature) FROM device_metrics;
```

## Aggregate UDAF

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

## Remote UDF

Register an external webhook as a UDF. Supports any language/framework.

```sql
CREATE OR REPLACE FUNCTION remote_func(value string)
RETURNS string
URL 'https://my-endpoint.example.com/process'
EXECUTION_TIMEOUT 5000;
```

Slower than JavaScript UDFs. Does not support custom emit policies.

## Debugging

Use `console.log(...)` in JavaScript UDFs. Output goes to server logs.

## Drop

```sql
DROP FUNCTION [IF EXISTS] <function_name>;
```
