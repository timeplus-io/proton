# Source Layout

## Core engine (src/)

```
src/
├── Parsers/                          # SQL parsing
├── Interpreters/                     # Query execution logic
│   └── Streaming/                    # Streaming-specific interpreters
├── Storages/                         # Storage engines
│   ├── Stream/                       # StorageStream, streaming sources/sinks
│   └── MatView/                      # Materialized View implementation
├── Functions/                        # SQL functions
├── AggregateFunctions/               # Aggregation functions
├── Processors/                       # Query pipeline processors
│   ├── Transforms/Streaming/         # 99+ streaming transforms (windowing, aggregation, joins)
│   └── QueryPlan/Streaming/          # 42 streaming query plan steps
└── Cluster/                          # Clustering components
    ├── Common/                       # Base networking/serialization
    ├── Protocol/                     # Wire protocol
    ├── Raft/                         # Consensus
    ├── NativeLog/                    # Native log storage backend
    ├── KafkaLog/                     # Kafka log storage backend
    ├── Replica/                      # Replication
    └── MetaStore/                    # Metadata management
```

## Programs (programs/)

```
programs/
├── server/            # proton server
├── client/            # proton client
├── local/             # Local mode (single binary)
├── benchmark/         # Benchmarking tools
└── compressor/        # Compression utilities
```

## Tests (tests/)

```
tests/
├── queries_ported/    # Stateless SQL test cases
│   ├── <id>_*.sql     # Test SQL files
│   └── <id>_*.reference  # Expected outputs
└── cluster/           # Smoke and deployment probe tests
    ├── smoke/         # Single-instance smoke tests
    └── deployment/    # Deployment probe scripts
```

Note: C++ unit tests live alongside source files in `src/` (built into `build/src/stripped/bin/unit_tests_dbms`).

## Build artifacts (build/)

```
build/
├── programs/
│   ├── stripped/bin/   # ALWAYS use these
│   │   └── proton
│   └── proton          # Fallback only
└── src/
    └── stripped/bin/
        └── unit_tests_dbms
```

## Proton-specific vs ClickHouse-inherited

| Area | Origin | Proton fences needed? |
|------|--------|----------------------|
| `src/Storages/Stream/` | Proton | No (already Proton-specific) |
| `namespace DB::Streaming` | Proton | No (already Proton-specific) |
| Everything else in `src/` | ClickHouse | Yes, use `/// proton: starts/ends` |
