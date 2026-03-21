# Cluster Dependency Hierarchy

## Diagram

```
                         ┌──────────────┐     ┌────────────┐
                         │              │     │            │
                         │   MetaStore  │     │   Replica  │
                         │              │     │            │
                         └───────┬──────┘     └──┬───┬─────┘
                                 │               │   │
                                 │               │   │
     ┌─────────────┐     ┌───────▼──────┐        │   │
     │             │     │              │        │   │
     │  KafkaLog   │     │   NativeLog  ◄────────┘   │
     │             │     │              │            │
     └─────────────┘     └──────────────┘            │
                                                     │
                                                     │
                          ┌─────────────┐            │
                          │             │            │
                          │     Raft    │◄───────────┘
                          │             │
                          └──────┬──────┘
                                 │
                          ┌──────▼──────┐
                          │  Requests   │
                          └──────┬──────┘
                                 │
                          ┌──────▼──────┐
                          │  Protocol   │
                          └──────┬──────┘
                                 │
                          ┌──────▼──────┐
                          │   Common    │
                          └─────────────┘
```

## Dependency rules

| Component | Can depend on | MUST NOT depend on |
|-----------|--------------|-------------------|
| MetaStore | NativeLog, KafkaLog, Raft, lower | Replica |
| Replica | NativeLog, Raft, lower | MetaStore |
| NativeLog | Raft, lower | MetaStore, Replica, KafkaLog |
| KafkaLog | (standalone) | NativeLog, Raft, MetaStore, Replica |
| Raft | Requests, Protocol, Common | NativeLog, KafkaLog, Replica, MetaStore |

## Key rules

- [ ] NEVER introduce circular dependencies
- [ ] Lower layers must not import from upper layers
- [ ] See `src/Cluster/README.md` for authoritative reference
