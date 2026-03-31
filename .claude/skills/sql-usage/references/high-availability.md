# High Availability and Fault Tolerance

## Proton (open-source) vs Timeplus Enterprise

Proton is a **single-node** streaming SQL engine. It does not support clustering, replication, or multi-node high availability. For production HA deployments, use **Timeplus Enterprise**, which extends Proton with full cluster support.

| Capability | Proton (open-source) | Timeplus Enterprise |
|------------|---------------------|-------------------|
| Single-node deployment | Yes | Yes |
| Multi-node cluster | No | Yes |
| Stream replication (Raft) | No | Yes |
| Materialized view HA | No | Yes (Raft-based + Scheduler-based) |
| Checkpoint replication | Local file only | NativeLog, shared storage (S3), local |
| Distributed ingest | No | Yes |
| Distributed query | No | Yes |
| Compute node auto-scaling | No | Yes (K8s HPA, AWS ASG) |

## Proton single-node resilience

On a single Proton node, you can still configure:

- **Checkpointing** for materialized views to recover state after restart:
  ```sql
  CREATE MATERIALIZED VIEW mv_name INTO target_stream
  SETTINGS checkpoint_interval = 30
  AS SELECT ... FROM source_stream;
  ```
- **Persistent storage** for streams (data survives process restart)
- **Dead letter queue** for poison event handling:
  ```sql
  CREATE MATERIALIZED VIEW mv_name INTO target_stream
  SETTINGS enable_dlq = true, recovery_policy = 'best_effort'
  AS SELECT ... FROM source_stream;
  ```

## Timeplus Enterprise cluster architecture

A Timeplus Enterprise cluster has three node roles:

- **Metadata nodes**: Manage cluster topology, store metadata (streams, MVs, UDFs, users). Run system routines (load balancing, scheduling, alerts).
- **Data nodes**: Handle persistence, replication, and read/write operations. Can also execute computations.
- **Compute nodes**: Dedicated to computation (MVs, tasks, alerts). Stateless and ephemeral — ideal for auto-scaling with K8s HPA or AWS ASG.

Underlying both metadata and data layers is **NativeLog**, the distributed journal and WAL built on **Multi-Raft consensus**.

### Typical 3-node cluster

Each node serves as both metadata + data node. Streams default to `replication_factor = 3`.

```sql
-- In Enterprise, streams are automatically replicated
CREATE STREAM device_metrics (
    device_id string,
    temperature float64
) SETTINGS shards = 2, replication_factor = 3;
```

### Distributed ingest flow

1. Ingest lands on leader replica → appended to NativeLog → Raft replicates to followers.
2. If ingest lands on follower → forwarded to leader → replicated (slightly slower).
3. Multi-shard streams: batch is split by sharding expression, each shard ingests independently.

### Materialized view HA models

#### Raft-based HA (default)

- Three replicas on different nodes
- Leader executes the query, checkpoints state via NativeLog + Raft
- On leader failure: follower elected, recovers from replicated checkpoint
- Best for: on-prem, low failover latency, steady workloads

```sql
-- Default: Raft-based HA with NativeLog checkpoint replication
CREATE MATERIALIZED VIEW mv_agg INTO target_stream AS
SELECT window_start, count(*) FROM tumble(source, 1m) GROUP BY window_start;
```

#### Scheduler-based HA

- Centralized scheduler monitors and reschedules failed MVs
- Checkpoints to shared storage (S3)
- Best for: large-scale, elastic workloads, K8s/cloud auto-scaling

```sql
-- Scheduler-based HA with shared storage checkpoints
CREATE MATERIALIZED VIEW mv_agg INTO target_stream
SETTINGS checkpoint_settings = 'replication_type=shared;shared_disk=s3://bucket/checkpoints'
AS SELECT window_start, count(*) FROM tumble(source, 1m) GROUP BY window_start;
```

### Monitoring cluster health

Enterprise provides built-in system views:

| System View | Purpose |
|-------------|---------|
| `v_no_leader_shards` | Shards without a leader |
| `v_replication_lags` | Replication lag between replicas |
| `v_shard_leaders` | Current shard leader assignments |
| `v_under_replication_replicas` | Under-replicated data |
| `v_mat_view_lags` | MV processing lag |
| `v_failed_mat_views` | Failed materialized views |

### Rack-aware replica placement

Enterprise supports placement policies for multi-site deployments:

```sql
-- Place replicas across availability zones
ALTER STREAM my_stream MODIFY SETTING
    placement_policy = 'balanced',
    placement_affinity = 'node';
```

## Recommendation

- **Development / testing / edge**: Use Proton (single-node) with local checkpointing.
- **Production / critical pipelines**: Use Timeplus Enterprise with cluster deployment for replication, automatic failover, and distributed query.

See https://docs.timeplus.com for Enterprise deployment guides and Kubernetes Helm charts.
