# NativeLog 

NativeLog is a home-grown distributed log which adopts lots concepts from Apache Kafka, Pulsar etc systems, but implemented 
in a very light-weight by compromising fewer features. 

# Develop Guideline 

NativeLog is tightly coupled with existing proton code. A clean / standalone buildable code base for NativeLog is not a goal 
since tightly couple is the only way for efficiency (we don't want to introduce another abstraction layer comparing to Kafka).

# Goals
- Simple instance simple.
- Distributed env, replicated.
- Can horizontally scale out for one shard 
- Write availability over read availability 
- Throughput over low latency, but tuned.
- Tail latency (99p) shall be within 100 ~ 500 milliseconds
- Zero serde for Records
- Can be disaggregated or converged
- No external dependency

# Concepts
**Record** : A record is the smallest read/write unit in NativeLog. It contains CRC, sequence number, timestamp etc metadata and a Proton `Block` as the payload

**Segment** : A segment is the smallest unit in file system which contains a range of Record. Every time there is only one active segment which accepts data append request.

**Shard** : A shard is a unit in a `Stream`. Physically in file system, different shards in the same `Stream` have different folders. 

**Stream** : The highest entity which represents a write-ahead-log which backs Proton's stream. A stream can have multiple shards

**AppendTimeIndex** : An reverse index which maps record append time to the starting record. It is used to fast seek by append time. 

**EventTimeIndex** : An reverse index which maps record event time to the starting record. It is used to fast seek by append time.

**Namespace** : A logic concept which groups `Streams` to a namespace. In filesystem, different namespaces have different folders

**Sequencer** : Assign each record a unique number called `Sequence Number` (similar to `offset` in Kafka)

**MetaStore** : A data store which persists stream metadata information like schema, shards, etc

# APIs

## Client APIs

# On Disk Layouts
## Stream On Disk Layout

```

```

## Record On Disk Layout